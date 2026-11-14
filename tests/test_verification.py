"""名单核验：版本绑定、冲突识别、一次性落盘、过期标记与按原版本下载。"""
import io
import threading

import pytest

from app import db, repository, services, worker
from app.web import create_app


@pytest.fixture()
def app_ctx(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    upload_dir = data_dir / "uploads"
    upload_dir.mkdir()

    import app.config as config
    saved = (config.DATA_DIR, config.DB_PATH, config.UPLOAD_DIR)
    config.DATA_DIR = str(data_dir)
    config.DB_PATH = str(data_dir / "test.db")
    config.UPLOAD_DIR = str(upload_dir)

    db.init_db()
    worker._stop.clear()
    worker._started = False
    worker.start_worker()
    yield {
        "services": services,
        "repository": repository,
        "db": db,
        "worker": worker,
        "config": config,
    }
    worker.stop_worker()
    config.DATA_DIR, config.DB_PATH, config.UPLOAD_DIR = saved


def _wait_jobs(timeout=10):
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not repository.pending_jobs():
            return
        time.sleep(0.05)
    raise AssertionError("后台任务未在超时内完成")


def _import(text):
    services.create_import_job("p.csv", text.encode("utf-8"))
    _wait_jobs()


def _pid(hospital, mrn):
    with db.Snapshot() as conn:
        return conn.execute(
            "SELECT id FROM patients WHERE hospital=? AND hospital_mrn=?",
            (hospital, mrn),
        ).fetchone()["id"]


def _verify(text, filename="list.csv"):
    return services.verify_csv(filename, text.encode("utf-8"))


def _rows(report_id):
    with db.Snapshot() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM verification_rows WHERE report_id=? ORDER BY row_no",
                (report_id,),
            )
        ]


def _setup_groups():
    """H01/A1 + H02/B1 同组；H03/C1 独立；H04/D1 + H05/E1 同组。"""
    _import("hospital,mrn\nH01,A1\nH02,B1\nH03,C1\nH04,D1\nH05,E1\n")
    a, b = _pid("H01", "A1"), _pid("H02", "B1")
    d, e = _pid("H04", "D1"), _pid("H05", "E1")
    services.confirm_relation(a, b)
    services.confirm_relation(d, e)
    _wait_jobs()
    return a, b, d, e


def test_verify_requires_published_groups(app_ctx):
    with pytest.raises(services.ServiceError) as exc:
        _verify("study_id,hospital,mrn\nS1,H01,A1\n")
    assert "尚无已发布分组" in str(exc.value)
    # 拒绝时不得留下任何报告
    assert repository.verification_page()["reports"] == []


def test_verify_ok_unmatched_and_invalid(app_ctx):
    _setup_groups()
    report_id = _verify(
        "study_id,hospital,mrn\n"
        "S1,H01,A1\n"
        "S1,H02,B1\n"
        "S2,H99,NOPE\n"
        "S3,,\n"
    )
    rows = _rows(report_id)
    assert [r["status"] for r in rows] == ["OK", "OK", "UNMATCHED", "INVALID"]
    assert rows[0]["group_no"] == rows[1]["group_no"]
    report = repository.verification_report(report_id)
    assert report["total_rows"] == 4
    assert report["ok_rows"] == 2
    assert report["unmatched_rows"] == 1
    assert report["invalid_rows"] == 1
    assert report["conflict_rows"] == 0


def test_study_in_multiple_groups_reports_rows_and_groups(app_ctx):
    _setup_groups()
    report_id = _verify(
        "study_id,hospital,mrn\n"
        "S1,H01,A1\n"
        "S1,H03,C1\n"
        "S2,H04,D1\n"
    )
    rows = _rows(report_id)
    assert rows[0]["status"] == "CONFLICT_STUDY"
    assert rows[1]["status"] == "CONFLICT_STUDY"
    assert rows[0]["related_rows"] == "2,3"
    assert len(rows[0]["related_groups"].split(",")) == 2
    assert rows[2]["status"] == "OK"
    assert repository.verification_report(report_id)["conflict_rows"] == 2


def test_group_with_multiple_studies_reports_rows_and_groups(app_ctx):
    _setup_groups()
    report_id = _verify(
        "study_id,hospital,mrn\n"
        "S1,H01,A1\n"
        "S2,H02,B1\n"
    )
    rows = _rows(report_id)
    assert all(r["status"] == "CONFLICT_GROUP" for r in rows)
    assert rows[0]["related_rows"] == "2,3"
    assert rows[0]["related_groups"] == str(rows[0]["group_no"])
    assert "S1" in rows[0]["detail"] and "S2" in rows[0]["detail"]


def test_report_binds_snapshot_and_old_report_served_from_original_version(app_ctx):
    _setup_groups()
    old_id = _verify("study_id,hospital,mrn\nS1,H01,A1\nS1,H02,B1\n")
    old = repository.verification_report(old_id)
    assert old["stale"] is False
    old_text, _ = repository.export_verification_csv(old_id)

    # 关系变化触发新版本发布
    c = _pid("H03", "C1")
    e = _pid("H05", "E1")
    services.confirm_relation(c, e)
    _wait_jobs()

    # 旧报告标记过期，但内容仍按原版本，下载内容完全一致
    old_after = repository.verification_report(old_id)
    assert old_after["stale"] is True
    assert old_after["compute_id"] == old["compute_id"]
    new_text, info = repository.export_verification_csv(old_id)
    assert new_text == old_text
    assert info["compute_id"] == old["compute_id"]
    assert info["stale"] is True
    # 报告行未混入新版本数据
    assert all(r["status"] == "OK" for r in _rows(old_id))


def test_verify_uses_latest_version_at_submit(app_ctx):
    _setup_groups()
    first = repository.verification_page()["latest_version"]
    c, e = _pid("H03", "C1"), _pid("H05", "E1")
    services.confirm_relation(c, e)
    _wait_jobs()
    second = repository.verification_page()["latest_version"]
    assert second > first
    report_id = _verify("study_id,hospital,mrn\nS1,H03,C1\nS1,H05,E1\n")
    report = repository.verification_report(report_id)
    assert report["compute_id"] == second
    assert report["ok_rows"] == 2


def test_failed_generation_leaves_no_report(app_ctx):
    _setup_groups()
    with pytest.raises(services.ServiceError):
        _verify("study_id,hospital,mrn\n")  # 无有效数据行
    assert repository.verification_page()["reports"] == []
    with db.Snapshot() as conn:
        assert conn.execute(
            "SELECT COUNT(*) c FROM verification_reports"
        ).fetchone()["c"] == 0


def test_row_limit_1000(app_ctx):
    _setup_groups()
    header = "study_id,hospital,mrn\n"
    ok = header + "".join(f"S{i},H99,X{i}\n" for i in range(1000))
    report_id = _verify(ok)
    assert repository.verification_report(report_id)["total_rows"] == 1000

    too_many = header + "".join(f"S{i},H99,X{i}\n" for i in range(1001))
    with pytest.raises(services.ServiceError) as exc:
        _verify(too_many)
    assert "1000" in str(exc.value)


def test_partial_write_failure_leaves_no_downloadable_report(app_ctx):
    """明细写入失败时事务回滚：不得留下元数据或可下载的半份报告。"""
    _setup_groups()

    # 用触发器模拟写入逐行明细时中断（元数据已先插入）
    conn = db.connect()
    try:
        conn.execute(
            "CREATE TRIGGER fail_verify_rows BEFORE INSERT ON verification_rows "
            "BEGIN SELECT RAISE(ABORT, '模拟写入中断'); END"
        )
    finally:
        conn.close()

    with pytest.raises(Exception):
        _verify("study_id,hospital,mrn\nS1,H01,A1\nS2,H02,B1\n")

    assert repository.verification_page()["reports"] == []
    with db.Snapshot() as conn:
        assert conn.execute(
            "SELECT COUNT(*) c FROM verification_reports"
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM verification_rows"
        ).fetchone()["c"] == 0

    drop = db.connect()
    try:
        drop.execute("DROP TRIGGER fail_verify_rows")
    finally:
        drop.close()

    # 故障排除后核验恢复正常
    report_id = _verify("study_id,hospital,mrn\nS1,H01,A1\n")
    assert repository.verification_report(report_id)["status"] == "DONE"


def test_stale_version_during_save_recomputes_against_new_version(
    app_ctx, monkeypatch
):
    """保存前版本被新发布超越：重算并绑定新版本，报告不混用两个版本。"""
    _setup_groups()
    first = repository.verification_page()["latest_version"]

    # 在第一次保存尝试前发布一个新版本，模拟提交瞬间的并发更新
    original_save = services._save_report
    calls = {"n": 0}

    def save_after_publish(filename, version, errors, results, counts):
        if calls["n"] == 0:
            calls["n"] += 1
            c, e = _pid("H03", "C1"), _pid("H05", "E1")
            services.confirm_relation(c, e)
            _wait_jobs()
        return original_save(filename, version, errors, results, counts)

    monkeypatch.setattr(services, "_save_report", save_after_publish)
    report_id = _verify("study_id,hospital,mrn\nS1,H03,C1\n")
    report = repository.verification_report(report_id)
    assert report["compute_id"] > first
    assert report["stale"] is False
    rows = _rows(report_id)
    assert rows[0]["status"] == "OK"
    assert rows[0]["group_no"] == _pid("H03", "C1")


def test_report_survives_restart(app_ctx):
    _setup_groups()
    report_id = _verify("study_id,hospital,mrn\nS1,H01,A1\n")
    before = _rows(report_id)

    worker.stop_worker()
    import time
    time.sleep(0.2)
    worker._stop.clear()
    worker._started = False
    worker.start_worker()

    assert repository.verification_report(report_id)["status"] == "DONE"
    assert _rows(report_id) == before


def test_web_submit_view_download(app_ctx):
    _setup_groups()
    app = create_app()
    app.config["TESTING"] = True
    client = app.test_client()

    resp = client.post(
        "/verification",
        data={
            "file": (
                io.BytesIO(
                    "study_id,hospital,mrn\nS1,H01,A1\nS1,H03,C1\n".encode()
                ),
                "partner.csv",
            )
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "研究编号多组" in body
    with db.Snapshot() as conn:
        report_id = conn.execute(
            "SELECT MAX(id) AS m FROM verification_reports"
        ).fetchone()["m"]

    view = client.get(f"/verification?report_id={report_id}")
    assert view.status_code == 200
    assert "涉及行" in view.get_data(as_text=True)

    download = client.get(f"/download/verification/{report_id}.csv")
    assert download.status_code == 200
    assert download.headers["X-Group-Version"].isdigit()
    text = download.get_data(as_text=True)
    assert "CONFLICT_STUDY" in text
    assert "group_version" in text


def test_concurrent_verification_during_recompute(app_ctx):
    """核验计算不占写事务：计算期间导入/对照下载仍可用。"""
    _setup_groups()
    results = []

    def do_verify(i):
        try:
            results.append(
                ("ok", _verify(f"study_id,hospital,mrn\nS{i},H01,A1\n"))
            )
        except Exception as exc:  # noqa: BLE001
            results.append(("err", str(exc)))

    threads = [threading.Thread(target=do_verify, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    # 同时进行导入与对照下载
    _import("hospital,mrn\nH05,E1\nH06,F1\n")
    assert repository.export_mapping_csv() is not None
    for t in threads:
        t.join()

    assert all(status == "ok" for status, _ in results)
    ids = [rid for _s, rid in results]
    assert len(set(ids)) == 5
    for rid in ids:
        assert repository.verification_report(rid)["status"] == "DONE"


def test_version_ids_do_not_repeat_after_empty_publish(app_ctx):
    """发布版本号单调递增，空分组发布后也不复用旧 compute_id。"""
    _setup_groups()
    with db.Snapshot() as conn:
        before = conn.execute(
            "SELECT MAX(compute_id) c FROM group_versions"
        ).fetchone()["c"]

    # 清空患者/关系后触发一次重算（此时没有 groups 行可作版本基数）
    def wipe(conn):
        conn.execute("DELETE FROM relations")
        conn.execute("DELETE FROM groups")
        services.enqueue_recompute(conn, 0)

    db.write_tx(wipe)
    _wait_jobs()
    with db.Snapshot() as conn:
        versions = [
            r["compute_id"]
            for r in conn.execute(
                "SELECT compute_id FROM group_versions ORDER BY compute_id"
            )
        ]
    assert before in versions
    assert len(versions) == len(set(versions))
    newest = max(versions)
    # 再来一次有成员的分组发布，版本号仍更大
    _import("hospital,mrn\nH07,G1\n")
    with db.Snapshot() as conn:
        after = conn.execute(
            "SELECT MAX(compute_id) c FROM group_versions"
        ).fetchone()["c"]
    assert after > newest
