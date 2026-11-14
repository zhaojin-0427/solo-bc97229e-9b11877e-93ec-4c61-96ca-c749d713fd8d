"""只读查询层：进度、成员、拒绝原因、对照下载。

每个公开方法在单个快照事务内完成全部读取，保证一次查询或下载
来自同一个完整状态（绝不在多事务间拼装）。
"""
import csv
import io

from . import db


def _group_published(conn):
    return conn.execute(
        "SELECT COUNT(*) AS c FROM groups"
    ).fetchone()["c"] > 0


def overview():
    with db.Snapshot() as conn:
        stats = {
            "patients": conn.execute(
                "SELECT COUNT(*) AS c FROM patients"
            ).fetchone()["c"],
            "active_relations": conn.execute(
                "SELECT COUNT(*) AS c FROM relations WHERE status='ACTIVE'"
            ).fetchone()["c"],
            "revoked_relations": conn.execute(
                "SELECT COUNT(*) AS c FROM relations WHERE status='REVOKED'"
            ).fetchone()["c"],
            "groups": conn.execute(
                "SELECT COUNT(DISTINCT group_no) AS c FROM groups"
            ).fetchone()["c"],
            "published": _group_published(conn),
            "latest_compute": conn.execute(
                "SELECT COALESCE(MAX(compute_id), 0) AS c FROM groups"
            ).fetchone()["c"],
        }
        jobs = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM jobs ORDER BY id DESC LIMIT 20"
            )
        ]
        return stats, jobs


def index_page():
    """首页所需全部数据取自同一个快照事务（一个完整状态）。"""
    with db.Snapshot() as conn:
        stats, jobs = _stats(conn), _recent_jobs(conn)
        pending = _pending(conn)
        failed = _failed(conn)
        return stats, jobs, pending, failed


def relations_page(q=None, limit=200):
    """核对页：患者、关系流水、失败原因取自同一个快照。"""
    with db.Snapshot() as conn:
        patients = _patients(conn, q, limit)
        rels = _relations(conn, limit)
        failed = _failed(conn)
        return patients, rels, failed


def groups_page():
    """分组页：分组、进行中任务、失败原因取自同一个快照。"""
    with db.Snapshot() as conn:
        groups = _groups(conn)
        pending = _pending(conn)
        failed = _failed(conn)
        return groups, pending, failed


# ---- 单快照内的私有读取单元，供页面级组合复用 ----

def _stats(conn):
    return {
        "patients": conn.execute(
            "SELECT COUNT(*) AS c FROM patients").fetchone()["c"],
        "active_relations": conn.execute(
            "SELECT COUNT(*) AS c FROM relations WHERE status='ACTIVE'"
        ).fetchone()["c"],
        "revoked_relations": conn.execute(
            "SELECT COUNT(*) AS c FROM relations WHERE status='REVOKED'"
        ).fetchone()["c"],
        "groups": conn.execute(
            "SELECT COUNT(DISTINCT group_no) AS c FROM groups"
        ).fetchone()["c"],
        "published": _group_published(conn),
        "latest_compute": conn.execute(
            "SELECT COALESCE(MAX(compute_id), 0) AS c FROM groups"
        ).fetchone()["c"],
    }


def _recent_jobs(conn):
    return [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT 20")
    ]


def _pending(conn):
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, kind, status, filename, total_rows, done_rows, "
            "inserted, reused, rejected, seq_cutoff, created_at "
            "FROM jobs WHERE status IN ('PENDING','RUNNING') ORDER BY id")
    ]


def _failed(conn):
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, kind, status, filename, detail, finished_at "
            "FROM jobs WHERE status='FAILED' ORDER BY id DESC")
    ]


def _patients(conn, q, limit):
    if q:
        like = f"%{q}%"
        rows = conn.execute(
            "SELECT p.id, p.hospital, p.hospital_mrn, g.group_no "
            "FROM patients p LEFT JOIN groups g ON g.patient_id=p.id "
            "WHERE p.hospital LIKE ? OR p.hospital_mrn LIKE ? "
            "OR CAST(p.id AS TEXT)=? ORDER BY p.id LIMIT ?",
            (like, like, q.strip(), limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT p.id, p.hospital, p.hospital_mrn, g.group_no "
            "FROM patients p LEFT JOIN groups g ON g.patient_id=p.id "
            "ORDER BY p.id LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def _relations(conn, limit):
    rows = conn.execute(
        "SELECT r.id, r.patient_a, r.patient_b, r.status, "
        "r.created_at, r.revoked_at, "
        "pa.hospital AS ha, pa.hospital_mrn AS ma, "
        "pb.hospital AS hb, pb.hospital_mrn AS mb "
        "FROM relations r "
        "JOIN patients pa ON pa.id=r.patient_a "
        "JOIN patients pb ON pb.id=r.patient_b "
        "ORDER BY r.id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def _groups(conn):
    if not _group_published(conn):
        return None
    rows = conn.execute(
        "SELECT g.group_no, g.compute_id, p.id AS patient_id, "
        "p.hospital, p.hospital_mrn "
        "FROM groups g JOIN patients p ON p.id=g.patient_id "
        "ORDER BY g.group_no, p.id"
    ).fetchall()
    out = {}
    for r in rows:
        out.setdefault(
            r["group_no"],
            {"group_no": r["group_no"], "compute_id": r["compute_id"],
             "members": []},
        )["members"].append(
            {
                "patient_id": r["patient_id"],
                "hospital": r["hospital"],
                "hospital_mrn": r["hospital_mrn"],
            }
        )
    return list(out.values())


def list_patients(q=None, limit=200):
    with db.Snapshot() as conn:
        return _patients(conn, q, limit)


def list_relations(limit=200):
    with db.Snapshot() as conn:
        return _relations(conn, limit)


def list_groups():
    with db.Snapshot() as conn:
        return _groups(conn)


def failed_jobs():
    """分组/导入失败与拒绝原因（页面"拒绝原因"区域）。"""
    with db.Snapshot() as conn:
        rows = conn.execute(
            "SELECT id, kind, status, filename, detail, finished_at "
            "FROM jobs WHERE status='FAILED' ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def pending_jobs():
    with db.Snapshot() as conn:
        rows = conn.execute(
            "SELECT id, kind, status, filename, total_rows, done_rows, "
            "inserted, reused, rejected, seq_cutoff, created_at "
            "FROM jobs WHERE status IN ('PENDING','RUNNING') ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]


def export_mapping_csv():
    """生成编号对照 CSV。返回 (csv_text, state_info)；未发布时返回 None。"""
    with db.Snapshot() as conn:
        if not _group_published(conn):
            return None
        state = conn.execute(
            "SELECT MAX(compute_id) AS compute_id, "
            "COUNT(DISTINCT group_no) AS groups, COUNT(*) AS members "
            "FROM groups"
        ).fetchone()
        rows = conn.execute(
            "SELECT g.group_no, p.id AS record_no, p.hospital, "
            "p.hospital_mrn, g.compute_id "
            "FROM groups g JOIN patients p ON p.id=g.patient_id "
            "ORDER BY g.group_no, p.id"
        ).fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        ["group_no", "record_no", "hospital", "hospital_mrn", "compute_id"]
    )
    for r in rows:
        writer.writerow(
            [r["group_no"], r["record_no"], r["hospital"],
             r["hospital_mrn"], r["compute_id"]]
        )
    return buf.getvalue(), dict(state)


# ---- 名单核验 ----

VERIFICATION_STATUS_LABELS = {
    "OK": "一致",
    "UNMATCHED": "未匹配",
    "CONFLICT_GROUP": "同组多研究编号",
    "CONFLICT_STUDY": "研究编号多组",
    "INVALID": "格式错误",
}


def _latest_version(conn):
    """最新一个含成员的分组版本；空分组不视为可用版本。"""
    return conn.execute(
        "SELECT MAX(compute_id) AS c FROM group_version_members"
    ).fetchone()["c"]


def _report_dict(row, latest):
    d = dict(row)
    d["status_label"] = (
        "生成失败" if d["status"] == "FAILED" else "已完成"
    )
    # 过期按"当前最新已发布版本"动态判定，旧报告内容永远按原版本保留
    d["stale"] = (
        d["compute_id"] is not None
        and d["compute_id"] < (latest or 0)
    )
    return d


def verification_page(report_id=None):
    """核验页：报告列表、所选报告问题行、当前版本取自同一个快照。"""
    with db.Snapshot() as conn:
        latest = _latest_version(conn)
        reports = [
            _report_dict(r, latest)
            for r in conn.execute(
                "SELECT id, filename, status, compute_id, total_rows, "
                "matched_rows, unmatched_rows, invalid_rows, conflict_rows, "
                "ok_rows, detail, created_at, finished_at "
                "FROM verification_reports ORDER BY id DESC LIMIT 50"
            )
        ]
        selected = None
        rows = []
        if report_id is not None:
            row = conn.execute(
                "SELECT id, filename, status, compute_id, total_rows, "
                "matched_rows, unmatched_rows, invalid_rows, conflict_rows, "
                "ok_rows, detail, created_at, finished_at "
                "FROM verification_reports WHERE id=?",
                (report_id,),
            ).fetchone()
            if row is not None:
                selected = _report_dict(row, latest)
        elif reports:
            selected = reports[0]
        if selected is not None:
            rows = [
                dict(r)
                for r in conn.execute(
                    "SELECT row_no, study_id, hospital, hospital_mrn, "
                    "status, group_no, related_rows, related_groups, detail "
                    "FROM verification_rows WHERE report_id=? "
                    "ORDER BY row_no",
                    (selected["id"],),
                )
            ]
            # 页面只列问题行；一致行只计入统计
            rows = [r for r in rows if r["status"] != "OK"]
            for r in rows:
                r["status_label"] = VERIFICATION_STATUS_LABELS.get(
                    r["status"], r["status"]
                )
        return {
            "reports": reports,
            "selected": selected,
            "rows": rows,
            "latest_version": latest,
            "has_published": latest is not None,
        }


def export_verification_csv(report_id):
    """导出整份核验报告。报告不存在或生成失败时返回 None。

    报告与逐行结果都由创建时的同一事务写入且永不改写，因此下载内容
    恒为提交时绑定版本的完整结果，不会混入新数据。
    """
    with db.Snapshot() as conn:
        report = conn.execute(
            "SELECT * FROM verification_reports WHERE id=?", (report_id,)
        ).fetchone()
        if report is None or report["status"] != "DONE":
            return None
        latest = _latest_version(conn)
        rows = conn.execute(
            "SELECT row_no, study_id, hospital, hospital_mrn, status, group_no, "
            "related_rows, related_groups, detail "
            "FROM verification_rows WHERE report_id=? ORDER BY row_no",
            (report_id,),
        ).fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        ["group_version", "row_no", "study_id", "hospital", "hospital_mrn",
         "status", "status_label", "group_no", "related_rows",
         "related_groups", "detail"]
    )
    for r in rows:
        writer.writerow(
            [report["compute_id"], r["row_no"], r["study_id"], r["hospital"],
             r["hospital_mrn"], r["status"],
             VERIFICATION_STATUS_LABELS.get(r["status"], r["status"]),
             r["group_no"] if r["group_no"] is not None else "",
             r["related_rows"], r["related_groups"], r["detail"]]
        )
    return buf.getvalue(), _report_dict(report, latest)


def verification_report(report_id):
    with db.Snapshot() as conn:
        report = conn.execute(
            "SELECT * FROM verification_reports WHERE id=?", (report_id,)
        ).fetchone()
        if report is None:
            return None
        return _report_dict(report, _latest_version(conn))
