"""业务服务层：导入入队、关系确认/撤销、重算调度。

所有写操作通过 db.write_tx 串行化，确认/撤销在同一 IMMEDIATE 事务内
基于"提交时的完整关系"做判定，保证并发确认的原子语义。
"""
import csv
import io
import os
import uuid

from . import config, db, groups as group_mod


class ServiceError(Exception):
    """可直接展示给用户的业务错误。"""


MAX_VERIFY_ROWS = 1000
VERIFY_STATUS_LABELS = {
    "OK": "一致",
    "UNMATCHED": "未匹配",
    "CONFLICT_GROUP": "同组多研究编号",
    "CONFLICT_STUDY": "研究编号多组",
    "INVALID": "格式错误",
}


def _canonical_pair(a, b):
    lo, hi = (a, b) if a < b else (b, a)
    return lo, hi


def get_patient(conn, pid):
    row = conn.execute(
        "SELECT id, hospital, hospital_mrn FROM patients WHERE id=?",
        (pid,),
    ).fetchone()
    if row is None:
        raise ServiceError(f"记录号 {pid} 不存在")
    return row


def _active_edges(conn):
    return [
        (r["patient_a"], r["patient_b"])
        for r in conn.execute(
            "SELECT patient_a, patient_b FROM relations "
            "WHERE status='ACTIVE'"
        )
    ]


def _would_conflict(components_by_pid, a, b):
    """模拟把 a、b 合并后是否违反"同组同医院唯一编号"约束。"""
    ga = components_by_pid.get(a)
    gb = components_by_pid.get(b)
    if ga is None or gb is None or ga is gb:
        return None  # 不产生新合并，无新增冲突
    hospital_mrns = {}
    for comp in (ga, gb):
        for pid, hospital, mrn in comp["members"]:
            hospital_mrns.setdefault(hospital, set()).add(mrn)
    for hospital, mrns in hospital_mrns.items():
        if len(mrns) > 1:
            return (
                f"合并后医院 {hospital} 在同组内出现不同院内编号 "
                f"{sorted(mrns)}，违反同组唯一编号约束"
            )
    return None


def confirm_relation(a_id, b_id):
    """确认两条记录为同人。成功返回 dict，冲突/错误抛 ServiceError。"""

    def tx(conn):
        pa = get_patient(conn, a_id)
        pb = get_patient(conn, b_id)
        if pa["id"] == pb["id"]:
            raise ServiceError("不能将同一条记录与自身建立同人关系")

        lo, hi = _canonical_pair(pa["id"], pb["id"])

        existing = conn.execute(
            "SELECT id, status FROM relations "
            "WHERE patient_a=? AND patient_b=? ORDER BY id DESC LIMIT 1",
            (lo, hi),
        ).fetchone()
        if existing and existing["status"] == "ACTIVE":
            return {
                "outcome": "already_active",
                "relation_id": existing["id"],
                "message": "该同人关系已确认，重复提交已幂等复用",
            }

        # 提交时完整关系上构建分量（计算在写事务内，但仅为内存运算，
        # 不涉及额外的数据库写；外部导入/查询使用独立连接不受阻塞）
        patients, edges, _ = group_mod.read_snapshot(conn)
        components, violations = group_mod.build_components(patients, edges)
        by_pid = {}
        for comp in components:
            for pid, _h, _m in comp["members"]:
                by_pid[pid] = comp

        # 已有违规时拒绝一切新确认，要求先撤销矛盾关系
        if violations:
            raise ServiceError(
                "当前关系已存在未解决的同医院多编号冲突："
                + "；".join(violations)
            )

        reason = _would_conflict(by_pid, lo, hi)
        if reason:
            raise ServiceError(
                f"拒绝确认（记录 {lo} ↔ {hi}）：{reason}"
            )

        cur = conn.execute(
            "INSERT INTO relations(patient_a, patient_b, status) "
            "VALUES (?, ?, 'ACTIVE')",
            (lo, hi),
        )
        rel_id = cur.lastrowid
        seq = conn.execute("SELECT MAX(id) AS m FROM relations").fetchone()["m"]
        enqueue_recompute(conn, seq)
        return {
            "outcome": "confirmed",
            "relation_id": rel_id,
            "message": f"已确认记录 {lo} 与 {hi} 的同人关系",
        }

    return db.write_tx(tx)


def revoke_relation(a_id, b_id):
    """撤销两条记录间的同人关系；按剩余关系重新拆组。"""

    def tx(conn):
        pa = get_patient(conn, a_id)
        pb = get_patient(conn, b_id)
        if pa["id"] == pb["id"]:
            raise ServiceError("不能撤销记录与自身的关系")
        lo, hi = _canonical_pair(pa["id"], pb["id"])

        row = conn.execute(
            "SELECT id FROM relations "
            "WHERE patient_a=? AND patient_b=? AND status='ACTIVE'",
            (lo, hi),
        ).fetchone()
        if row is None:
            raise ServiceError(
                f"记录 {lo} 与 {hi} 之间不存在有效的（ACTIVE）同人关系，"
                "无可撤销内容"
            )
        conn.execute(
            "UPDATE relations SET status='REVOKED', "
            "revoked_at=datetime('now') WHERE id=?",
            (row["id"],),
        )
        seq = conn.execute("SELECT MAX(id) AS m FROM relations").fetchone()["m"]
        enqueue_recompute(conn, seq)
        return {
            "outcome": "revoked",
            "relation_id": row["id"],
            "message": f"已撤销记录 {lo} 与 {hi} 的同人关系，将按剩余关系重新分组",
        }

    return db.write_tx(tx)


def parse_verification_csv(text, max_rows=MAX_VERIFY_ROWS):
    """解析核验 CSV，返回 (rows, errors)。

    rows: [dict(row_no, study_id, hospital, hospital_mrn)]
    errors: [str] 文件级问题（表头缺失/超过行数上限）；无有效行时拒绝核验。
    要求表头含研究编号、医院、院内编号三列（中英文均可）。
    单行缺列或空值时保留该行，由核验阶段标为 INVALID 并展示行号。
    """
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return [], ["CSV 为空，缺少表头"]

    normalized = [h.strip().strip("﻿").lower() for h in header]
    alias = {
        "study_id": "study_id", "studyno": "study_id",
        "研究编号": "study_id", "研究号": "study_id", "项目编号": "study_id",
        "hospital": "hospital", "医院": "hospital", "hospital_code": "hospital",
        "hospital_id": "hospital", "中心": "hospital", "医院编码": "hospital",
        "mrn": "mrn", "hospital_mrn": "mrn", "院内编号": "mrn",
        "医院内编号": "mrn", "编号": "mrn", "本地编号": "mrn",
    }
    cols = {}
    for idx, name in enumerate(normalized):
        key = alias.get(name)
        if key and key not in cols:
            cols[key] = idx
    missing = [k for k in ("study_id", "hospital", "mrn") if k not in cols]
    if missing:
        return [], [
            "表头必须包含研究编号（study_id/研究编号）、医院（hospital/医院）"
            "与院内编号（mrn/院内编号）三列，实际表头：" + str(header)
        ]

    def cell(record, key):
        idx = cols[key]
        return record[idx].strip() if idx < len(record) else ""

    rows = []
    for line_no, record in enumerate(reader, start=2):
        if not any(cell.strip() for cell in record):
            continue  # 跳过空行
        if len(rows) >= max_rows:
            return rows, [
                f"数据行超过上限 {max_rows} 行，本次核验未执行，"
                "请拆分文件后重试"
            ]
        rows.append(
            {
                "row_no": line_no,
                "study_id": cell(record, "study_id"),
                "hospital": cell(record, "hospital"),
                "hospital_mrn": cell(record, "mrn"),
            }
        )
    return rows, []


def verify_csv(filename, raw_bytes):
    """核验提交名单：读取并绑定提交时的已发布分组版本，存为一次性报告。

    计算完全在只读快照中完成（不占写事务）；报告元数据与逐行结果在
    同一个写事务内保存，任何失败都不会留下可下载的半份结果。
    返回报告 id。
    """
    text = raw_bytes.decode("utf-8-sig")
    rows, errors = parse_verification_csv(text)
    safe = os.path.basename(filename or "verification.csv").replace("/", "_")

    if errors:
        raise ServiceError("核验未执行：" + "；".join(errors))
    if not rows:
        raise ServiceError("核验未执行：文件中没有有效数据行")

    # 绑定提交时刻的已发布版本：先在只读快照中读取版本与成员并完成计算
    # （不占写事务）；保存报告时在同一写事务内确认版本仍是最新，若期间
    # 有新版本发布则重取并重算，保证报告版本与提交时的一致状态对应。
    for _attempt in range(5):
        with db.Snapshot() as conn:
            version = conn.execute(
                "SELECT MAX(compute_id) AS c FROM group_version_members"
            ).fetchone()["c"]
            if version is None:
                raise ServiceError(
                    "尚无已发布分组，无法核验名单；请先导入患者并等待分组发布"
                )
            members = [
                dict(r)
                for r in conn.execute(
                    "SELECT group_no, patient_id, hospital, hospital_mrn "
                    "FROM group_version_members WHERE compute_id=? "
                    "ORDER BY group_no, patient_id",
                    (version,),
                )
            ]
        # —— 快照已关闭，计算在事务外进行 ——
        results, counts = _evaluate_rows(rows, members)
        try:
            return _save_report(safe, version, errors, results, counts)
        except _StaleVersion:
            continue
    raise ServiceError("分组持续更新，无法取得稳定版本，请稍后重试")


class _StaleVersion(Exception):
    """内部信号：保存报告时发现分组版本已更新，需要按新版本重算。"""


def _save_report(filename, version, errors, results, counts):
    """在一个写事务内一次性保存报告元数据与全部逐行结果。

    版本校验与插入同事务：不存在"元数据已写、明细未写"的半份报告。
    """

    def tx(conn):
        latest = conn.execute(
            "SELECT MAX(compute_id) AS c FROM group_version_members"
        ).fetchone()["c"]
        if latest != version:
            raise _StaleVersion()
        cur = conn.execute(
            "INSERT INTO verification_reports"
            "(filename, status, compute_id, total_rows, matched_rows, "
            "unmatched_rows, invalid_rows, conflict_rows, ok_rows, detail, "
            "finished_at) "
            "VALUES (?, 'DONE', ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                filename,
                version,
                len(results),
                counts["matched"],
                counts["unmatched"],
                counts["invalid"],
                counts["conflict"],
                counts["ok"],
                "；".join(errors),
            ),
        )
        report_id = cur.lastrowid
        conn.executemany(
            "INSERT INTO verification_rows"
            "(report_id, row_no, study_id, hospital, hospital_mrn, status, "
            "group_no, related_rows, related_groups, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    report_id, r["row_no"], r["study_id"], r["hospital"],
                    r["hospital_mrn"], r["status"], r.get("group_no"),
                    r.get("related_rows", ""), r.get("related_groups", ""),
                    r.get("detail", ""),
                )
                for r in results
            ],
        )
        return report_id

    return db.write_tx(tx)


def _evaluate_rows(rows, members):
    """把提交行与给定版本的分组成员比对，返回 (逐行结果, 计数)。

    规则：
    - 医院+院内编号不在该版本分组中 -> UNMATCHED（未匹配）
    - 同一研究编号对应多个组 -> CONFLICT_STUDY（报告涉及行与组）
    - 同组对应多个研究编号 -> CONFLICT_GROUP（报告涉及行与组）
    - 同一研究编号只落在同一个组 -> OK
    """
    by_key = {(m["hospital"], m["hospital_mrn"]): m for m in members}

    # 先做单行匹配，再按研究编号/组聚合，冲突需要看到全部行
    results = []
    for row in rows:
        if not row["study_id"] or not row["hospital"] or not row["hospital_mrn"]:
            results.append(
                {
                    **row,
                    "status": "INVALID",
                    "related_groups": "",
                    "related_rows": "",
                    "detail": "研究编号、医院、院内编号均不能为空",
                }
            )
            continue
        member = by_key.get((row["hospital"], row["hospital_mrn"]))
        if member is None:
            results.append(
                {
                    **row,
                    "status": "UNMATCHED",
                    "related_groups": "",
                    "related_rows": str(row["row_no"]),
                    "detail": "该医院与院内编号组合未收录于当前分组版本",
                }
            )
            continue
        results.append(
            {**row, "status": "OK", "group_no": member["group_no"]}
        )

    # 研究编号 -> 涉及的有效行（含 group_no）
    study_rows = {}
    group_rows = {}
    for r in results:
        if r["status"] != "OK":
            continue
        study_rows.setdefault(r["study_id"], []).append(r)
        group_rows.setdefault(r["group_no"], []).append(r)

    for study_id, rs in study_rows.items():
        groups = sorted({r["group_no"] for r in rs})
        if len(groups) > 1:
            line_nos = [r["row_no"] for r in rs]
            for r in rs:
                r["status"] = "CONFLICT_STUDY"
                r["related_rows"] = ",".join(str(n) for n in line_nos)
                r["related_groups"] = ",".join(str(g) for g in groups)
                r["detail"] = (
                    f"研究编号 {study_id} 在名单中出现于多个组 {groups}，"
                    f"涉及 CSV 行 {line_nos}，请合作方更正"
                )

    for group_no, rs in group_rows.items():
        studies = sorted({r["study_id"] for r in rs})
        if len(studies) > 1:
            line_nos = [r["row_no"] for r in rs]
            for r in rs:
                if r["status"] == "CONFLICT_STUDY":
                    # 同时涉及研究编号多组：两类冲突一并注明
                    r["detail"] += (
                        f"；同组 {group_no} 内还出现多个研究编号 {studies}"
                    )
                else:
                    r["status"] = "CONFLICT_GROUP"
                    r["detail"] = (
                        f"同组 {group_no} 内出现多个研究编号 {studies}，"
                        f"涉及 CSV 行 {line_nos}，请合作方更正"
                    )
                r["related_rows"] = ",".join(str(n) for n in line_nos)
                r["related_groups"] = ",".join(str(g) for g in sorted(
                    {rr["group_no"] for rr in rs}
                ))

    counts = {"ok": 0, "unmatched": 0, "invalid": 0, "conflict": 0, "matched": 0}
    for r in results:
        if r["status"] == "OK":
            counts["ok"] += 1
            counts["matched"] += 1
        elif r["status"] == "UNMATCHED":
            counts["unmatched"] += 1
        elif r["status"] == "INVALID":
            counts["invalid"] += 1
        else:
            counts["conflict"] += 1
            counts["matched"] += 1
    return results, counts


def enqueue_recompute(conn, seq_cutoff):
    """安排一次后台重算。

    规则：若已有 PENDING 任务，复用最新的 seq_cutoff（合并抖动）；
    否则新建。RUNNING 的任务不改动，其发布时会因 seq 过期自行作废并
    由后续 enqueue 补上新任务，迟到计算不会恢复旧分组。
    """
    pending = conn.execute(
        "SELECT id, seq_cutoff FROM jobs "
        "WHERE kind='RECOMPUTE' AND status='PENDING' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if pending:
        new_seq = max(seq_cutoff or 0, pending["seq_cutoff"] or 0)
        conn.execute(
            "UPDATE jobs SET seq_cutoff=? WHERE id=?",
            (new_seq, pending["id"]),
        )
        return pending["id"]
    cur = conn.execute(
        "INSERT INTO jobs(kind, status, seq_cutoff) "
        "VALUES ('RECOMPUTE', 'PENDING', ?)",
        (seq_cutoff,),
    )
    return cur.lastrowid


def create_import_job(filename, raw_bytes):
    """保存上传文件并创建 IMPORT 任务，返回 job_id。"""
    safe = os.path.basename(filename).replace("/", "_")
    stored = f"{uuid.uuid4().hex}_{safe}"
    path = os.path.join(config.UPLOAD_DIR, stored)
    with open(path, "wb") as fh:
        fh.write(raw_bytes)

    # 先解析一遍，得到总行数与可读错误（正式入库仍在 worker 中逐行做）
    text = raw_bytes.decode("utf-8-sig")
    rows, parse_errors = parse_csv(text)
    total = len(rows)

    def tx(conn):
        detail = "; ".join(parse_errors)
        cur = conn.execute(
            "INSERT INTO jobs(kind, status, filename, total_rows, detail, "
            "file_path) VALUES ('IMPORT', 'PENDING', ?, ?, ?, ?)",
            (safe, total, detail, path),
        )
        return cur.lastrowid

    job_id = db.write_tx(tx)
    return job_id, total, parse_errors


def parse_csv(text):
    """解析 CSV 文本，返回 (rows[(hospital, mrn)], errors[str])。

    要求表头包含 hospital(医院) 与 mrn(院内编号) 两列（中英文均可）。
    """
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return [], ["CSV 为空，缺少表头"]

    normalized = [h.strip().strip("﻿").lower() for h in header]
    alias = {
        "hospital": "hospital", "医院": "hospital", "hospital_code": "hospital",
        "mrn": "mrn", "hospital_mrn": "mrn", "院内编号": "mrn",
        "医院内编号": "mrn", "患者编号": "mrn", "编号": "mrn",
    }
    cols = {}
    for idx, name in enumerate(normalized):
        key = alias.get(name)
        if key and key not in cols:
            cols[key] = idx
    if "hospital" not in cols or "mrn" not in cols:
        return [], [
            "表头必须包含医院列（hospital/医院）与院内编号列（mrn/院内编号），"
            f"实际表头：{header}"
        ]

    rows, errors = [], []
    seen_in_file = set()
    for line_no, record in enumerate(reader, start=2):
        if not any(cell.strip() for cell in record):
            continue  # 跳过空行
        try:
            hospital = record[cols["hospital"]].strip()
            mrn = record[cols["mrn"]].strip()
        except IndexError:
            errors.append(f"第 {line_no} 行列数不足，已跳过")
            continue
        if not hospital or not mrn:
            errors.append(f"第 {line_no} 行医院或院内编号为空，已跳过")
            continue
        key = (hospital, mrn)
        if key in seen_in_file:
            errors.append(
                f"第 {line_no} 行 ({hospital}, {mrn}) 在文件内重复，已跳过"
            )
            continue
        seen_in_file.add(key)
        rows.append((hospital, mrn))
    return rows, errors
