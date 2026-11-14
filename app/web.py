"""Flask Web 入口：上传导入、确认/撤销、进度查询、成员查看、下载。"""
from flask import (
    Flask, Response, flash, redirect, render_template, request, url_for,
)

from . import repository, services, worker
from .services import ServiceError


def create_app():
    app = Flask(__name__)
    app.secret_key = "multi-center-patient-mapping-demo"
    app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024  # 32MB

    worker.start_worker()

    @app.get("/")
    def index():
        stats, jobs, pending, failed = repository.index_page()
        return render_template(
            "index.html", stats=stats, jobs=jobs,
            pending=pending, failed=failed,
        )

    @app.post("/import")
    def import_csv():
        upload = request.files.get("file")
        if upload is None or upload.filename == "":
            flash("请选择要上传的患者 CSV 文件", "error")
            return redirect(url_for("index"))
        raw = upload.read()
        if not raw.strip():
            flash("上传文件为空", "error")
            return redirect(url_for("index"))
        try:
            job_id, total, parse_errors = services.create_import_job(
                upload.filename, raw
            )
        except UnicodeDecodeError:
            flash("文件不是有效的 UTF-8 文本 CSV", "error")
            return redirect(url_for("index"))
        flash(
            f"导入任务 #{job_id} 已提交，共 {total} 行有效数据，后台处理中。"
            + (" 解析提示：" + "；".join(parse_errors) if parse_errors else ""),
            "info",
        )
        return redirect(url_for("index"))

    @app.post("/relations/confirm")
    def confirm():
        a = request.form.get("record_a", type=int)
        b = request.form.get("record_b", type=int)
        if not a or not b:
            flash("请填写两条有效记录号", "error")
            return redirect(url_for("relations_view"))
        try:
            result = services.confirm_relation(a, b)
            flash(result["message"], "info")
        except ServiceError as exc:
            flash(str(exc), "error")
        return redirect(url_for("relations_view"))

    @app.post("/relations/revoke")
    def revoke():
        a = request.form.get("record_a", type=int)
        b = request.form.get("record_b", type=int)
        if not a or not b:
            flash("请填写两条有效记录号", "error")
            return redirect(url_for("relations_view"))
        try:
            result = services.revoke_relation(a, b)
            flash(result["message"], "info")
        except ServiceError as exc:
            flash(str(exc), "error")
        return redirect(url_for("relations_view"))

    @app.get("/relations")
    def relations_view():
        patients, rels, failed = repository.relations_page(
            request.args.get("q"))
        return render_template(
            "relations.html", patients=patients, rels=rels,
            failed=failed, q=request.args.get("q", ""),
        )

    @app.get("/groups")
    def groups_view():
        groups, pending, failed = repository.groups_page()
        return render_template(
            "groups.html", groups=groups, failed=failed, pending=pending,
        )

    @app.get("/download/mapping.csv")
    def download():
        result = repository.export_mapping_csv()
        if result is None:
            flash("分组尚未发布（首次计算进行中或当前存在冲突），暂无可下载对照",
                  "error")
            return redirect(url_for("groups_view"))
        csv_text, state = result
        resp = Response(csv_text, mimetype="text/csv; charset=utf-8")
        resp.headers["Content-Disposition"] = (
            "attachment; filename=patient_mapping.csv"
        )
        # 标注本次下载对应的完整状态版本
        resp.headers["X-Compute-Id"] = str(state["compute_id"])
        return resp

    # ---- 名单核验 ----

    @app.get("/verification")
    def verification_view():
        report_id = request.args.get("report_id", type=int)
        data = repository.verification_page(report_id)
        return render_template("verification.html", **data)

    @app.post("/verification")
    def verification_submit():
        upload = request.files.get("file")
        if upload is None or upload.filename == "":
            flash("请选择要核验的名单 CSV 文件", "error")
            return redirect(url_for("verification_view"))
        raw = upload.read()
        if not raw.strip():
            flash("上传文件为空", "error")
            return redirect(url_for("verification_view"))
        try:
            report_id = services.verify_csv(upload.filename, raw)
        except UnicodeDecodeError:
            flash("文件不是有效的 UTF-8 文本 CSV，核验未执行", "error")
            return redirect(url_for("verification_view"))
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("verification_view"))
        report = repository.verification_report(report_id)
        flash(
            f"核验报告 #{report_id} 已生成（绑定分组版本 "
            f"compute_id={report['compute_id']}）：一致 {report['ok_rows']} 行，"
            f"未匹配 {report['unmatched_rows']} 行，"
            f"冲突 {report['conflict_rows']} 行，"
            f"格式错误 {report['invalid_rows']} 行。",
            "info",
        )
        return redirect(url_for("verification_view", report_id=report_id))

    @app.get("/download/verification/<int:report_id>.csv")
    def download_verification(report_id):
        result = repository.export_verification_csv(report_id)
        if result is None:
            flash("该核验报告不存在或已生成失败，无可下载内容", "error")
            return redirect(url_for("verification_view"))
        csv_text, report = result
        resp = Response(csv_text, mimetype="text/csv; charset=utf-8")
        resp.headers["Content-Disposition"] = (
            f"attachment; filename=verification_report_{report_id}.csv"
        )
        # 报告内容按其绑定的原版本生成，与当前最新版本无关
        resp.headers["X-Group-Version"] = str(report["compute_id"])
        resp.headers["X-Report-Stale"] = "1" if report["stale"] else "0"
        return resp

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app
