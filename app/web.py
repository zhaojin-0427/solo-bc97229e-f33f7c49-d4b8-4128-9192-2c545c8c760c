"""Flask Web 入口：上传批次（两步：上传 -> 逐列声明类型）、按站点预览、下载统一结构 CSV、
按批次 ID 撤回、回收状态查看与重试。

取数（预览 / 下载）先固定快照（短事务登记租约），提交后立即放开目录库；
慢读取期间不持有任何库事务，因此不会阻塞撤回、其他上传或取数。
"""
import datetime
import io
import os

import jinja2
from flask import (Flask, flash, redirect, render_template, request,
                   send_file, url_for)

from . import catalog, csvparse, retention, service, store
from .config import Config

MAX_UPLOAD_BYTES = 64 * 1024 * 1024

_RETRACT_ERRORS = {
    "empty": "请填写批次 ID",
    "not_found": "找不到该批次（请检查批次 ID）",
    "ambiguous": "批次 ID 前缀不唯一，请填写完整批次 ID",
}
_RECLAIM_HINTS = {
    "not_found": "找不到该批次",
    "not_retracted": "该批次尚未撤回，无可回收文件",
}


def create_app(config=None):
    cfg = config or Config()
    service.init_storage(cfg)
    retention.start_reclaimer(cfg)

    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
    app.secret_key = os.environ.get("SECRET_KEY", "bird-lake-dev-secret")
    app.extensions["lake_cfg"] = cfg

    # 只缓存模板编译结果（文件 IO 复用），但每次都从磁盘 stat 模板：
    # 多进程部署下别处更新模板不会被旧缓存覆盖；Jinja 的编译缓存是线程安全的。
    tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
    app.jinja_env.cache = {}
    app.jinja_env.loader = jinja2.FileSystemLoader(tmpl_dir)
    app.jinja_env.auto_reload = True

    def snapshot(station=None):
        conn = catalog.connect(cfg.db_path)
        try:
            return catalog.snapshot(conn, station)
        finally:
            conn.close()

    def all_batches(include_retracted=True):
        conn = catalog.connect(cfg.db_path)
        try:
            return catalog.all_batches(conn, include_retracted=include_retracted)
        finally:
            conn.close()

    @app.get("/")
    def index():
        snap = snapshot()
        stations = {}
        for b in snap["batches"]:
            s = stations.setdefault(
                b["station"], {"station": b["station"], "n_batches": 0, "n_rows": 0})
            s["n_batches"] += 1
            s["n_rows"] += b["n_rows"]
        recent = list(reversed(snap["batches"]))[:50]
        return render_template("index.html", snap=snap,
                               stations=sorted(stations.values(), key=lambda x: x["station"]),
                               recent=recent)

    @app.get("/upload")
    def upload_form():
        return render_template("upload.html")

    @app.post("/upload")
    def upload():
        station = (request.form.get("station") or "").strip()
        f = request.files.get("file")
        if not station:
            flash("请填写站点", "err")
            return redirect(url_for("upload_form"))
        if f is None or not f.filename:
            flash("请选择 CSV 文件", "err")
            return redirect(url_for("upload_form"))
        token = service.stage_upload(cfg, f.stream)
        try:
            header = service.staged_header(cfg, token)
        except csvparse.CsvError as e:
            service.discard_staged(cfg, token)
            flash(f"CSV 表头无效：{e}", "err")
            return redirect(url_for("upload_form"))
        if store.DATE_COLUMN in header[1:]:
            service.discard_staged(cfg, token)
            flash(f"鸟种列不能使用保留列名 {store.DATE_COLUMN}", "err")
            return redirect(url_for("upload_form"))
        return render_template("declare.html", token=token, station=station,
                               filename=f.filename, date_col=header[0],
                               columns=header[1:])

    @app.post("/publish")
    def publish():
        token = request.form.get("token") or ""
        station = (request.form.get("station") or "").strip()
        filename = request.form.get("filename") or ""
        try:
            path = service.staged_path(cfg, token)
        except ValueError as e:
            flash(str(e), "err")
            return redirect(url_for("upload_form"))
        try:
            header = service.staged_header(cfg, token)
            types = [request.form.get(f"type_{i}", "") for i in range(len(header) - 1)]
            result = service.publish(cfg, station, filename, path, types)
        except (csvparse.CsvError, ValueError) as e:
            flash(f"批次未发布：{e}", "err")
            return redirect(url_for("upload_form"))
        except OSError as e:
            flash(f"批次未发布：写入存储失败（{e}）", "err")
            return redirect(url_for("upload_form"))
        except Exception as e:
            # 任何未预期错误同样整批不发布（publish 已保证失败无残件登记）
            app.logger.exception("publish failed")
            flash(f"批次未发布：{type(e).__name__}: {e}", "err")
            return redirect(url_for("upload_form"))
        finally:
            service.discard_staged(cfg, token)
        parts = [f"批次 {result['batch_id'][:8]} 已发布：站点「{result['station']}」，{result['n_rows']} 行"]
        for c in result["changes"]:
            if c["change"] == "added":
                parts.append(f"新增列 {c['name']}({c['type']})")
            elif c["change"] == "promoted":
                parts.append(f"列 {c['name']} 类型提升为 text")
        flash("；".join(parts), "ok")
        return redirect(url_for("preview", station=result["station"]))

    @app.get("/preview")
    def preview():
        station = request.args.get("station") or None
        try:
            limit = max(1, min(int(request.args.get("limit", "200")), 5000))
        except ValueError:
            limit = 200
        # 固定快照：短事务登记租约后立刻放开库锁；读取期间不阻塞撤回/上传/取数
        snapshot_pin = retention.pin_snapshot(cfg, station)
        try:
            schema, table = snapshot_pin.load_unified()
        finally:
            snapshot_pin.release()
        rows = table.slice(0, limit).to_pylist()
        all_stations = sorted({b["station"] for b in snapshot()["batches"]})
        return render_template("preview.html", station=station, stations=all_stations,
                               columns=schema.names, rows=rows, total=table.num_rows,
                               limit=limit, version=snapshot_pin.schema_version)

    @app.get("/download.csv")
    def download_csv():
        station = request.args.get("station") or None
        snapshot_pin = retention.pin_snapshot(cfg, station)
        try:
            _schema, table = snapshot_pin.load_unified()
            data = store.table_to_csv_bytes(table)
        finally:
            snapshot_pin.release()
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"birds_{station or 'all'}_{ts}.csv"
        return send_file(io.BytesIO(data), mimetype="text/csv",
                         as_attachment=True, download_name=name)

    # ---------- 撤回 ----------

    @app.post("/retract")
    def retract():
        batch_ref = (request.form.get("batch_id") or "").strip()
        note = (request.form.get("note") or "").strip()
        result = service.retract_batch(cfg, batch_ref, note)
        if result["status"] in ("retracted", "already_retracted"):
            b = result["batch"]
            if result["status"] == "retracted":
                flash(f"批次 {b['id'][:8]} 已撤回：站点「{b['station']}」，"
                      f"{b['n_rows']} 行；新预览与下载已排除该批。", "ok")
            else:
                flash(f"批次 {b['id'][:8]} 此前已撤回（{b['retracted_at']}），"
                      "沿用原撤回记录，未做任何修改。", "ok")
            return redirect(url_for("batches"))
        flash(_RETRACT_ERRORS.get(result["status"], "撤回失败"), "err")
        nxt = request.form.get("next") or ""
        # 只允许站内相对路径，防止开放重定向
        if not nxt.startswith("/") or nxt.startswith("//"):
            nxt = url_for("batches")
        return redirect(nxt)

    @app.get("/batches")
    def batches():
        return render_template("batches.html", batches=all_batches())

    # ---------- 回收 ----------

    @app.post("/reclaim")
    def reclaim_route():
        batch_id = (request.form.get("batch_id") or "").strip()
        results = retention.reclaim(cfg, batch_id=batch_id)
        if results:
            r = results[0]
            if r["state"] == "done":
                flash("回收完成：该批 Parquet 已删除，撤回记录保留。", "ok")
            elif r["state"] == "pending":
                flash(f"待回收：仍有 {r.get('active_leases', 0)} 个读者租约在用，"
                      "释放后将自动回收（也可稍后重试）。", "ok")
            elif r["state"] == "error":
                flash(f"删除错误：{r.get('error', '')}（可重试）", "err")
            else:
                flash(_RECLAIM_HINTS.get(r["state"], "无法回收该批次"), "err")
        else:
            flash("当前没有待回收的批次。", "ok")
        return redirect(url_for("batches"))

    @app.post("/reclaim-pending")
    def reclaim_pending_route():
        results = retention.reclaim(cfg, include_error=True)
        counts = {"done": 0, "pending": 0, "error": 0}
        for r in results:
            counts[r["state"]] = counts.get(r["state"], 0) + 1
        flash(f"回收重试：完成 {counts['done']} 个，待回收 {counts['pending']} 个，"
              f"删除错误 {counts['error']} 个。", "ok")
        return redirect(url_for("batches"))

    @app.get("/healthz")
    def healthz():
        return "ok", 200

    @app.errorhandler(413)
    def too_large(_e):
        flash("文件过大（上限 64MB）", "err")
        return redirect(url_for("upload_form"))

    return app


app = create_app()
