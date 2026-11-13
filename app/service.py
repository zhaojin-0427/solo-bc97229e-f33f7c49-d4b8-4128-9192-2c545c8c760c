"""入湖编排：暂存 -> 解析校验 -> 写 Parquet -> 短事务登记目录。

不变量：
1. 解析与写文件期间不持有目录库写事务；
2. 目录只引用已完整落盘的文件（先 rename 后登记；登记失败删文件）；
3. 坏整数 / 写盘失败 / 登记失败 => 整批不发布，不留残件引用。
"""
import datetime
import os
import re
import shutil
import time
import uuid

from . import catalog, csvparse, retention, store

_TOKEN_RE = re.compile(r"[0-9a-f]{32}")
STAGING_MAX_AGE_SEC = 3600
ORPHAN_MAX_AGE_SEC = 600


def init_storage(cfg):
    os.makedirs(cfg.data_dir, exist_ok=True)
    os.makedirs(cfg.parquet_dir, exist_ok=True)
    os.makedirs(cfg.staging_dir, exist_ok=True)
    catalog.init_db(cfg.db_path)
    cleanup_staging(cfg)
    cleanup_orphan_files(cfg)
    # 接续上次进程中断的回收：已撤回批次不会恢复可见性，也不会永久占盘
    retention.recover(cfg)


# ---------- 暂存（声明列类型的两步表单之间） ----------

def stage_upload(cfg, stream):
    token = uuid.uuid4().hex
    path = os.path.join(cfg.staging_dir, token + ".csv")
    with open(path, "wb") as out:
        shutil.copyfileobj(stream, out, 256 * 1024)
    return token


def staged_path(cfg, token):
    if not _TOKEN_RE.fullmatch(token or ""):
        raise ValueError("无效的暂存令牌")
    path = os.path.join(cfg.staging_dir, token + ".csv")
    if not os.path.isfile(path):
        raise ValueError("暂存文件不存在或已过期，请重新上传")
    return path


def staged_header(cfg, token):
    return csvparse.read_header(staged_path(cfg, token))


def discard_staged(cfg, token):
    if _TOKEN_RE.fullmatch(token or ""):
        try:
            os.unlink(os.path.join(cfg.staging_dir, token + ".csv"))
        except OSError:
            pass


def retract_batch(cfg, batch_ref, note=""):
    """撤回整批观测（保留站点 / 文件名 / 行数 / 撤回记录）。"""
    result = retention.retract(cfg, batch_ref, note)
    if result["status"] == "retracted":
        retention.reclaim(cfg, batch_id=result["batch"]["id"])
        retention.wake_reclaimer(cfg)
    return result


# ---------- 发布 ----------

def publish(cfg, station, source_filename, csv_path, col_types):
    """校验并发布一个批次。任何失败都整批不发布。"""
    station = (station or "").strip()
    if not station:
        raise ValueError("站点不能为空")
    if len(station) > 200:
        raise ValueError("站点名过长")
    header = csvparse.read_header(csv_path)
    species_names = header[1:]
    if len(species_names) != len(col_types):
        raise ValueError("列类型数量与 CSV 鸟种列数不一致")
    for t in col_types:
        if t not in catalog.COLUMN_TYPES:
            raise ValueError(f"非法列类型: {t!r}")
    if store.DATE_COLUMN in species_names:
        raise csvparse.CsvError(f"鸟种列不能使用保留列名 {store.DATE_COLUMN}")

    # 1) 解析与校验（不持有目录库写事务）；坏整数在此整批拒绝
    dates, values = csvparse.parse_rows(csv_path, col_types)
    columns = list(zip(species_names, col_types, values))
    table = store.build_batch_table(dates, columns)

    # 2) 写独立 Parquet 文件（不持有目录库写事务）；写盘失败整批不发布
    batch_id = uuid.uuid4().hex
    final_name = batch_id + ".parquet"
    final_path = os.path.join(cfg.parquet_dir, final_name)
    store.write_parquet_atomic(table, final_path)

    # 3) 短事务：合并逻辑结构 + 登记批次（并发发布者在此串行）
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            changes = catalog.merge_schema(conn, [(n, t) for n, t, _ in columns])
            catalog.insert_batch(conn, {
                "id": batch_id,
                "station": station,
                "source_filename": source_filename or "",
                "parquet_file": final_name,
                "n_rows": table.num_rows,
                "created_at": datetime.datetime.now(datetime.timezone.utc)
                              .isoformat(timespec="seconds"),
            })
    except BaseException:
        try:
            os.unlink(final_path)  # 目录未登记 => 删除未发布文件，目录永不引用残件
        except OSError:
            pass
        raise
    finally:
        conn.close()
    return {"batch_id": batch_id, "station": station,
            "n_rows": table.num_rows, "changes": changes}


# ---------- 启动清理 ----------

def cleanup_staging(cfg, max_age_sec=STAGING_MAX_AGE_SEC):
    now = time.time()
    for name in os.listdir(cfg.staging_dir):
        path = os.path.join(cfg.staging_dir, name)
        try:
            if now - os.path.getmtime(path) > max_age_sec:
                os.unlink(path)
        except OSError:
            pass


def cleanup_orphan_files(cfg, max_age_sec=ORPHAN_MAX_AGE_SEC):
    """清理崩溃遗留的临时文件与从未登记的残件。

    宽限期（max_age_sec）避免误删其他进程刚 rename、尚未登记的批次文件；
    已撤回、等待回收的批次仍被目录引用，由 retention 回收流程处理，
    这里绝不抢先删除（避免绕过租约、误删读者正在读的文件）。"""
    conn = catalog.connect(cfg.db_path)
    try:
        # 含已撤回批次：其文件归回收流程管，孤儿清理只管从未登记的残件
        referenced = {r["parquet_file"] for r in conn.execute("SELECT parquet_file FROM batches")}
    finally:
        conn.close()
    now = time.time()
    for name in os.listdir(cfg.parquet_dir):
        path = os.path.join(cfg.parquet_dir, name)
        try:
            if name.startswith(".") and name.endswith(".tmp"):
                if now - os.path.getmtime(path) > max_age_sec:
                    os.unlink(path)
            elif name.endswith(".parquet") and name not in referenced:
                if now - os.path.getmtime(path) > max_age_sec:
                    os.unlink(path)
        except OSError:
            pass
