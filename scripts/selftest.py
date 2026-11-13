#!/usr/bin/env python3
"""端到端自检：CSV 校验、发布、结构合并、类型提升、并发、重启持久化、
批次撤回 / 固定快照 / 租约回收 / 崩溃接续、Web 入口。

用法（在仓库根目录，容器内或装好依赖的环境）：
    python scripts/selftest.py
"""
import io
import multiprocessing
import os
import re
import sys
import tempfile
import threading
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import catalog, csvparse, retention, service, store  # noqa: E402
from app.config import Config  # noqa: E402


def check(name, cond, extra=""):
    if cond:
        print(f"  ✓ {name}")
    else:
        raise AssertionError(f"{name}  {extra}")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def make_cfg(tmp):
    cfg = Config(tmp)
    service.init_storage(cfg)
    return cfg


def snap_of(cfg, station=None):
    conn = catalog.connect(cfg.db_path)
    try:
        return catalog.snapshot(conn, station)
    finally:
        conn.close()


def batch_row(cfg, batch_id):
    conn = catalog.connect(cfg.db_path)
    try:
        return dict(conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone())
    finally:
        conn.close()


def publish_text(cfg, tmp, name, text, types, station="站A"):
    path = os.path.join(tmp, name)
    write(path, text)
    return service.publish(cfg, station, name, path, types)


# ---------- 1. CSV 解析与校验 ----------

def test_parser(tmp):
    p = os.path.join(tmp, "a.csv")
    write(p, "日期,麻雀,白鹭\n2024-01-01,3,x y\n2024-01-02,,5\n")
    header = csvparse.read_header(p)
    check("表头解析", header == ["日期", "麻雀", "白鹭"])
    dates, cols = csvparse.parse_rows(p, ["uint64", "text"])
    check("日期解析", str(dates[0]) == "2024-01-01")
    check("空单元格为NULL(uint64)", cols[0] == [3, None])
    check("文本列保留原文", cols[1] == ["x y", "5"])

    def expect_csv_error(name, content, types=("uint64",)):
        write(p, content)
        try:
            csvparse.parse_rows(p, list(types))
        except csvparse.CsvError:
            print(f"  ✓ {name}")
        else:
            raise AssertionError(name + " 未被拒绝")

    expect_csv_error("负数拒绝", "d,麻雀\n2024-01-01,-3\n")
    expect_csv_error("非数字拒绝", "d,麻雀\n2024-01-01,12x\n")
    expect_csv_error("超出uint64拒绝", "d,麻雀\n2024-01-01,18446744073709551616\n")
    expect_csv_error("坏日期拒绝", "d,麻雀\n2024-13-01,1\n")
    expect_csv_error("字段数不一致拒绝", "d,麻雀\n2024-01-01,1,2\n")
    expect_csv_error("无数据行拒绝", "d,麻雀\n")

    write(p, "d,麻雀\n2024-01-01,18446744073709551615\n")
    _, cols = csvparse.parse_rows(p, ["uint64"])
    check("uint64上限可用", cols[0] == [2 ** 64 - 1])

    write(p, "d,麻雀,麻雀\n2024-01-01,1,2\n")
    try:
        csvparse.read_header(p)
        raise AssertionError("重复列名未被拒绝")
    except csvparse.CsvError:
        print("  ✓ 重复列名拒绝")


# ---------- 2. 发布、结构合并、类型提升 ----------

def test_publish_merge(tmp):
    cfg = make_cfg(tmp)
    f1 = os.path.join(tmp, "s1.csv")
    f2 = os.path.join(tmp, "s2.csv")
    f3 = os.path.join(tmp, "s3.csv")
    write(f1, "日期,麻雀,白鹭\n2023-05-01,12,3\n2023-05-02,9,\n")
    write(f2, "日期,麻雀,苍鹭\n2024-04-20,7,1\n")
    write(f3, "日期,白鹭,黑水鸡\n2023-05-01,约12,5\n")
    r1 = service.publish(cfg, "青湾", "s1.csv", f1, ["uint64", "uint64"])
    r2 = service.publish(cfg, "青湾", "s2.csv", f2, ["uint64", "uint64"])
    r3 = service.publish(cfg, "芦苇荡", "s3.csv", f3, ["text", "uint64"])
    check("新列加入", any(c["change"] == "added" and c["name"] == "苍鹭" for c in r2["changes"]))
    check("同名异型提升text", any(c["change"] == "promoted" and c["name"] == "白鹭" for c in r3["changes"]))
    check("每批独立Parquet", len({r["batch_id"] for r in (r1, r2, r3)}) == 3
          and len(os.listdir(cfg.parquet_dir)) == 3)

    snap_all = snap_of(cfg)
    snap_qw = snap_of(cfg, "青湾")
    types = {c["name"]: c["type"] for c in snap_all["columns"]}
    check("逻辑结构按列名合并", types == {"麻雀": "uint64", "白鹭": "text", "苍鹭": "uint64", "黑水鸡": "uint64"}, str(types))
    check("批次全部登记", len(snap_all["batches"]) == 3)
    check("按站点过滤", len(snap_qw["batches"]) == 2)

    schema, table = store.load_unified(cfg.parquet_dir, snap_all["columns"], snap_all["batches"])
    check("统一行数", table.num_rows == 4)
    check("统一列序", schema.names == ["obs_date", "麻雀", "白鹭", "苍鹭", "黑水鸡"])
    rows = table.to_pylist()
    check("缺列补NULL", rows[1]["苍鹭"] is None and rows[1]["白鹭"] is None)
    check("旧整数按十进制转文本", rows[0]["白鹭"] == "3" and isinstance(rows[0]["白鹭"], str))
    check("文本原样保留", rows[3]["白鹭"] == "约12")
    check("整数列保持uint64", rows[0]["麻雀"] == 12 and isinstance(rows[0]["麻雀"], int))

    # 提升后不得退回整数：再发一批把「白鹭」声明为 uint64
    f4 = os.path.join(tmp, "s4.csv")
    write(f4, "日期,白鹭\n2024-05-01,6\n")
    service.publish(cfg, "青湾", "s4.csv", f4, ["uint64"])
    snap2 = snap_of(cfg)
    types2 = {c["name"]: c["type"] for c in snap2["columns"]}
    check("提升后不退回整数", types2["白鹭"] == "text")
    _, t2 = store.load_unified(cfg.parquet_dir, snap2["columns"], snap2["batches"])
    check("新批整数也按文本读出", t2.to_pylist()[-1]["白鹭"] == "6")

    # 统一 CSV 导出
    csv_bytes = store.table_to_csv_bytes(t2).decode("utf-8")
    check("统一CSV含全部列", csv_bytes.splitlines()[0] == "obs_date,麻雀,白鹭,苍鹭,黑水鸡", csv_bytes.splitlines()[0])


# ---------- 3. 并发发布 ----------

def test_concurrent(tmp):
    cfg = make_cfg(tmp)
    errors = []

    def worker(i, decl):
        try:
            f = os.path.join(tmp, f"c{i}.csv")
            col = "共有种" if i < 2 else f"鸟种{i}"
            write(f, f"日期,{col}\n2024-06-0{i % 9 + 1},5\n")
            service.publish(cfg, f"站{i}", f"c{i}.csv", f, [decl])
        except Exception:
            errors.append(traceback.format_exc())

    # 线程 0/1 对同一列给出冲突类型(uint64/text)，其余线程各添新列
    decls = ["uint64", "text"] + ["uint64"] * 8
    threads = [threading.Thread(target=worker, args=(i, decls[i])) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check("并发无异常", not errors, "\n".join(errors))
    snap = snap_of(cfg)
    types = {c["name"]: c["type"] for c in snap["columns"]}
    check("并发批次全保留", len(snap["batches"]) == 10, str(len(snap["batches"])))
    check("并发新增列全保留", all(f"鸟种{i}" in types for i in range(2, 10)), str(types))
    check("并发下同列提升为text", types.get("共有种") == "text", str(types.get("共有种")))
    _, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
    check("并发统一行数", table.num_rows == 10)
    vals = sorted(r["共有种"] for r in table.to_pylist() if r["共有种"] is not None)
    check("冲突列两批数据均保留为文本", vals == ["5", "5"], str(vals))


# ---------- 4. 坏批不发布 ----------

def test_bad_batch(tmp):
    cfg = make_cfg(tmp)
    good = os.path.join(tmp, "g.csv")
    write(good, "日期,麻雀\n2024-01-01,3\n")
    service.publish(cfg, "站A", "g.csv", good, ["uint64"])
    before_files = set(os.listdir(cfg.parquet_dir))

    bad = os.path.join(tmp, "b.csv")
    write(bad, "日期,麻雀,白鹭\n2024-01-02,abc,1\n")
    try:
        service.publish(cfg, "站A", "b.csv", bad, ["uint64", "uint64"])
        raise AssertionError("坏整数应整批拒绝")
    except csvparse.CsvError:
        print("  ✓ 坏整数整批拒绝")
    check("坏批未登记", len(snap_of(cfg)["batches"]) == 1)
    check("坏批无残件", set(os.listdir(cfg.parquet_dir)) == before_files)

    # 写盘失败（模拟）=> 整批不发布
    orig = store.write_parquet_atomic
    def boom(_table, _path):
        raise OSError("disk full (simulated)")
    store.write_parquet_atomic = boom
    try:
        try:
            service.publish(cfg, "站A", "g2.csv", good, ["uint64"])
            raise AssertionError("写盘失败应抛错")
        except OSError:
            print("  ✓ 写盘失败整批不发布")
    finally:
        store.write_parquet_atomic = orig
    check("写盘失败未登记", len(snap_of(cfg)["batches"]) == 1)
    check("写盘失败无残件", set(os.listdir(cfg.parquet_dir)) == before_files)


# ---------- 5. 重启持久化 ----------

def test_restart(tmp):
    cfg = make_cfg(tmp)
    f = os.path.join(tmp, "r.csv")
    write(f, "日期,麻雀\n2024-01-01,3\n")
    service.publish(cfg, "站A", "r.csv", f, ["uint64"])
    cfg2 = Config(cfg.data_dir)          # 模拟重启：全新对象指向同一数据目录
    service.init_storage(cfg2)
    snap = snap_of(cfg2)
    check("重启后批次在", len(snap["batches"]) == 1)
    _, table = store.load_unified(cfg2.parquet_dir, snap["columns"], snap["batches"])
    check("重启后数据可读", table.num_rows == 1 and table.to_pylist()[0]["麻雀"] == 3)
    check("目录不引用残件", all(
        os.path.isfile(os.path.join(cfg2.parquet_dir, b["parquet_file"]))
        for b in snap["batches"]))


# ---------- 6. 撤回 / 固定快照 / 回收 ----------

def test_retract(tmp):
    cfg = make_cfg(tmp)
    a = publish_text(cfg, tmp, "a.csv", "日期,麻雀\n2024-01-01,3\n", ["uint64"])
    b = publish_text(cfg, tmp, "b.csv", "日期,麻雀\n2024-02-01,4\n", ["uint64"], station="站B")

    r = service.retract_batch(cfg, a["batch_id"][:8], "误交")
    check("按唯一前缀撤回", r["status"] == "retracted", str(r["status"]))
    row = batch_row(cfg, a["batch_id"])
    check("记录保留站点/文件名/行数", (row["station"], row["source_filename"], row["n_rows"])
          == ("站A", "a.csv", 1))
    check("记录撤回时间与原因", bool(row["retracted_at"]) and row["retract_note"] == "误交")

    snap = snap_of(cfg)
    check("新快照排除撤回批", [x["id"] for x in snap["batches"]] == [b["batch_id"]])
    _, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
    check("新取数只剩另一批", table.num_rows == 1 and table.to_pylist()[0]["麻雀"] == 4)

    # 无读者 => 撤回后文件被回收，记录不丢
    check("撤回后文件回收", not os.path.exists(os.path.join(cfg.parquet_dir, a["batch_id"] + ".parquet")))
    check("回收状态 done", batch_row(cfg, a["batch_id"])["reclaim_state"] == "done")
    check("其他批次文件完好", os.path.isfile(os.path.join(cfg.parquet_dir, b["batch_id"] + ".parquet")))
    check("全量记录仍含两批", len(catalog.all_batches(
        catalog.connect(cfg.db_path))) == 2)

    before = batch_row(cfg, a["batch_id"])
    r2 = service.retract_batch(cfg, a["batch_id"], "重复")
    after = batch_row(cfg, a["batch_id"])
    check("重复撤回复用原记录", r2["status"] == "already_retracted"
          and r2["batch"]["retracted_at"] == before["retracted_at"]
          and r2["batch"]["retract_note"] == "误交")
    check("重复撤回不改任何字段", before == after)

    # 类型提升不回退：撤回不会动逻辑结构
    publish_text(cfg, tmp, "c1.csv", "日期,白鹭\n2024-03-01,约5\n", ["text"])
    c2 = publish_text(cfg, tmp, "c2.csv", "日期,白鹭\n2024-03-02,6\n", ["uint64"])
    types = {x["name"]: x["type"] for x in snap_of(cfg)["columns"]}
    check("撤回前类型已提升", types["白鹭"] == "text")
    service.retract_batch(cfg, c2["batch_id"])
    types2 = {x["name"]: x["type"] for x in snap_of(cfg)["columns"]}
    check("撤回后逻辑结构不回退", types2 == types, str(types2))


def test_pinned_snapshot(tmp):
    cfg = make_cfg(tmp)
    a = publish_text(cfg, tmp, "a.csv", "日期,麻雀\n2024-01-01,3\n", ["uint64"])

    pin = retention.pin_snapshot(cfg)          # 固定快照，但尚未打开 Parquet
    r = service.retract_batch(cfg, a["batch_id"])
    check("撤回成功", r["status"] == "retracted")
    row = batch_row(cfg, a["batch_id"])
    path = os.path.join(cfg.parquet_dir, a["batch_id"] + ".parquet")
    check("有读者租约 => 待回收且文件仍在",
          row["reclaim_state"] == "pending" and os.path.isfile(path))
    check("撤回不改可见性以外的记录", row["retracted_at"] in (r["batch"]["retracted_at"],))

    schema, table = pin.load_unified()         # 撤回之后才打开 Parquet
    check("固定快照仍完整返回原结果",
          table.num_rows == 1 and table.to_pylist()[0]["麻雀"] == 3
          and schema.names == ["obs_date", "麻雀"])

    pin.release()
    check("读者释放后回收该批文件", not os.path.exists(path))
    check("释放后标记完成", batch_row(cfg, a["batch_id"])["reclaim_state"] == "done")
    check("新取数仍排除该批", snap_of(cfg)["batches"] == [])


def test_lease_expiry(tmp):
    """读者进程退出（不释放租约）=> 租约过期后回收，不永久占用。"""
    cfg = make_cfg(tmp)
    a = publish_text(cfg, tmp, "a.csv", "日期,麻雀\n2024-01-01,3\n", ["uint64"])
    path = os.path.join(cfg.parquet_dir, a["batch_id"] + ".parquet")
    ttl_before = retention.LEASE_TTL_SEC
    retention.LEASE_TTL_SEC = 1.0
    try:
        conn = catalog.connect(cfg.db_path)
        with catalog.write_txn(conn):
            conn.execute(
                "INSERT INTO leases(lease_id,batch_id,parquet_file,owner,created_at,expires_at)"
                " VALUES(?,?,?,?,?,?)",
                ("dead", a["batch_id"], a["batch_id"] + ".parquet", "dead-proc",
                 time.time(), time.time() + 1.0))
        conn.close()
        service.retract_batch(cfg, a["batch_id"])
        check("僵尸租约未过期时保持待回收",
              retention.reclaim(cfg, batch_id=a["batch_id"])[0]["state"] == "pending"
              and os.path.isfile(path))
        time.sleep(1.2)
        check("租约过期后回收",
              retention.reclaim(cfg, batch_id=a["batch_id"])[0]["state"] == "done"
              and not os.path.exists(path))
    finally:
        retention.LEASE_TTL_SEC = ttl_before


def test_crash_recovery(tmp):
    cfg = make_cfg(tmp)
    a = publish_text(cfg, tmp, "a.csv", "日期,麻雀\n2024-01-01,3\n", ["uint64"])
    b = publish_text(cfg, tmp, "b.csv", "日期,麻雀\n2024-02-01,4\n", ["uint64"])
    b_file = os.path.join(cfg.parquet_dir, b["batch_id"] + ".parquet")

    # 模拟「撤回已提交、删除前进程中断」
    conn = catalog.connect(cfg.db_path)
    with catalog.write_txn(conn):
        conn.execute("UPDATE batches SET retracted_at=?, reclaim_state='pending' WHERE id=?",
                     ("2024-01-01T00:00:00+00:00", a["batch_id"]))
    conn.close()
    cfg2 = Config(cfg.data_dir)
    service.init_storage(cfg2)                 # 重启
    row = batch_row(cfg2, a["batch_id"])
    check("重启接续回收", row["reclaim_state"] == "done" and row["retracted_at"])
    check("重启不会恢复可见性", [x["id"] for x in snap_of(cfg2)["batches"]] == [b["batch_id"]])
    check("重启不误删其他批次", os.path.isfile(b_file))

    # 模拟「文件已删、状态未落盘就中断」
    c = publish_text(cfg, tmp, "c.csv", "日期,麻雀\n2024-03-01,5\n", ["uint64"])
    c_file = os.path.join(cfg.parquet_dir, c["batch_id"] + ".parquet")
    os.unlink(c_file)
    conn = catalog.connect(cfg.db_path)
    with catalog.write_txn(conn):
        conn.execute("UPDATE batches SET retracted_at=?, reclaim_state='pending' WHERE id=?",
                     ("2024-01-02T00:00:00+00:00", c["batch_id"]))
    conn.close()
    service.init_storage(Config(cfg.data_dir))
    check("删除后中断：重启补记完成", batch_row(cfg, c["batch_id"])["reclaim_state"] == "done")
    check("补记不改撤回记录", batch_row(cfg, c["batch_id"])["retracted_at"]
          == "2024-01-02T00:00:00+00:00")


def test_reclaim_error_retry(tmp):
    cfg = make_cfg(tmp)
    a = publish_text(cfg, tmp, "a.csv", "日期,麻雀\n2024-01-01,3\n", ["uint64"])
    path = os.path.join(cfg.parquet_dir, a["batch_id"] + ".parquet")

    # 让删除失败一次（模拟权限 / IO 错误）；此时文件还在（未撤回前不回收）
    real_unlink = os.unlink

    def flaky(p):
        if p == path:
            raise PermissionError("simulated deny")
        return real_unlink(p)

    os.unlink = flaky
    try:
        r = service.retract_batch(cfg, a["batch_id"])
        res = retention.reclaim(cfg, batch_id=a["batch_id"])
    finally:
        os.unlink = real_unlink
    check("撤回提交", r["status"] == "retracted")
    check("删除失败显示 error", res[0]["state"] == "error", str(res))
    row = batch_row(cfg, a["batch_id"])
    check("错误信息入库", row["reclaim_state"] == "error" and "simulated" in row["reclaim_error"])
    check("失败时撤回记录不变", row["retracted_at"] is not None)
    check("失败时文件保留待重试", os.path.isfile(path))

    check("重试成功", retention.reclaim(cfg, batch_id=a["batch_id"], include_error=True)[0]["state"]
          == "done")
    check("重试后文件已删", not os.path.exists(path))
    check("重试后撤回记录仍在", batch_row(cfg, a["batch_id"])["retracted_at"] is not None)
    check("重试后状态 done", batch_row(cfg, a["batch_id"])["reclaim_state"] == "done")


def _slow_reader_proc(data_dir, batch_id, q):
    """独立进程：固定快照后睡到主进程放行，再打开 Parquet。"""
    cfg = Config(data_dir)
    pin = retention.pin_snapshot(cfg)
    q.put("pinned")
    q.get()
    _schema, table = pin.load_unified()
    q.put(("read", table.num_rows, table.to_pylist()[0]["麻雀"]))
    pin.release()
    q.put("released")


def test_multiprocess_slow_read(tmp):
    """多进程共用 DATA_DIR：慢取数不阻塞撤回 / 上传 / 取数，且完整返回原结果。"""
    cfg = make_cfg(tmp)
    big = publish_text(cfg, tmp, "big.csv",
                       "日期,麻雀\n" + "".join(f"2024-01-{i % 28 + 1:02d},{i}\n" for i in range(20000)),
                       ["uint64"], station="慢站")

    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    proc = ctx.Process(target=_slow_reader_proc, args=(cfg.data_dir, big["batch_id"], q))
    proc.start()
    try:
        check("慢读者已固定快照", q.get(timeout=60) == "pinned")
        # 慢读者此刻尚未打开 Parquet，且不持有任何库事务
        t0 = time.time()
        r = service.retract_batch(cfg, big["batch_id"], "并发撤回")
        t_retract = time.time() - t0
        check("撤回成功", r["status"] == "retracted")
        t0 = time.time()
        other = publish_text(cfg, tmp, "u.csv", "日期,麻雀\n2024-06-01,9\n", ["uint64"], station="新站")
        t_upload = time.time() - t0
        t0 = time.time()
        snap_of(cfg)
        t_snap = time.time() - t0
        check("慢取数期间撤回不阻塞", t_retract < 2.0, f"{t_retract:.2f}s")
        check("慢取数期间上传不阻塞", t_upload < 2.0, f"{t_upload:.2f}s")
        check("慢取数期间取数不阻塞", t_snap < 2.0, f"{t_snap:.2f}s")

        q.put("go")
        result = q.get(timeout=60)
        check("慢读者跨撤回完整返回原结果",
              result[0] == "read" and result[1] == 20000 and result[2] == 0, str(result))
        check("读者释放", q.get(timeout=60) == "released")
        proc.join(timeout=60)
        check("读者进程正常退出", proc.exitcode == 0)
    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=10)

    retention.reclaim(cfg)
    check("释放后回收该批", batch_row(cfg, big["batch_id"])["reclaim_state"] == "done")
    check("其他批次文件完好",
          os.path.isfile(os.path.join(cfg.parquet_dir, other["batch_id"] + ".parquet")))


# ---------- 7. Web 入口 ----------

def test_web(tmp):
    os.environ["DATA_DIR"] = os.path.join(tmp, "webdata")
    from app.web import create_app
    app = create_app()
    app.config["TESTING"] = True
    c = app.test_client()

    r = c.get("/")
    check("首页200", r.status_code == 200)

    data = {"station": "青湾",
            "file": (io.BytesIO("日期,麻雀,白鹭\n2023-05-01,12,3\n".encode("utf-8")), "a.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    check("上传后进入声明页", r.status_code == 200 and "逐列声明类型".encode() in r.data)
    m = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data)
    check("声明页含暂存令牌", m is not None)
    token = m.group(1).decode()

    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "a.csv",
                                 "type_0": "uint64", "type_1": "text"})
    check("发布后跳转预览", r.status_code == 302 and "/preview" in r.headers["Location"])

    r = c.get("/preview?station=青湾")
    check("预览含数据", r.status_code == 200 and "麻雀".encode() in r.data and b"12" in r.data)

    r = c.get("/download.csv?station=青湾")
    body = r.data.decode("utf-8")
    check("下载统一CSV", r.status_code == 200 and r.headers["Content-Type"].startswith("text/csv"))
    check("CSV含表头与数据", "obs_date" in body and "麻雀" in body and "12" in body)

    # 网页按批次 ID 撤回
    r = c.get("/batches")
    check("批次页200", r.status_code == 200 and "按批次 ID 撤回".encode() in r.data)
    batch_id = re.search(r'name="batch_id" value="([0-9a-f]{32})"', r.data.decode()).group(1)
    r = c.post("/retract", data={"batch_id": batch_id[:8], "note": "网页撤回"},
               follow_redirects=True)
    check("网页撤回成功提示", "已撤回".encode() in r.data)
    check("撤回记录显示", "网页撤回".encode() in r.data and "已撤回".encode() in r.data)
    check("待回收或已回收状态可见",
          "已回收".encode() in r.data or "待回收".encode() in r.data)
    r = c.get("/download.csv?station=青湾")
    check("撤回后下载排除该批", b"2023-05-01" not in r.data)
    r = c.get("/preview?station=青湾")
    check("撤回后预览排除该批", b"2023-05-01" not in r.data and "共 0 行".encode() in r.data)
    r = c.post("/retract", data={"batch_id": batch_id}, follow_redirects=True)
    check("网页重复撤回复用原记录", "此前已撤回".encode() in r.data)
    r = c.post("/retract", data={"batch_id": "ffffffff", "next": "/batches"},
               follow_redirects=True)
    check("网页撤回不存在批次报错", "找不到该批次".encode() in r.data)
    r = c.post("/reclaim", data={"batch_id": batch_id}, follow_redirects=True)
    check("网页回收无待回收提示", "回收完成".encode() in r.data
          or "没有待回收".encode() in r.data or "待回收".encode() in r.data)

    # 坏整数批次：网页流程下整批不发布
    data = {"station": "青湾",
            "file": (io.BytesIO("日期,麻雀\n2023-05-02,oops\n".encode("utf-8")), "bad.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    token = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data).group(1).decode()
    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "bad.csv",
                                 "type_0": "uint64"}, follow_redirects=True)
    check("坏批提示未发布", "批次未发布".encode() in r.data)
    check("坏批未入统一结构", b"oops" not in c.get("/download.csv").data)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        steps = [
            ("CSV 解析与校验", test_parser),
            ("发布 / 结构合并 / 类型提升", test_publish_merge),
            ("并发发布（新增列 + 同列类型冲突）", test_concurrent),
            ("坏整数 / 写盘失败整批不发布", test_bad_batch),
            ("重启后已发布数据可用", test_restart),
            ("按批次撤回 / 记录保留 / 排除取数", test_retract),
            ("固定快照跨撤回 / 读者释放后回收", test_pinned_snapshot),
            ("读者进程退出后租约过期回收", test_lease_expiry),
            ("删除失败可重试 / 中断后重启接续", test_crash_recovery),
            ("删除错误状态与重试", test_reclaim_error_retry),
            ("多进程共用 DATA_DIR：慢取数不阻塞", test_multiprocess_slow_read),
            ("Web 入口（Flask test client）", test_web),
        ]
        for i, (title, fn) in enumerate(steps, 1):
            print(f"[{i}/{len(steps)}] {title}")
            fn(os.path.join(tmp, f"case{i}"))
    print("\n全部自检通过 ✅")


if __name__ == "__main__":
    main()
