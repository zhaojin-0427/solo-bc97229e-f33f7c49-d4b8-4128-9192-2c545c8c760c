"""批次撤回、固定快照租约与文件回收（多进程共用 DATA_DIR）。

不变量：
1. 撤回是目录库里一次不可逆的短事务事实（batches.retracted_at 非 NULL）。
   撤回提交后，任何新的取数快照都不再包含该批；撤回记录（站点 / 文件名 /
   行数 / 时间 / 备注）永远保留。
2. 取数快照固定 = 一个短写事务里读出「逻辑结构 + 在用批次清单」并登记租约，
   提交后立刻释放库锁；随后读取 Parquet 的慢过程不持有任何库事务。
   因此慢取数不会阻塞撤回、其他上传或取数。
3. 租约由读者进程心跳续期；读者释放即删除租约，进程退出则租约自然过期，
   不会永久占用文件。
4. 回收 = 已撤回批次在「无有效租约」时删除其 Parquet：
      pending(待回收) --无读者--> done(已回收)
                            \--> error(删除错误，可重试)
   永远先删文件、后写 done；任一步骤崩溃后，重启看到 pending 且文件缺失
   就补记 done。回收只改 reclaim_* 列，绝不触碰 retracted_at，绝不恢复可见性。
5. 只删除该批自己登记的 Parquet 文件名（必须是 parquet_dir 下的直接子文件名），
   且只对已撤回批次操作，不会误删其他批次。
"""
import datetime
import os
import re
import socket
import sqlite3
import threading
import time
import uuid
import weakref

from . import catalog, store


def _env_number(name, default):
    """空值 / 非法值回落到默认，避免容器里空环境变量导致启动失败。"""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        v = float(raw)
    except ValueError:
        return default
    return v if v > 0 else default


LEASE_TTL_SEC = _env_number("LEASE_TTL_SEC", 30.0)
RECLAIM_INTERVAL_SEC = _env_number("RECLAIM_INTERVAL_SEC", 2.0)

# 进程标识（写进租约便于排查谁在占用；多进程 / 多主机都只是不透明字符串）
OWNER = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"

_PREFIX_RE = re.compile(r"[0-9a-fA-F]{4,32}")


def _now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


# ==========================================================================
# 固定快照 + 租约
# ==========================================================================

def _lease_keeper(ref, db_path, lease_id, ttl):
    """心跳线程：续租，直到读者释放（stop 置位）或读者对象被回收。

    只持有对 PinnedSnapshot 的弱引用：若调用方忘了 release() 就把对象丢掉，
    线程会自行退出并删除租约，不会永久占用文件。
    """
    conn = None
    try:
        conn = catalog.connect(db_path)
        while True:
            obj = ref()
            if obj is None:
                break               # 读者对象已被回收：视为释放
            stop = obj._stop
            if stop.wait(max(1.0, ttl / 3.0)):
                break               # 显式释放
            try:
                with catalog.write_txn(conn):
                    conn.execute(
                        "UPDATE leases SET expires_at = ? WHERE lease_id = ?",
                        (time.time() + ttl, lease_id),
                    )
            except sqlite3.Error:
                pass                # 下次再续；若一直失败，租约到期后自动失效
    finally:
        try:
            if conn is not None:
                with catalog.write_txn(conn):
                    conn.execute("DELETE FROM leases WHERE lease_id = ?", (lease_id,))
        except sqlite3.Error:
            pass                    # 残留租约到期后失效，不会永久占用
        finally:
            if conn is not None:
                conn.close()


class PinnedSnapshot:
    """一次已固定的取数快照：结构/清单已在短事务中固定并登记租约。

    在 release() 之前（以及租约心跳有效期间）对应 Parquet 文件一定存在，
    即使批次随后被撤回；因此拿快照后再打开 Parquet 仍能完整返回原结果。
    """

    def __init__(self, cfg, lease_id, columns, batches, schema_version, ttl=None):
        self.cfg = cfg
        self.lease_id = lease_id
        self.columns = columns
        self.batches = batches
        self.schema_version = schema_version
        self.ttl = LEASE_TTL_SEC if ttl is None else ttl
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._released = False
        self._thread = None

    def _start(self):
        self._thread = threading.Thread(
            target=_lease_keeper,
            args=(weakref.ref(self), self.cfg.db_path, self.lease_id, self.ttl),
            daemon=True, name=f"lease-{self.lease_id[:8]}")
        self._thread.start()

    def load_unified(self):
        """按固定快照读取统一结构表（缺列补 NULL、类型投影）。"""
        return store.load_unified(self.cfg.parquet_dir, self.columns, self.batches)

    def release(self):
        """读者结束：停心跳、删租约，并顺带回收已撤回批次。"""
        with self._lock:
            if self._released:
                return
            self._released = True
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.ttl / 3.0) + 1.0)
        # 释放后立即尝试回收（其他读者仍持租约的批次会保持 pending）
        try:
            reclaim(self.cfg)
        except Exception:
            pass
        wake_reclaimer(self.cfg)

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        self.release()
        return False


def pin_snapshot(cfg, station=None):
    """固定「逻辑结构 + 在用批次清单」并登记租约。不打开任何 Parquet 文件。

    与撤回在同一个 SQLite 写锁上串行化：本事务提交前撤回无法提交、
    本事务提交后撤回才能看到租约。
    """
    lease_id = uuid.uuid4().hex
    ttl = LEASE_TTL_SEC
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            columns = [dict(r) for r in conn.execute(
                "SELECT name, type FROM columns ORDER BY id")]
            batches = [dict(r) for r in catalog.live_rows(conn, station)]
            ver = conn.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
            now = time.time()
            for b in batches:
                conn.execute(
                    "INSERT INTO leases(lease_id, batch_id, parquet_file, owner,"
                    " created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (lease_id, b["id"], b["parquet_file"], OWNER,
                     now, now + ttl))
    finally:
        conn.close()
    snap = PinnedSnapshot(cfg, lease_id, columns, batches,
                          int(ver["value"]) if ver else 0, ttl=ttl)
    snap._start()
    return snap


# ==========================================================================
# 撤回
# ==========================================================================

def resolve_batch(conn, ref):
    """按完整批次 ID 或唯一前缀定位批次。返回 (row, 错误码)。"""
    ref = (ref or "").strip()
    if not ref:
        return None, "empty"
    row = conn.execute("SELECT * FROM batches WHERE id = ?", (ref,)).fetchone()
    if row is not None:
        return row, None
    if not _PREFIX_RE.fullmatch(ref):
        return None, "not_found"
    ref = ref.lower()
    rows = conn.execute(
        "SELECT * FROM batches WHERE id LIKE ?", (ref + "%",)).fetchall()
    if len(rows) == 1:
        return rows[0], None
    if len(rows) > 1:
        return None, "ambiguous"
    return None, "not_found"


def retract(cfg, batch_ref, note=""):
    """撤回一个批次（不可逆）。重复撤回复用原撤回记录，不覆盖时间 / 备注。"""
    note = (note or "").strip()[:500]
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            row, err = resolve_batch(conn, batch_ref)
            if row is None:
                return {"status": err, "batch_id": (batch_ref or "").strip()}
            if row["retracted_at"] is not None:
                original = dict(conn.execute(
                    "SELECT * FROM batches WHERE id = ?",
                    (row["id"],)).fetchone())
                # 原记录原样返回：撤回时间、备注、回收进度都不改写
                return {"status": "already_retracted", "batch": original}
            conn.execute(
                "UPDATE batches SET retracted_at = ?, retract_note = ?,"
                " reclaim_state = 'pending', reclaim_error = NULL,"
                " reclaimed_at = NULL WHERE id = ? AND retracted_at IS NULL",
                (_now_iso(), note or None, row["id"]))
            updated = dict(conn.execute(
                "SELECT * FROM batches WHERE id = ?", (row["id"],)).fetchone())
    finally:
        conn.close()
    return {"status": "retracted", "batch": updated}


# ==========================================================================
# 回收（删除文件）
# ==========================================================================

def _safe_parquet_path(cfg, filename):
    """只接受 parquet_dir 下的直接子文件名，杜绝路径穿越 / 误删他批。"""
    name = filename or ""
    if not name or name.startswith(".") or os.path.basename(name) != name:
        raise ValueError(f"非法 Parquet 文件名: {filename!r}")
    return os.path.join(cfg.parquet_dir, name)


def _update_state(cfg, batch_id, state, error=None):
    """短事务更新回收状态；永远要求该批已撤回，绝不修改 retracted_at。"""
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            if state == "done":
                conn.execute(
                    "UPDATE batches SET reclaim_state = 'done',"
                    " reclaim_error = NULL, reclaimed_at = ?"
                    " WHERE id = ? AND retracted_at IS NOT NULL"
                    " AND reclaim_state != 'done'",
                    (_now_iso(), batch_id))
            elif state == "pending":
                conn.execute(
                    "UPDATE batches SET reclaim_state = 'pending',"
                    " reclaim_error = NULL"
                    " WHERE id = ? AND retracted_at IS NOT NULL"
                    " AND reclaim_state != 'done'",
                    (batch_id,))
            else:  # error
                conn.execute(
                    "UPDATE batches SET reclaim_state = 'error',"
                    " reclaim_error = ?"
                    " WHERE id = ? AND retracted_at IS NOT NULL"
                    " AND reclaim_state != 'done'",
                    ((error or "")[:500], batch_id))
    finally:
        conn.close()


def _active_leases(conn, batch_id, now):
    return conn.execute(
        "SELECT COUNT(*) FROM leases WHERE batch_id = ? AND expires_at > ?",
        (batch_id, now)).fetchone()[0]


def _sweep_expired_leases(cfg, now=None):
    now = time.time() if now is None else now
    conn = catalog.connect(cfg.db_path)
    try:
        with catalog.write_txn(conn):
            conn.execute("DELETE FROM leases WHERE expires_at <= ?", (now,))
    except sqlite3.Error:
        pass
    finally:
        conn.close()


def _targets(cfg, batch_id=None, include_error=False):
    conn = catalog.connect(cfg.db_path)
    try:
        states = ["pending"] + (["error"] if include_error else [])
        sql = ("SELECT id, parquet_file, reclaim_state, retracted_at"
               " FROM batches WHERE retracted_at IS NOT NULL"
               " AND reclaim_state IN (" + ",".join("?" * len(states)) + ")")
        args = list(states)
        if batch_id is not None:
            sql += " AND id = ?"
            args.append(batch_id)
        return [dict(r) for r in conn.execute(sql, args)]
    finally:
        conn.close()


def _reclaim_one(cfg, row, now):
    batch_id = row["id"]
    if row["retracted_at"] is None:  # 防御：未撤回的批次永不参与回收
        return {"id": batch_id, "state": "not_retracted"}

    conn = catalog.connect(cfg.db_path)
    try:
        active = _active_leases(conn, batch_id, now)
    finally:
        conn.close()
    if active:
        if row["reclaim_state"] != "pending":
            _update_state(cfg, batch_id, "pending")
        return {"id": batch_id, "state": "pending", "active_leases": active}

    try:
        path = _safe_parquet_path(cfg, row["parquet_file"])
    except ValueError as e:
        _update_state(cfg, batch_id, "error", str(e))
        return {"id": batch_id, "state": "error", "error": str(e)}

    try:
        os.unlink(path)
    except FileNotFoundError:
        pass  # 上次崩溃已删掉文件但没来得及记 done：视为已完成
    except OSError as e:
        # 若删除失败期间文件已被别人删掉，仍按完成处理
        if not os.path.exists(path):
            pass
        else:
            msg = f"删除 {row['parquet_file']} 失败：{e}"
            _update_state(cfg, batch_id, "error", msg)
            return {"id": batch_id, "state": "error", "error": msg}

    _update_state(cfg, batch_id, "done")
    return {"id": batch_id, "state": "done"}

def reclaim(cfg, batch_id=None, include_error=False):
    """回收已撤回且无有效租约的批次文件；可指定单个批次（网页重试用）。"""
    now = time.time()
    try:
        _sweep_expired_leases(cfg, now)
    except Exception:
        pass
    if batch_id is not None:
        conn = catalog.connect(cfg.db_path)
        try:
            row = conn.execute(
                "SELECT id, parquet_file, reclaim_state, retracted_at FROM batches"
                " WHERE id = ?", (batch_id,)).fetchone()
        finally:
            conn.close()
        if row is None:
            return [{"id": batch_id, "state": "not_found"}]
        rows = [dict(row)]
    else:
        rows = _targets(cfg, batch_id=None, include_error=include_error)
    results = []
    for row in rows:
        if row.get("retracted_at") is None:
            results.append({"id": row["id"], "state": "not_retracted"})
            continue
        results.append(_reclaim_one(cfg, row, now))
    return results


# ==========================================================================
# 启动接续 + 后台回收线程
# ==========================================================================

def recover(cfg):
    """进程启动：清过期租约，并接续所有未完成的回收（含上次删除失败）。"""
    try:
        _sweep_expired_leases(cfg)
    except Exception:
        pass
    return reclaim(cfg, include_error=True)


_RUNNERS = {}
_RUNNERS_LOCK = threading.Lock()


def _reclaimer_loop(cfg, stop, wake, interval):
    while not stop.is_set():
        try:
            reclaim(cfg)
        except Exception:
            pass
        wake.wait(interval)
        wake.clear()


def start_reclaimer(cfg, interval=None):
    """每个 DATA_DIR 只启动一个守护线程；多进程下各进程都可运行（操作幂等）。"""
    interval = RECLAIM_INTERVAL_SEC if interval is None else interval
    key = cfg.data_dir
    with _RUNNERS_LOCK:
        ent = _RUNNERS.get(key)
        if ent is not None and ent[0].is_alive():
            return ent[0]
        stop = threading.Event()
        wake = threading.Event()
        th = threading.Thread(
            target=_reclaimer_loop, args=(cfg, stop, wake, interval),
            daemon=True, name=f"reclaim-{os.path.basename(key) or 'data'}")
        _RUNNERS[key] = (th, stop, wake)
        th.start()
        return th

def stop_reclaimer(cfg):
    with _RUNNERS_LOCK:
        ent = _RUNNERS.pop(cfg.data_dir, None)
    if ent is not None:
        ent[1].set()
        ent[2].set()


def wake_reclaimer(cfg):
    """唤醒后台回收线程（若已启动）。"""
    ent = _RUNNERS.get(cfg.data_dir)
    if ent is not None:
        ent[2].set()
