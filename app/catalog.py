"""目录库（SQLite, WAL 模式）：逻辑结构 + 批次清单 + 撤回/回收状态。

事务纪律：
- 写事务（write_txn）只在「Parquet 文件已完整落盘」之后短暂持有，
  用于合并逻辑结构 + 登记批次，解析/写文件期间绝不持有库写事务。
- 读事务（read_txn）给调用方一个一致快照：同一次取数看到同一状态的
  逻辑结构与文件清单。
- 取数固定快照时，在同一个 BEGIN IMMEDIATE 事务里读取清单并写入租约
  （见 retention.pin_snapshot）；慢取数本身不持有任何库事务。
"""
import sqlite3
from contextlib import contextmanager

COLUMN_TYPES = ("uint64", "text")

# 批次行完整字段（撤回后行仍保留站点 / 源文件名 / 行数 / 撤回记录）
BATCH_COLS = (
    "id", "station", "source_filename", "parquet_file", "n_rows", "created_at",
    "retracted_at", "retract_note", "reclaim_state", "reclaim_error", "reclaimed_at",
)

DDL = """
CREATE TABLE IF NOT EXISTS columns (
  id   INTEGER PRIMARY KEY AUTOINCREMENT,   -- 稳定列序：先出现的列排前
  name TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL CHECK (type IN ('uint64','text'))
);
CREATE TABLE IF NOT EXISTS batches (
  id              TEXT PRIMARY KEY,
  station         TEXT NOT NULL,
  source_filename TEXT NOT NULL,
  parquet_file    TEXT NOT NULL UNIQUE,
  n_rows          INTEGER NOT NULL,
  created_at      TEXT NOT NULL,
  retracted_at    TEXT,                     -- NULL=在用；非 NULL=已撤回（永不恢复可见）
  retract_note    TEXT,                     -- 首次撤回的备注（重复撤回不覆盖）
  reclaim_state   TEXT NOT NULL DEFAULT 'none'
                  CHECK (reclaim_state IN ('none','pending','done','error')),
  reclaim_error   TEXT,
  reclaimed_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_station ON batches(station);
CREATE TABLE IF NOT EXISTS leases (
  lease_id     TEXT NOT NULL,
  batch_id     TEXT NOT NULL,
  parquet_file TEXT NOT NULL,
  owner        TEXT NOT NULL,               -- host:pid:uuid，便于排查占用者
  created_at   REAL NOT NULL,
  expires_at   REAL NOT NULL,               -- 心跳续租；进程退出后自然过期
  PRIMARY KEY (lease_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_leases_batch ON leases(batch_id);
CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at);
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

# 旧库升级：只加列，不改既有数据
_BATCH_MIGRATIONS = (
    ("retracted_at", "TEXT"),
    ("retract_note", "TEXT"),
    ("reclaim_state", "TEXT NOT NULL DEFAULT 'none'"),
    ("reclaim_error", "TEXT"),
    ("reclaimed_at", "TEXT"),
)


def connect(db_path):
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _migrate(conn):
    """旧目录库补齐撤回/回收列与索引（幂等，多进程同时启动也安全）。"""
    have = {r["name"] for r in conn.execute("PRAGMA table_info(batches)")}
    for name, decl in _BATCH_MIGRATIONS:
        if name in have:
            continue
        try:
            conn.execute(f"ALTER TABLE batches ADD COLUMN {name} {decl}")
        except sqlite3.OperationalError as e:
            # 另一进程抢先加了同一列：视为已迁移
            if "duplicate column" not in str(e).lower():
                raise
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_batches_retracted"
                     " ON batches(retracted_at)")
    except sqlite3.OperationalError:
        pass  # 另一进程正在建同名索引


def init_db(db_path):
    conn = connect(db_path)
    try:
        conn.executescript(DDL)
        _migrate(conn)
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '0')")
    finally:
        conn.close()


@contextmanager
def write_txn(conn):
    """短写事务：BEGIN IMMEDIATE 串行化并发发布者。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def read_txn(conn):
    """一致读快照（WAL 下读写不互斥）。"""
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")


def merge_schema(conn, decls):
    """在同一写事务内合并逻辑结构。

    decls: [(name, 'uint64'|'text'), ...]
    规则：新列加入；同名同型不变；同名异型提升为 text（text 吸收，永不退回 uint64）。
    """
    changes = []
    for name, new_type in decls:
        if new_type not in COLUMN_TYPES:
            raise ValueError(f"非法列类型: {new_type!r}")
        row = conn.execute("SELECT type FROM columns WHERE name = ?", (name,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO columns(name, type) VALUES (?, ?)", (name, new_type))
            changes.append({"change": "added", "name": name, "type": new_type})
        elif row["type"] == new_type:
            continue
        elif row["type"] == "text":
            continue  # 已提升为 text，不退回整数
        else:
            conn.execute("UPDATE columns SET type = 'text' WHERE name = ? AND type = 'uint64'", (name,))
            changes.append({"change": "promoted", "name": name, "type": "text"})
    if changes:
        conn.execute(
            "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
            "WHERE key = 'schema_version'"
        )
    return changes


def insert_batch(conn, batch):
    conn.execute(
        "INSERT INTO batches(id, station, source_filename, parquet_file, n_rows, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (batch["id"], batch["station"], batch["source_filename"],
         batch["parquet_file"], batch["n_rows"], batch["created_at"]),
    )


_LIVE = "retracted_at IS NULL"


def live_rows(conn, station=None):
    """未撤回批次（含被撤回批次保留给撤回记录，不参与取数）。"""
    q = f"SELECT {', '.join(BATCH_COLS)} FROM batches"
    if station:
        return conn.execute(
            q + f" WHERE {_LIVE} AND station = ? ORDER BY rowid", (station,)).fetchall()
    return conn.execute(q + f" WHERE {_LIVE} ORDER BY rowid").fetchall()


def snapshot(conn, station=None):
    """一个读事务内同时取逻辑结构与在用批次清单（同一状态）。

    仅用于浏览 / 统计；撤回是已提交的独立事实，浏览即可见。
    """
    with read_txn(conn):
        cols = [dict(r) for r in conn.execute("SELECT name, type FROM columns ORDER BY id")]
        rows = live_rows(conn, station)
        ver = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    return {
        "schema_version": int(ver["value"]) if ver else 0,
        "columns": cols,
        "batches": [dict(r) for r in rows],
    }


def all_batches(conn, include_retracted=True):
    q = f"SELECT {', '.join(BATCH_COLS)} FROM batches"
    if not include_retracted:
        q += f" WHERE {_LIVE}"
    return [dict(r) for r in conn.execute(q + " ORDER BY rowid")]
