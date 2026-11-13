"""meta.db：备份记录与恢复记录。

独立于业务库存放，恢复切换不会冲掉这些记录，程序重启后仍可查询。
"""
from .core import connect, now_iso

META_SCHEMA = """
CREATE TABLE IF NOT EXISTS backups (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at    TEXT NOT NULL,
  last_seq      INTEGER NOT NULL,   -- 备份对应的最后事务序号（明确关联）
  head_checksum TEXT NOT NULL,      -- 该序号的日志链头部校验值
  file          TEXT NOT NULL DEFAULT '',
  note          TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS restores (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  backup_id       INTEGER NOT NULL,
  backup_last_seq INTEGER NOT NULL,
  target_seq      INTEGER NOT NULL,  -- 恢复目标序号
  status          TEXT NOT NULL,     -- running/staged/confirmed/discarded/failed
  error           TEXT NOT NULL DEFAULT '',
  staging_file    TEXT NOT NULL DEFAULT '',
  created_at      TEXT NOT NULL,
  confirmed_at    TEXT
);
"""

RESTORE_STATUS_LABELS = {
    "running": "进行中",
    "staged": "待确认",
    "confirmed": "已切换",
    "discarded": "已放弃",
    "failed": "失败",
}


def init_meta_db(path) -> None:
    conn = connect(path)
    try:
        conn.executescript(META_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def _rows(conn, sql, args=()):
    return [dict(r) for r in conn.execute(sql, args)]


# ---------------------------------------------------------------- 备份记录

def create_backup_record(meta_path, last_seq, head_checksum, note) -> int:
    conn = connect(meta_path)
    try:
        cur = conn.execute(
            "INSERT INTO backups(created_at, last_seq, head_checksum, note) VALUES (?,?,?,?)",
            (now_iso(), last_seq, head_checksum, note),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def set_backup_file(meta_path, backup_id, file) -> None:
    conn = connect(meta_path)
    try:
        conn.execute("UPDATE backups SET file=? WHERE id=?", (str(file), backup_id))
        conn.commit()
    finally:
        conn.close()


def delete_backup_record(meta_path, backup_id) -> None:
    conn = connect(meta_path)
    try:
        conn.execute("DELETE FROM backups WHERE id=?", (backup_id,))
        conn.commit()
    finally:
        conn.close()


def get_backup(meta_path, backup_id):
    conn = connect(meta_path)
    try:
        rows = _rows(conn, "SELECT * FROM backups WHERE id=?", (backup_id,))
        return rows[0] if rows else None
    finally:
        conn.close()


def list_backups(meta_path):
    conn = connect(meta_path)
    try:
        return _rows(conn, "SELECT * FROM backups ORDER BY id DESC")
    finally:
        conn.close()


# ---------------------------------------------------------------- 恢复记录

def create_restore_record(meta_path, backup_id, backup_last_seq, target_seq) -> int:
    conn = connect(meta_path)
    try:
        cur = conn.execute(
            "INSERT INTO restores(backup_id, backup_last_seq, target_seq, status, created_at)"
            " VALUES (?,?,?,?,?)",
            (backup_id, backup_last_seq, target_seq, "running", now_iso()),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_restore(meta_path, restore_id, **fields) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    conn = connect(meta_path)
    try:
        conn.execute(f"UPDATE restores SET {cols} WHERE id=?",
                     (*fields.values(), restore_id))
        conn.commit()
    finally:
        conn.close()


def get_restore(meta_path, restore_id):
    conn = connect(meta_path)
    try:
        rows = _rows(conn, "SELECT * FROM restores WHERE id=?", (restore_id,))
        return rows[0] if rows else None
    finally:
        conn.close()


def list_restores(meta_path):
    conn = connect(meta_path)
    try:
        return _rows(conn, "SELECT * FROM restores ORDER BY id DESC")
    finally:
        conn.close()


def find_active_restore(meta_path):
    """进行中的恢复（running 或待确认 staged），同一时刻至多一个。"""
    conn = connect(meta_path)
    try:
        rows = _rows(
            conn,
            "SELECT * FROM restores WHERE status IN ('running','staged')"
            " ORDER BY id DESC LIMIT 1",
        )
        return rows[0] if rows else None
    finally:
        conn.close()
