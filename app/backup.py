"""一致备份。

用 SQLite 在线备份接口（Connection.backup）在写锁保护下生成一致快照，
并在 meta.db 中记录快照对应的最后事务序号与链头校验值——
恢复时以该序号为基线，只重放其后的日志。
"""
from pathlib import Path

from . import store
from .core import GENESIS, connect, verify_chain


class BackupError(Exception):
    pass


def create_backup(paths, note: str = "") -> int:
    # 调用方需持有写锁，保证快照内容与记录的 last_seq 严格一致
    live = connect(paths.live)
    try:
        row = live.execute(
            "SELECT seq, checksum FROM op_log ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        last_seq = row["seq"] if row else 0
        head = row["checksum"] if row else GENESIS
    finally:
        live.close()

    backup_id = store.create_backup_record(paths.meta, last_seq, head, note)
    fpath = paths.backups / f"backup_{backup_id:04d}_seq{last_seq}.db"
    try:
        src = connect(paths.live)
        dst = connect(fpath)
        try:
            src.backup(dst)  # 在线一致备份，得到的是备份时刻的快照
        finally:
            dst.close()
            src.close()
        # 一致性校验：快照自身日志链完整，且最大序号与记录一致
        snap = connect(fpath)
        try:
            v = verify_chain(snap)
            if not v["ok"]:
                raise BackupError(f"备份一致性校验失败：{v['detail']}")
            if v["last_seq"] != last_seq:
                raise BackupError(
                    f"备份一致性校验失败：快照序号 #{v['last_seq']} 与记录 #{last_seq} 不符")
        finally:
            snap.close()
    except Exception:
        store.delete_backup_record(paths.meta, backup_id)
        Path(fpath).unlink(missing_ok=True)
        raise
    store.set_backup_file(paths.meta, backup_id, str(fpath))
    return backup_id
