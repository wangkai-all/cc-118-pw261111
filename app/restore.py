"""时点恢复（PITR）。

流程：
1. start_restore：以备份快照为基线复制出一个**独立的暂存库**，
   从当前库的日志中逐笔重放 (backup.last_seq, target_seq] 区间的事务，
   每条都校验序号连续性和链式校验值。断号、损坏、目标超范围都会停止并指出缺口。
   整个过程不碰当前正在使用的库。
2. compare_with_live：把暂存库与当前台账逐项对照，供页面核对。
3. confirm_restore：核对无误后由页面确认，用暂存库整体替换当前库；
   discard_restore：放弃并清理暂存库。

注意：基线是备份时刻生成的快照文件，不是复制当前库文件充当历史时点。
"""
import json
import os
import shutil
from pathlib import Path

from . import store
from .core import (checksum_of, connect, list_inventory, max_seq, now_iso,
                   replay_one, verify_chain)


class RestoreError(Exception):
    pass


def start_restore(paths, backup_id: int, target_seq: int) -> int:
    target_seq = int(target_seq)
    backup = store.get_backup(paths.meta, backup_id)
    if not backup:
        raise RestoreError("备份不存在")
    if not os.path.exists(backup["file"]):
        raise RestoreError("备份文件不存在，可能已被清理")

    live = connect(paths.live)
    try:
        live_max = max_seq(live)
        if target_seq < backup["last_seq"]:
            raise RestoreError(
                f"目标序号 #{target_seq} 早于备份点 #{backup['last_seq']}，"
                "该备份覆盖不到，请换更早的备份或更晚的目标")
        if target_seq > live_max:
            raise RestoreError(
                f"目标序号 #{target_seq} 超出可恢复范围：当前日志最大序号为 #{live_max}")
        # 血缘锚点：备份点那一笔在当前日志中必须一致，否则说明台账已切换到别的历史线
        if backup["last_seq"] > 0:
            anchor = live.execute(
                "SELECT checksum FROM op_log WHERE seq=?", (backup["last_seq"],)
            ).fetchone()
            if anchor is None:
                raise RestoreError(
                    f"当前日志缺少备份锚点序号 #{backup['last_seq']}，"
                    "无法确认与备份同源，停止恢复")
            if anchor["checksum"] != backup["head_checksum"]:
                raise RestoreError(
                    "备份锚点校验值与当前日志不一致：台账已切换到其他历史线，"
                    "不能基于该备份继续恢复")
        rows = live.execute(
            "SELECT * FROM op_log WHERE seq>? AND seq<=? ORDER BY seq",
            (backup["last_seq"], target_seq),
        ).fetchall()
    finally:
        live.close()

    # 同一时刻只允许一个进行中的恢复，旧的暂存任务作废
    active = store.find_active_restore(paths.meta)
    if active:
        store.update_restore(paths.meta, active["id"],
                             status="discarded", error="被新的恢复任务取代")
        if active["staging_file"]:
            Path(active["staging_file"]).unlink(missing_ok=True)

    rid = store.create_restore_record(paths.meta, backup_id,
                                      backup["last_seq"], target_seq)
    staging_path = paths.staging / f"restore_{rid:04d}_to_seq{target_seq}.db"
    try:
        # 基线 = 备份时刻的快照，而不是当前库文件
        shutil.copyfile(backup["file"], staging_path)
        st = connect(staging_path)
        try:
            v = verify_chain(st, upto_seq=backup["last_seq"])
            if not v["ok"]:
                raise RestoreError(f"备份快照自身校验失败：{v['detail']}")
            if v["last_seq"] != backup["last_seq"]:
                raise RestoreError(
                    f"备份快照序号 #{v['last_seq']} 与备份记录 #{backup['last_seq']} 不符")
            prev = v["head"]
            expect = backup["last_seq"] + 1
            for row in rows:
                if row["seq"] != expect:
                    raise RestoreError(
                        f"日志断号：缺少序号 #{expect}（实际读到 #{row['seq']}），"
                        "恢复在缺口前停止")
                try:
                    payload = json.loads(row["payload"])
                except ValueError:
                    raise RestoreError(
                        f"日志损坏：序号 #{row['seq']} 的输入内容无法解析，恢复停止")
                if (row["prev_checksum"] != prev
                        or checksum_of(prev, row["seq"], row["op_type"], payload) != row["checksum"]):
                    raise RestoreError(
                        f"日志损坏：序号 #{row['seq']} 的校验值与内容不匹配，恢复停止")
                # 每条日志在暂存库中独立成一个事务：调拨两边同时生效，
                # 且 replay_one 会拒绝重复应用同一序号
                replay_one(st, row)
                prev = row["checksum"]
                expect += 1
            if expect - 1 != target_seq:
                raise RestoreError(
                    f"日志断号：序号 #{expect} 起缺失，无法推进到目标 #{target_seq}")
        finally:
            st.close()
    except Exception as e:
        staging_path.unlink(missing_ok=True)
        store.update_restore(paths.meta, rid, status="failed", error=str(e))
        raise

    store.update_restore(paths.meta, rid, status="staged",
                         staging_file=str(staging_path))
    return rid


def compare_with_live(paths, restore_id: int) -> dict:
    """暂存库与当前台账逐项对照，并列出切换后会被移除的线上事务。"""
    rec = store.get_restore(paths.meta, restore_id)
    if not rec:
        raise RestoreError("恢复记录不存在")
    if not rec["staging_file"] or not os.path.exists(rec["staging_file"]):
        raise RestoreError("暂存库文件不存在")
    st = connect(rec["staging_file"])
    live = connect(paths.live)
    try:
        s_inv = {(r["warehouse"], r["sku"]): r["qty"] for r in list_inventory(st)}
        l_inv = {(r["warehouse"], r["sku"]): r["qty"] for r in list_inventory(live)}
        rows = []
        for key in sorted(set(s_inv) | set(l_inv)):
            restored = s_inv.get(key, 0)
            live_qty = l_inv.get(key, 0)
            rows.append({
                "warehouse": key[0], "sku": key[1],
                "live_qty": live_qty, "restored_qty": restored,
                "diff": restored - live_qty,
            })
        discarded = [dict(r) for r in live.execute(
            "SELECT * FROM op_log WHERE seq>? ORDER BY seq", (rec["target_seq"],))]
        return {
            "record": rec,
            "rows": rows,
            "discarded": discarded,
            "replayed": rec["target_seq"] - rec["backup_last_seq"],
        }
    finally:
        st.close()
        live.close()


def confirm_restore(paths, restore_id: int) -> None:
    """核对完成后确认切换：用暂存库整体替换当前库。失败不动当前库。"""
    rec = store.get_restore(paths.meta, restore_id)
    if not rec or rec["status"] != "staged":
        raise RestoreError("该恢复任务不在待确认状态")
    staging_file = rec["staging_file"]
    if not staging_file or not os.path.exists(staging_file):
        store.update_restore(paths.meta, restore_id,
                             status="failed", error="暂存库文件丢失")
        raise RestoreError("暂存库文件丢失，无法切换")
    st = connect(staging_file)
    try:
        v = verify_chain(st)
        if not v["ok"]:
            store.update_restore(paths.meta, restore_id, status="failed",
                                 error=f"切换前校验失败：{v['detail']}")
            raise RestoreError(f"暂存库校验失败：{v['detail']}，已取消切换，当前台账未受影响")
        dst = connect(paths.live)
        try:
            st.backup(dst)  # 用恢复结果整体替换当前库
        finally:
            dst.close()
    finally:
        st.close()
    store.update_restore(paths.meta, restore_id,
                         status="confirmed", confirmed_at=now_iso())


def discard_restore(paths, restore_id: int) -> None:
    rec = store.get_restore(paths.meta, restore_id)
    if not rec or rec["status"] not in ("staged", "running"):
        raise RestoreError("该恢复任务不能放弃（当前状态不允许）")
    store.update_restore(paths.meta, restore_id, status="discarded")
    if rec["staging_file"]:
        Path(rec["staging_file"]).unlink(missing_ok=True)


def recover_interrupted(paths) -> None:
    """程序重启时清理：中断的恢复标记失败，暂存文件丢失的待确认任务标记失败。"""
    for rec in store.list_restores(paths.meta):
        if rec["status"] == "running":
            store.update_restore(paths.meta, rec["id"],
                                 status="failed", error="程序重启，恢复流程中断")
        elif rec["status"] == "staged" and not os.path.exists(rec["staging_file"] or ""):
            store.update_restore(paths.meta, rec["id"],
                                 status="failed", error="暂存库文件丢失")
