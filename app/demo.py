"""演示辅助：种子数据、连续操作、故障注入、一键重置。

故障注入直接篡改当前库的 op_log（删行制造断号、改 payload 制造校验不符），
用来演示恢复流程如何发现缺口并停止。
"""
import json
import random
import shutil
from pathlib import Path

from . import store
from .core import (BizError, OP_INBOUND, OP_OUTBOUND, OP_TRANSFER, canonical,
                   connect, execute_op, init_live_db, max_seq)

WAREHOUSES = ["主仓", "东区仓", "西区仓"]
SKUS = ["键盘", "鼠标", "显示器", "数据线"]

SEED_OPS = [
    (OP_INBOUND, {"warehouse": "主仓", "sku": "键盘", "qty": 100}),
    (OP_INBOUND, {"warehouse": "主仓", "sku": "鼠标", "qty": 150}),
    (OP_INBOUND, {"warehouse": "主仓", "sku": "显示器", "qty": 80}),
    (OP_INBOUND, {"warehouse": "东区仓", "sku": "键盘", "qty": 40}),
    (OP_INBOUND, {"warehouse": "西区仓", "sku": "鼠标", "qty": 60}),
    (OP_TRANSFER, {"from_warehouse": "主仓", "to_warehouse": "东区仓", "sku": "键盘", "qty": 20}),
    (OP_OUTBOUND, {"warehouse": "主仓", "sku": "鼠标", "qty": 10}),
]


def seed_if_empty(paths) -> bool:
    conn = connect(paths.live)
    try:
        if max_seq(conn) > 0:
            return False
        for op_type, payload in SEED_OPS:
            execute_op(conn, op_type, payload)
        return True
    finally:
        conn.close()


def continuous_ops(paths, count: int = 10) -> list:
    """连续产生一批合法业务事务（每笔都是独立事务、独立序号）。"""
    executed = []
    for _ in range(count):
        conn = connect(paths.live)
        try:
            inv = {(r["warehouse"], r["sku"]): r["qty"]
                   for r in conn.execute("SELECT warehouse, sku, qty FROM inventory")}
            candidates = []
            for wh in WAREHOUSES:
                for sku in SKUS:
                    candidates.append(
                        (OP_INBOUND, {"warehouse": wh, "sku": sku,
                                      "qty": random.randint(5, 30)}))
                    stock = inv.get((wh, sku), 0)
                    if stock > 0:
                        candidates.append(
                            (OP_OUTBOUND, {"warehouse": wh, "sku": sku,
                                           "qty": random.randint(1, min(10, stock))}))
            for src in WAREHOUSES:
                for dst in WAREHOUSES:
                    if src == dst:
                        continue
                    for sku in SKUS:
                        stock = inv.get((src, sku), 0)
                        if stock > 0:
                            candidates.append(
                                (OP_TRANSFER, {"from_warehouse": src, "to_warehouse": dst,
                                               "sku": sku,
                                               "qty": random.randint(1, min(10, stock))}))
            op_type, payload = random.choice(candidates)
            executed.append(execute_op(conn, op_type, payload))
        finally:
            conn.close()
    return executed


def inject_gap(paths) -> int:
    """注入断号：删除一条日志（优先删备份之后、影响重放区间的）。"""
    conn = connect(paths.live)
    try:
        seqs = [r["seq"] for r in conn.execute("SELECT seq FROM op_log ORDER BY seq")]
        if len(seqs) < 3:
            raise BizError("日志太短，先执行一些操作再注入断号")
        meta = connect(paths.meta)
        try:
            bmax = meta.execute(
                "SELECT COALESCE(MAX(last_seq),0) AS m FROM backups").fetchone()["m"]
        finally:
            meta.close()
        top = seqs[-1]
        candidates = [s for s in seqs if s > bmax and s != top]
        if not candidates:
            candidates = [s for s in seqs if s not in (seqs[0], top)]
        victim = random.choice(candidates)
        conn.execute("DELETE FROM op_log WHERE seq=?", (victim,))
        conn.commit()
        return victim
    finally:
        conn.close()


def inject_corruption(paths) -> int:
    """注入损坏：篡改一条日志的完整输入（数量 +7），但不更新校验值。"""
    conn = connect(paths.live)
    try:
        rows = conn.execute("SELECT seq, payload FROM op_log ORDER BY seq").fetchall()
        if not rows:
            raise BizError("日志为空，先执行一些操作再注入损坏")
        victim = random.choice(rows)
        payload = json.loads(victim["payload"])
        payload["qty"] = int(payload.get("qty", 1)) + 7
        conn.execute("UPDATE op_log SET payload=? WHERE seq=?",
                     (canonical(payload), victim["seq"]))
        conn.commit()
        return victim["seq"]
    finally:
        conn.close()


def reset_all(paths) -> None:
    """清空全部数据（业务库、元数据库、备份、暂存），重新生成种子数据。"""
    for f in (paths.live, paths.meta):
        Path(f).unlink(missing_ok=True)
    for d in (paths.backups, paths.staging):
        shutil.rmtree(d, ignore_errors=True)
    paths.ensure_dirs()
    init_live_db(paths.live)
    store.init_meta_db(paths.meta)
    seed_if_empty(paths)
