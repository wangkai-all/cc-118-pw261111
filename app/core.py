"""库存台账核心逻辑。

设计要点：
- 业务变更（inventory）与重放所需的操作日志（op_log）在同一个 SQLite 事务中写入，
  要么都成功，要么都回滚，不会出现"变了库存但没记日志"。
- 每条日志带连续序号 seq、完整输入 payload（JSON）、上一条校验值 prev_checksum
  和本条校验值 checksum（链式 SHA-256），断号或篡改都能被验出来。
- 调拨的两边（调出扣减、调入增加）在同一个事务里生效，重放时也一样。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime

GENESIS = "0" * 64  # 第一条日志的 prev_checksum

OP_INBOUND = "INBOUND"
OP_OUTBOUND = "OUTBOUND"
OP_TRANSFER = "TRANSFER"

OP_LABELS = {
    OP_INBOUND: "入库",
    OP_OUTBOUND: "出库",
    OP_TRANSFER: "调拨",
}


class BizError(Exception):
    """业务校验失败（库存不足、输入非法等）。"""


class ReplayError(Exception):
    """重放违反顺序/幂等约束（重复应用、序号跳跃）。"""


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def canonical(payload: dict) -> str:
    """完整输入的规范化文本，校验值以此为准。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def checksum_of(prev_checksum: str, seq: int, op_type: str, payload: dict) -> str:
    raw = f"{prev_checksum}|{seq}|{op_type}|{canonical(payload)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


LIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS inventory (
  warehouse TEXT NOT NULL,
  sku       TEXT NOT NULL,
  qty       INTEGER NOT NULL CHECK (qty >= 0),
  PRIMARY KEY (warehouse, sku)
);
CREATE TABLE IF NOT EXISTS op_log (
  seq           INTEGER PRIMARY KEY,   -- 连续递增，断号即异常
  op_type       TEXT NOT NULL,
  payload       TEXT NOT NULL,          -- 完整输入（JSON）
  prev_checksum TEXT NOT NULL,          -- 上一条日志的校验值
  checksum      TEXT NOT NULL,          -- 本条校验值
  created_at    TEXT NOT NULL
);
"""


def connect(path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_live_db(path) -> None:
    conn = connect(path)
    try:
        conn.executescript(LIVE_SCHEMA)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------- 业务操作

def validate_payload(op_type: str, p: dict) -> dict:
    """把外部输入整理成规范的完整输入；非法输入抛 BizError。"""
    try:
        if op_type in (OP_INBOUND, OP_OUTBOUND):
            wh = str(p["warehouse"]).strip()
            sku = str(p["sku"]).strip()
            qty = int(p["qty"])
            if not wh or not sku or qty <= 0:
                raise ValueError
            return {"warehouse": wh, "sku": sku, "qty": qty}
        if op_type == OP_TRANSFER:
            src = str(p["from_warehouse"]).strip()
            dst = str(p["to_warehouse"]).strip()
            sku = str(p["sku"]).strip()
            qty = int(p["qty"])
            if not src or not sku or qty <= 0:
                raise ValueError
            if src == dst:
                raise BizError("调拨的调出仓库与调入仓库不能相同")
            return {"from_warehouse": src, "to_warehouse": dst, "sku": sku, "qty": qty}
    except (KeyError, TypeError, ValueError):
        raise BizError("输入不完整，或数量不是正整数")
    raise BizError(f"未知操作类型: {op_type}")


def _add_qty(conn, warehouse: str, sku: str, delta: int) -> None:
    row = conn.execute(
        "SELECT qty FROM inventory WHERE warehouse=? AND sku=?", (warehouse, sku)
    ).fetchone()
    cur = row["qty"] if row else 0
    new = cur + delta
    if new < 0:
        raise BizError(f"库存不足：{warehouse} / {sku} 现有 {cur}，需要 {-delta}")
    conn.execute(
        "INSERT INTO inventory(warehouse, sku, qty) VALUES (?,?,?) "
        "ON CONFLICT(warehouse, sku) DO UPDATE SET qty=excluded.qty",
        (warehouse, sku, new),
    )


def apply_payload(conn, op_type: str, payload: dict) -> None:
    """按完整输入变更库存。调拨的两边在同一次调用中生效，
    调用方负责把它们包在一个事务里，保证不会只扣不加。"""
    if op_type == OP_INBOUND:
        _add_qty(conn, payload["warehouse"], payload["sku"], payload["qty"])
    elif op_type == OP_OUTBOUND:
        _add_qty(conn, payload["warehouse"], payload["sku"], -payload["qty"])
    elif op_type == OP_TRANSFER:
        _add_qty(conn, payload["from_warehouse"], payload["sku"], -payload["qty"])
        _add_qty(conn, payload["to_warehouse"], payload["sku"], payload["qty"])
    else:
        raise BizError(f"未知操作类型: {op_type}")


def execute_op(conn, op_type: str, raw_payload: dict) -> dict:
    """执行一笔业务事务：库存变更 + 操作日志在同一个数据库事务中落库。"""
    payload = validate_payload(op_type, raw_payload)
    try:
        conn.execute("BEGIN IMMEDIATE")
        apply_payload(conn, op_type, payload)
        last = conn.execute(
            "SELECT seq, checksum FROM op_log ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        seq = (last["seq"] + 1) if last else 1
        prev = last["checksum"] if last else GENESIS
        ck = checksum_of(prev, seq, op_type, payload)
        conn.execute(
            "INSERT INTO op_log(seq, op_type, payload, prev_checksum, checksum, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (seq, op_type, canonical(payload), prev, ck, now_iso()),
        )
        conn.commit()
        return {"seq": seq, "checksum": ck, "op_type": op_type, "payload": payload}
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------- 校验与查询

def verify_chain(conn, upto_seq: int | None = None) -> dict:
    """校验日志链：序号必须连续（断号报错），每条校验值必须匹配（损坏报错）。"""
    prev = GENESIS
    expect = 1
    count = 0
    rows = conn.execute(
        "SELECT seq, op_type, payload, prev_checksum, checksum FROM op_log ORDER BY seq"
    )
    for r in rows:
        if upto_seq is not None and r["seq"] > upto_seq:
            break
        if r["seq"] != expect:
            return {"ok": False, "kind": "gap", "seq": expect,
                    "detail": f"日志断号：期望序号 #{expect}，实际读到 #{r['seq']}"}
        try:
            payload = json.loads(r["payload"])
        except ValueError:
            return {"ok": False, "kind": "corrupt", "seq": r["seq"],
                    "detail": f"日志损坏：序号 #{r['seq']} 的输入内容不是合法 JSON"}
        if r["prev_checksum"] != prev or checksum_of(prev, r["seq"], r["op_type"], payload) != r["checksum"]:
            return {"ok": False, "kind": "corrupt", "seq": r["seq"],
                    "detail": f"日志损坏：序号 #{r['seq']} 的校验值与内容不匹配"}
        prev = r["checksum"]
        expect += 1
        count += 1
    return {"ok": True, "last_seq": expect - 1, "head": prev, "count": count}


def replay_one(conn, row) -> None:
    """在独立库中重放一条日志。

    - 库存变更与日志写入在同一事务（调拨两边同时生效）；
    - 序号必须恰好接上，已应用过的序号拒绝再次应用（同一笔不能应用两次）。
    """
    payload = json.loads(row["payload"])
    try:
        conn.execute("BEGIN IMMEDIATE")
        m = conn.execute("SELECT COALESCE(MAX(seq),0) AS m FROM op_log").fetchone()["m"]
        if row["seq"] <= m:
            raise ReplayError(f"序号 #{row['seq']} 已应用过，同一笔事务不能应用两次")
        if row["seq"] != m + 1:
            raise ReplayError(f"序号不连续：暂存库已到 #{m}，不能应用 #{row['seq']}")
        apply_payload(conn, row["op_type"], payload)
        conn.execute(
            "INSERT INTO op_log(seq, op_type, payload, prev_checksum, checksum, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (row["seq"], row["op_type"], row["payload"],
             row["prev_checksum"], row["checksum"], row["created_at"]),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def list_inventory(conn):
    return conn.execute(
        "SELECT warehouse, sku, qty FROM inventory ORDER BY warehouse, sku"
    ).fetchall()


def list_log(conn, limit: int = 200):
    return conn.execute(
        "SELECT * FROM op_log ORDER BY seq DESC LIMIT ?", (limit,)
    ).fetchall()


def max_seq(conn) -> int:
    return conn.execute("SELECT COALESCE(MAX(seq),0) AS m FROM op_log").fetchone()["m"]


def describe(op_type: str, payload) -> str:
    if isinstance(payload, str):
        payload = json.loads(payload)
    if op_type == OP_INBOUND:
        return f"入库 {payload['warehouse']} · {payload['sku']} ×{payload['qty']}"
    if op_type == OP_OUTBOUND:
        return f"出库 {payload['warehouse']} · {payload['sku']} ×{payload['qty']}"
    if op_type == OP_TRANSFER:
        return (f"调拨 {payload['from_warehouse']}→{payload['to_warehouse']}"
                f" · {payload['sku']} ×{payload['qty']}")
    return str(op_type)
