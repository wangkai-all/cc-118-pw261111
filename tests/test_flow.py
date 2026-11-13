"""端到端逻辑测试：台账、日志链、一致备份、独立库恢复、故障检测、确认切换。"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import demo, store  # noqa: E402
from app.backup import create_backup  # noqa: E402
from app.core import (OP_INBOUND, OP_OUTBOUND, OP_TRANSFER, ReplayError,  # noqa: E402
                      apply_payload, connect, execute_op, init_live_db,
                      list_inventory, max_seq, replay_one, verify_chain)
from app.paths import Paths  # noqa: E402
from app.restore import (RestoreError, compare_with_live, confirm_restore,  # noqa: E402
                         discard_restore, start_restore)


def model_apply(model, op_type, p):
    """纯 Python 参照模型，用来和数据库重放结果对照。"""
    if op_type == OP_INBOUND:
        model[(p["warehouse"], p["sku"])] = model.get((p["warehouse"], p["sku"]), 0) + p["qty"]
    elif op_type == OP_OUTBOUND:
        model[(p["warehouse"], p["sku"])] = model.get((p["warehouse"], p["sku"]), 0) - p["qty"]
    elif op_type == OP_TRANSFER:
        model[(p["from_warehouse"], p["sku"])] = model.get((p["from_warehouse"], p["sku"]), 0) - p["qty"]
        model[(p["to_warehouse"], p["sku"])] = model.get((p["to_warehouse"], p["sku"]), 0) + p["qty"]


def db_inventory(path):
    conn = connect(path)
    try:
        return {(r["warehouse"], r["sku"]): r["qty"] for r in list_inventory(conn)}
    finally:
        conn.close()


class FlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.paths = Paths(Path(self.tmp.name) / "data")
        self.paths.ensure_dirs()
        init_live_db(self.paths.live)
        store.init_meta_db(self.paths.meta)
        demo.seed_if_empty(self.paths)

    def tearDown(self):
        self.tmp.cleanup()

    def live_conn(self):
        return connect(self.paths.live)

    def run_ops(self, n=10):
        return demo.continuous_ops(self.paths, n)

    # ---------------------------------------------------------- 基础链路

    def test_seed_chain_and_same_transaction(self):
        conn = self.live_conn()
        try:
            v = verify_chain(conn)
            self.assertTrue(v["ok"])
            self.assertEqual(v["last_seq"], len(demo.SEED_OPS))
            seqs = [r["seq"] for r in conn.execute("SELECT seq FROM op_log ORDER BY seq")]
            self.assertEqual(seqs, list(range(1, len(demo.SEED_OPS) + 1)))
        finally:
            conn.close()

    def test_business_error_rolls_back_log_too(self):
        conn = self.live_conn()
        try:
            before = max_seq(conn)
            with self.assertRaises(Exception):
                execute_op(conn, OP_OUTBOUND,
                           {"warehouse": "主仓", "sku": "键盘", "qty": 10 ** 9})
            # 业务失败时日志也不能留下记录（同一事务）
            self.assertEqual(max_seq(conn), before)
            self.assertTrue(verify_chain(conn)["ok"])
        finally:
            conn.close()

    # ---------------------------------------------------------- 备份 + 恢复 + 切换

    def test_backup_restore_confirm_matches_model(self):
        self.run_ops(5)
        bid = create_backup(self.paths, note="t")
        b = store.get_backup(self.paths.meta, bid)
        base_seq = b["last_seq"]
        self.run_ops(8)
        conn = self.live_conn()
        try:
            live_max = max_seq(conn)
            # 参照模型：从日志重放到目标序号
            rows = conn.execute("SELECT * FROM op_log ORDER BY seq").fetchall()
        finally:
            conn.close()
        target = (base_seq + live_max) // 2
        model = {}
        for r in rows:
            if r["seq"] > target:
                break
            model_apply(model, r["op_type"], json.loads(r["payload"]))

        rid = start_restore(self.paths, bid, target)
        rec = store.get_restore(self.paths.meta, rid)
        self.assertEqual(rec["status"], "staged")
        self.assertEqual(db_inventory(rec["staging_file"]), model)
        # 未确认前当前库不动
        self.assertNotEqual(db_inventory(self.paths.live), model)
        cmp = compare_with_live(self.paths, rid)
        self.assertEqual(len(cmp["discarded"]), live_max - target)
        confirm_restore(self.paths, rid)
        self.assertEqual(db_inventory(self.paths.live), model)
        self.assertEqual(store.get_restore(self.paths.meta, rid)["status"], "confirmed")
        # 切换后还能继续入账，序号接着目标走
        conn = self.live_conn()
        try:
            res = execute_op(conn, OP_INBOUND, {"warehouse": "主仓", "sku": "键盘", "qty": 3})
            self.assertEqual(res["seq"], target + 1)
            self.assertTrue(verify_chain(conn)["ok"])
        finally:
            conn.close()

    def test_restore_to_backup_point_only(self):
        self.run_ops(4)
        bid = create_backup(self.paths)
        b = store.get_backup(self.paths.meta, bid)
        self.run_ops(6)
        rid = start_restore(self.paths, bid, b["last_seq"])  # 纯快照，零重放
        rec = store.get_restore(self.paths.meta, rid)
        self.assertEqual(rec["status"], "staged")
        self.assertEqual(db_inventory(rec["staging_file"]), db_inventory(b["file"]))

    # ---------------------------------------------------------- 幂等与原子性

    def test_replay_rejects_double_apply(self):
        conn = self.live_conn()
        try:
            row = conn.execute("SELECT * FROM op_log WHERE seq=1").fetchone()
        finally:
            conn.close()
        staging = self.paths.staging / "idem.db"
        init_live_db(staging)
        st = connect(staging)
        try:
            replay_one(st, row)
            with self.assertRaises(ReplayError):
                replay_one(st, row)  # 同一笔不能应用两次
        finally:
            st.close()

    def test_transfer_applies_both_sides_atomically(self):
        conn = self.live_conn()
        try:
            execute_op(conn, OP_TRANSFER,
                       {"from_warehouse": "主仓", "to_warehouse": "西区仓",
                        "sku": "显示器", "qty": 15})
            row = conn.execute(
                "SELECT * FROM op_log ORDER BY seq DESC LIMIT 1").fetchone()
        finally:
            conn.close()
        staging = self.paths.staging / "xfer.db"
        init_live_db(staging)
        st = connect(staging)
        try:
            # 补齐前置日志后重放调拨
            src = self.live_conn()
            try:
                for r in src.execute("SELECT * FROM op_log ORDER BY seq"):
                    replay_one(st, r)
            finally:
                src.close()
            inv = {(r["warehouse"], r["sku"]): r["qty"] for r in list_inventory(st)}
        finally:
            st.close()
        live_inv = db_inventory(self.paths.live)
        self.assertEqual(inv, live_inv)  # 两边都生效，与线上一致
        self.assertEqual(row["op_type"], OP_TRANSFER)

    # ---------------------------------------------------------- 故障检测

    def test_gap_stops_restore_and_reports(self):
        self.run_ops(4)
        bid = create_backup(self.paths)
        self.run_ops(6)
        victim = demo.inject_gap(self.paths)
        live_before = db_inventory(self.paths.live)
        b = store.get_backup(self.paths.meta, bid)
        conn = self.live_conn()
        try:
            target = max_seq(conn)
        finally:
            conn.close()
        if victim <= b["last_seq"]:
            self.skipTest("断号落在备份点之前，不影响本次重放区间")
        with self.assertRaises(RestoreError) as ctx:
            start_restore(self.paths, bid, target)
        self.assertIn("断号", str(ctx.exception))
        self.assertIn(str(victim), str(ctx.exception))
        # 失败不影响正在使用的数据
        self.assertEqual(db_inventory(self.paths.live), live_before)
        rec = store.list_restores(self.paths.meta)[0]
        self.assertEqual(rec["status"], "failed")

    def test_corruption_stops_restore(self):
        self.run_ops(4)
        bid = create_backup(self.paths)
        self.run_ops(6)
        victim = demo.inject_corruption(self.paths)
        b = store.get_backup(self.paths.meta, bid)
        conn = self.live_conn()
        try:
            target = max_seq(conn)
            self.assertFalse(verify_chain(conn)["ok"])  # 链校验能发现
        finally:
            conn.close()
        if victim <= b["last_seq"]:
            self.skipTest("损坏落在备份点之前")
        with self.assertRaises(RestoreError) as ctx:
            start_restore(self.paths, bid, target)
        self.assertIn("损坏", str(ctx.exception))

    def test_target_out_of_range(self):
        self.run_ops(3)
        bid = create_backup(self.paths)
        self.run_ops(2)
        conn = self.live_conn()
        try:
            live_max = max_seq(conn)
        finally:
            conn.close()
        with self.assertRaises(RestoreError) as ctx:
            start_restore(self.paths, bid, live_max + 5)
        self.assertIn("超出可恢复范围", str(ctx.exception))
        b = store.get_backup(self.paths.meta, bid)
        with self.assertRaises(RestoreError):
            start_restore(self.paths, bid, b["last_seq"] - 1)

    # ---------------------------------------------------------- 持久化与清理

    def test_records_survive_reopen(self):
        self.run_ops(3)
        bid = create_backup(self.paths, note="重启前")
        self.run_ops(2)
        rid = start_restore(self.paths, bid, store.get_backup(self.paths.meta, bid)["last_seq"])
        confirm_restore(self.paths, rid)
        # 模拟重开程序：仅依据磁盘上的 meta.db
        backups = store.list_backups(self.paths.meta)
        restores = store.list_restores(self.paths.meta)
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0]["note"], "重启前")
        self.assertEqual(restores[0]["status"], "confirmed")
        self.assertTrue(Path(backups[0]["file"]).exists())

    def test_discard_restore(self):
        self.run_ops(3)
        bid = create_backup(self.paths)
        self.run_ops(2)
        rid = start_restore(self.paths, bid,
                            store.get_backup(self.paths.meta, bid)["last_seq"] + 1)
        rec = store.get_restore(self.paths.meta, rid)
        staging_file = rec["staging_file"]
        self.assertTrue(Path(staging_file).exists())
        discard_restore(self.paths, rid)
        self.assertEqual(store.get_restore(self.paths.meta, rid)["status"], "discarded")
        self.assertFalse(Path(staging_file).exists())


class WebTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        from app.web import create_app
        self.app = create_app(data_dir=str(Path(self.tmp.name) / "data"))
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def test_pages_and_api(self):
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/backup").status_code, 200)
        r = self.client.post("/api/op", json={
            "op_type": "INBOUND", "warehouse": "主仓", "sku": "键盘", "qty": 5})
        self.assertTrue(r.get_json()["ok"])
        r = self.client.post("/api/op", json={
            "op_type": "OUTBOUND", "warehouse": "主仓", "sku": "键盘", "qty": 10 ** 9})
        self.assertFalse(r.get_json()["ok"])  # 库存不足
        r = self.client.post("/api/backup/create", json={"note": "web"})
        self.assertTrue(r.get_json()["ok"])
        r = self.client.post("/api/demo/continuous", json={"count": 5})
        self.assertTrue(r.get_json()["ok"])
        # 备份后恢复到备份点之后的序号（备份点为 #8，连续操作后到 #13）
        r = self.client.post("/api/restore/start", json={"backup_id": 1, "target_seq": 10})
        self.assertTrue(r.get_json()["ok"], r.get_json())
        rid = r.get_json()["restore_id"]
        r = self.client.post("/api/restore/confirm", json={"restore_id": rid})
        self.assertTrue(r.get_json()["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
