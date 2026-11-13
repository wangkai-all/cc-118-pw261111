"""Flask 页面与接口。

两个页面：
- /       库存台账：库存表、入库/出库/调拨表单、连续操作演示、事务日志与链状态
- /backup 备份与恢复：生成一致备份、选择目标序号在独立库恢复、对照、确认切换、
          备份/恢复记录、故障注入

所有写操作共用一把进程级锁，保证序号连续、备份快照与记录一致。
"""
import json
import os
import threading

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from . import demo, store
from . import backup as backup_mod
from . import restore as restore_mod
from .core import (OP_LABELS, BizError, connect, describe, init_live_db,
                   list_inventory, list_log, max_seq, verify_chain, execute_op)
from .paths import Paths

LOCK = threading.RLock()

DEFAULT_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def _ok(message, **kw):
    return jsonify({"ok": True, "message": message, **kw})


def _fail(error, status=400):
    return jsonify({"ok": False, "error": str(error)}), status


def create_app(data_dir=None):
    paths = Paths(data_dir or os.environ.get("LEDGER_DATA", DEFAULT_DATA_DIR))
    paths.ensure_dirs()
    init_live_db(paths.live)
    store.init_meta_db(paths.meta)
    with LOCK:
        restore_mod.recover_interrupted(paths)  # 重启后清理中断的恢复
        demo.seed_if_empty(paths)

    app = Flask(__name__)

    # ------------------------------------------------------------ 视图数据

    def live_view():
        conn = connect(paths.live)
        try:
            return {
                "integrity": verify_chain(conn),
                "inventory": [dict(r) for r in list_inventory(conn)],
                "log": [dict(r) for r in list_log(conn, 200)],
                "max_seq": max_seq(conn),
            }
        finally:
            conn.close()

    # ------------------------------------------------------------ 页面

    @app.get("/")
    def ledger_page():
        view = live_view()
        return render_template(
            "ledger.html", active="ledger",
            integrity=view["integrity"], inventory=view["inventory"],
            log=view["log"], warehouses=demo.WAREHOUSES, skus=demo.SKUS)

    @app.get("/backup")
    def backup_page():
        view = live_view()
        backups = store.list_backups(paths.meta)
        restores = store.list_restores(paths.meta)
        active_restore = next((r for r in restores if r["status"] in ("running", "staged")), None)
        comparison = None
        if active_restore and active_restore["status"] == "staged":
            try:
                comparison = restore_mod.compare_with_live(paths, active_restore["id"])
            except restore_mod.RestoreError:
                comparison = None
        log_entries = [
            {"seq": r["seq"], "label": f"#{r['seq']} {describe(r['op_type'], r['payload'])}"}
            for r in reversed(view["log"])
        ]
        backups_js = [
            {"id": b["id"], "last_seq": b["last_seq"],
             "created_at": b["created_at"], "note": b["note"]}
            for b in backups
        ]
        return render_template(
            "backup.html", active="backup",
            integrity=view["integrity"],
            backups=backups, restores=restores,
            active_restore=active_restore, comparison=comparison,
            backups_json=json.dumps(backups_js, ensure_ascii=False),
            log_json=json.dumps(log_entries, ensure_ascii=False),
            status_labels=store.RESTORE_STATUS_LABELS)

    # ------------------------------------------------------------ 业务操作

    @app.post("/api/op")
    def api_op():
        data = request.get_json(force=True, silent=True) or {}
        with LOCK:
            conn = connect(paths.live)
            try:
                res = execute_op(conn, data.get("op_type", ""), data)
                return _ok(f"已入账，事务序号 #{res['seq']}", seq=res["seq"])
            except BizError as e:
                return _fail(e)
            finally:
                conn.close()

    # ------------------------------------------------------------ 备份

    @app.post("/api/backup/create")
    def api_backup_create():
        data = request.get_json(force=True, silent=True) or {}
        with LOCK:
            try:
                bid = backup_mod.create_backup(paths, note=str(data.get("note", ""))[:200])
                b = store.get_backup(paths.meta, bid)
                return _ok(f"备份 #{bid} 已生成，对应最后事务序号 #{b['last_seq']}")
            except Exception as e:
                return _fail(f"备份失败：{e}", 500)

    # ------------------------------------------------------------ 恢复

    @app.post("/api/restore/start")
    def api_restore_start():
        data = request.get_json(force=True, silent=True) or {}
        with LOCK:
            try:
                rid = restore_mod.start_restore(
                    paths, int(data.get("backup_id")), int(data.get("target_seq")))
                rec = store.get_restore(paths.meta, rid)
                return _ok(
                    f"恢复 #{rid} 已在独立库中重放完成（#{rec['backup_last_seq']} → "
                    f"#{rec['target_seq']}），请核对后确认切换", restore_id=rid)
            except (restore_mod.RestoreError, BizError) as e:
                return _fail(e)
            except Exception as e:
                return _fail(f"恢复失败：{e}", 500)

    @app.post("/api/restore/confirm")
    def api_restore_confirm():
        data = request.get_json(force=True, silent=True) or {}
        with LOCK:
            try:
                restore_mod.confirm_restore(paths, int(data.get("restore_id")))
                return _ok("已确认切换，当前台账已替换为恢复结果")
            except restore_mod.RestoreError as e:
                return _fail(e)

    @app.post("/api/restore/discard")
    def api_restore_discard():
        data = request.get_json(force=True, silent=True) or {}
        with LOCK:
            try:
                restore_mod.discard_restore(paths, int(data.get("restore_id")))
                return _ok("已放弃本次恢复，暂存库已清理")
            except restore_mod.RestoreError as e:
                return _fail(e)

    # ------------------------------------------------------------ 演示

    @app.post("/api/demo/continuous")
    def api_demo_continuous():
        data = request.get_json(force=True, silent=True) or {}
        count = max(1, min(100, int(data.get("count", 10))))
        with LOCK:
            executed = demo.continuous_ops(paths, count)
            return _ok(f"已连续执行 {len(executed)} 笔业务事务，"
                       f"最新序号 #{executed[-1]['seq']}")

    @app.post("/api/demo/inject_gap")
    def api_demo_inject_gap():
        with LOCK:
            try:
                victim = demo.inject_gap(paths)
                return _ok(f"已注入断号：日志序号 #{victim} 被删除。"
                           "可到备份页发起覆盖该序号的恢复，观察缺口报错")
            except BizError as e:
                return _fail(e)

    @app.post("/api/demo/inject_corruption")
    def api_demo_inject_corruption():
        with LOCK:
            try:
                victim = demo.inject_corruption(paths)
                return _ok(f"已注入损坏：日志序号 #{victim} 的输入被篡改（校验值未更新）。"
                           "可到备份页发起覆盖该序号的恢复，观察校验报错")
            except BizError as e:
                return _fail(e)

    @app.post("/api/demo/reset")
    def api_demo_reset():
        with LOCK:
            demo.reset_all(paths)
            return _ok("演示数据已重置：台账、备份与恢复记录全部重新初始化")

    # ------------------------------------------------------------ 模板辅助

    @app.template_filter("oplabel")
    def _oplabel(op_type):
        return OP_LABELS.get(op_type, op_type)

    @app.template_filter("short")
    def _short(s, n=16):
        return (s or "")[:n]

    app.jinja_env.globals["describe"] = describe

    @app.errorhandler(Exception)
    def _handle_error(e):
        if isinstance(e, HTTPException):
            return e
        if request.path.startswith("/api/"):
            return _fail(f"服务器内部错误：{e}", 500)
        raise e

    return app
