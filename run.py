#!/usr/bin/env python3
"""启动入口：python3 run.py  （数据目录默认 ./data，可用 LEDGER_DATA 覆盖）"""
import os

from app.web import create_app

app = create_app()

if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    print(f"库存台账演示已启动: http://{host}:{port}  （备份恢复页: http://{host}:{port}/backup）")
    app.run(host=host, port=port, threaded=True)
