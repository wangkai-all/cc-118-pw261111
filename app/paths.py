"""运行时数据目录布局：业务库、元数据库、备份快照目录、恢复暂存目录。"""
from pathlib import Path


class Paths:
    def __init__(self, root):
        self.root = Path(root)

    @property
    def live(self):
        """当前正在使用的业务库（台账 + 操作日志）。"""
        return self.root / "live.db"

    @property
    def meta(self):
        """备份与恢复记录库，独立于业务库，程序重启后记录仍可查。"""
        return self.root / "meta.db"

    @property
    def backups(self):
        return self.root / "backups"

    @property
    def staging(self):
        """恢复演练用的独立数据库目录，失败不影响当前库。"""
        return self.root / "staging"

    def ensure_dirs(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self.backups.mkdir(exist_ok=True)
        self.staging.mkdir(exist_ok=True)
