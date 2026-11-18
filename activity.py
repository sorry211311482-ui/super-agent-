"""Bounded task activity storage for local worker tool events."""

from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
import sqlite3


class ActivityLog:
    def __init__(self, state_dir, limit=120):
        if limit < 1:
            raise ValueError("活动记录上限须大于零")
        self.limit = limit
        self.path = Path(state_dir) / "activity.sqlite3"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db, db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""
                CREATE TABLE IF NOT EXISTS worker_activity (
                    seq INTEGER PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    label TEXT NOT NULL,
                    status TEXT,
                    path TEXT,
                    exit_code INTEGER
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS activity_task ON worker_activity(task_id, seq)")

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=2)
        db.row_factory = sqlite3.Row
        return closing(db)

    def record(self, task_id, event):
        kind = event.get("kind")
        label = event.get("label")
        if kind not in ("session", "tool_start", "tool_end", "file", "status"):
            return
        if not isinstance(label, str) or not label.strip():
            return
        status = event.get("status")
        if status not in ("running", "completed", "failed", "waiting", "ready"):
            status = None
        path = self._output_path(event.get("path"))
        exit_code = event.get("exit_code")
        if type(exit_code) is not int:
            exit_code = None
        ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        with self._connect() as db, db:
            db.execute("""
                INSERT INTO worker_activity(task_id, ts, kind, label, status, path, exit_code)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (task_id, ts, kind, label[:160], status, path, exit_code))
            db.execute("""
                DELETE FROM worker_activity WHERE task_id = ? AND seq <= (
                    SELECT seq FROM worker_activity WHERE task_id = ?
                    ORDER BY seq DESC LIMIT 1 OFFSET ?
                )
            """, (task_id, task_id, self.limit))

    def read(self, task_id):
        with self._connect() as db:
            rows = db.execute("""
                SELECT seq, ts, kind, label, status, path, exit_code
                FROM worker_activity WHERE task_id = ?
                ORDER BY seq DESC LIMIT ?
            """, (task_id, self.limit)).fetchall()
        return [{key: value for key, value in dict(row).items() if value is not None}
                for row in reversed(rows)]

    @staticmethod
    def _output_path(value):
        if not isinstance(value, str) or not value or len(value) > 512:
            return None
        if any(ord(char) < 32 or char in ("\\", "\x7f") for char in value):
            return None
        parts = PurePosixPath(value).parts
        if len(parts) < 2 or parts[0] != "outputs" or any(part.startswith(".") for part in parts):
            return None
        return PurePosixPath(value).as_posix()
