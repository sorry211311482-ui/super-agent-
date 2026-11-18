"""把本地消息交给 Router 和 Worker，SQLite 保存事件、任务和回复。"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


class Engine:
    def __init__(self, path):
        self.path = str(path)
        if self.path != ":memory:":
            self.path = str(Path(path).expanduser().resolve())
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS tickets (
                id TEXT PRIMARY KEY,
                thread TEXT NOT NULL UNIQUE,
                worker TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                thread TEXT NOT NULL,
                text TEXT NOT NULL,
                attachment TEXT,
                ticket_id TEXT REFERENCES tickets(id),
                status TEXT NOT NULL DEFAULT 'queued',
                response TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS thread_events ON events(thread, seq);
        """)

    def receive(self, thread, text, attachment=None, event_id=None):
        """持久化后即返回事件 ID，相同来源的重复投递沿用同一 ID。"""
        if not isinstance(thread, str) or not thread.strip():
            raise ValueError("线程名称不能为空")
        if not isinstance(text, str):
            raise ValueError("消息正文必须是文本")
        if attachment is not None:
            if (not isinstance(attachment, dict) or set(attachment) != {"name", "text"}
                    or not all(isinstance(value, str) for value in attachment.values())):
                raise ValueError("附件需要 name 和 text 两个文本字段")
            attachment = json.dumps(attachment, ensure_ascii=False, sort_keys=True)
        event_id = uuid4().hex if event_id is None else event_id
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("事件 ID 不能为空")
        payload = (thread, text, attachment)
        with self.db:
            inserted = self.db.execute("""
                INSERT INTO events (event_id, thread, text, attachment) VALUES (?, ?, ?, ?)
                ON CONFLICT(event_id) DO NOTHING
            """, (event_id, *payload))
            if not inserted.rowcount:
                stored = self.db.execute(
                    "SELECT thread, text, attachment FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                if tuple(stored) != payload:
                    raise ValueError(f"事件 ID {event_id} 已用于另一条消息")
        return event_id

    @staticmethod
    def _message(event):
        return {"role": "user", "content": event["text"],
                "attachment": json.loads(event["attachment"]) if event["attachment"] else None}

    def history(self, thread):
        """已交给该任务的对话。发生异常的输入保留下来，供下一轮继续。"""
        rows = self.db.execute("""
            SELECT text, attachment, status, response FROM events
            WHERE thread = ? AND ticket_id IS NOT NULL AND status != 'queued' ORDER BY seq
        """, (thread,))
        messages = []
        for row in rows:
            messages.append(self._message(row))
            if row["status"] in ("ready", "waiting"):
                messages.append({"role": "assistant", "content": row["response"], "attachment": None})
        return messages

    def result(self, event_id):
        row = self.db.execute("""
            SELECT e.event_id, e.thread, e.ticket_id, t.worker, e.status, e.response
            FROM events AS e LEFT JOIN tickets AS t ON t.id = e.ticket_id
            WHERE e.event_id = ? AND e.status != 'queued'
        """, (event_id,)).fetchone()
        return dict(row) if row else None

    @contextmanager
    def _one_runner(self):
        if self.path == ":memory:":
            yield
            return
        # 进程退出时系统释放锁。执行器运行期间其他进程仍可用 receive 入队。
        import fcntl
        with open(self.path + ".lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def run(self, route, workers):
        """按到达顺序处理本轮开始时已入队的消息。Worker 仅返回结果。"""
        with self._one_runner():
            events = self.db.execute(
                "SELECT * FROM events WHERE status = 'queued' ORDER BY seq"
            ).fetchall()
            return [self._dispatch(event, route, workers) for event in events]

    def _dispatch(self, event, route, workers):
        thread = event["thread"]
        message = self._message(event)
        ticket = self.db.execute("SELECT id, worker FROM tickets WHERE thread = ?", (thread,)).fetchone()
        ticket_id = ticket["id"] if ticket else None
        status, response = "ignored", ""
        try:
            worker_name = ticket["worker"] if ticket else route(
                {**message, "thread": thread, "event_id": event["event_id"]}
            )
            if worker_name is not None:
                worker = workers[worker_name]
                if ticket is None:
                    new_id = uuid4().hex
                    with self.db:
                        self.db.execute("INSERT INTO tickets (id, thread, worker) VALUES (?, ?, ?)",
                                        (new_id, thread, worker_name))
                    ticket_id = new_id
                # 网络调用发生在事务外。中断后事件仍在队列，并沿用已落盘的任务。
                status, response = worker(self.history(thread) + [message])
                if status not in ("ready", "waiting") or not isinstance(response, str):
                    raise ValueError("Worker 应返回 (ready 或 waiting, 回复文本)")
        except Exception as exc:
            status, response = "failed", f"{type(exc).__name__}: {exc}"
        with self.db:
            self.db.execute("UPDATE events SET ticket_id = ?, status = ?, response = ? WHERE seq = ?",
                            (ticket_id, status, response, event["seq"]))
        return self.result(event["event_id"])

    def close(self):
        self.db.close()
