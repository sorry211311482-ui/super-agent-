"""Persistent event routing and isolated task scheduling."""

import fcntl
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path


class _AmbiguousAttachments(ValueError):
    pass


class Orchestrator:
    def __init__(self, state_dir, assess, execute, slots=2):
        if slots < 1:
            raise ValueError("Worker 数量至少为 1")
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.assess = assess
        self.execute = execute
        self.slots = slots
        self.changed = threading.Condition()
        self.stopping = False
        self.threads = []
        self.service_lock = None
        self.db = sqlite3.connect(str(self.state_dir / "tasks.sqlite3"), timeout=10,
                                  check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                thread TEXT,
                reply_to TEXT,
                watch TEXT,
                text TEXT NOT NULL,
                files TEXT NOT NULL,
                routed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(source, source_id)
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                goal TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                summary TEXT NOT NULL DEFAULT '',
                question TEXT NOT NULL DEFAULT '',
                files TEXT NOT NULL DEFAULT '[]',
                session_id TEXT,
                updated REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                task_id TEXT NOT NULL REFERENCES tasks(id),
                thread TEXT NOT NULL,
                PRIMARY KEY(task_id, thread)
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                task_id TEXT NOT NULL REFERENCES tasks(id),
                event_id TEXT NOT NULL REFERENCES events(id),
                text TEXT NOT NULL,
                done INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(task_id, event_id)
            );
            CREATE TABLE IF NOT EXISTS messages (
                seq INTEGER PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL,
                text TEXT NOT NULL,
                task_id TEXT REFERENCES tasks(id),
                files TEXT NOT NULL,
                source TEXT
            );
            CREATE TABLE IF NOT EXISTS clarifications (
                message_id TEXT PRIMARY KEY REFERENCES messages(id),
                event_id TEXT NOT NULL REFERENCES events(id)
            );
            CREATE INDEX IF NOT EXISTS pending_events ON events(seq) WHERE routed = 0;
            CREATE INDEX IF NOT EXISTS pending_deliveries ON deliveries(task_id) WHERE done = 0;
            CREATE INDEX IF NOT EXISTS subscriptions_by_thread ON subscriptions(thread);
        """)

    def start(self):
        with self.changed:
            if self.stopping:
                raise RuntimeError("服务已经关闭")
            if self.threads:
                return
            lock = (self.state_dir / "service.lock").open("a")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lock.close()
                raise RuntimeError("该状态目录已经有一个 SuperAgent 服务在运行") from None
            self.service_lock = lock
            with self.db:
                self.db.execute("UPDATE tasks SET status = 'queued' WHERE status = 'running'")
            self.threads = [threading.Thread(target=self._route_loop, name="router")]
            self.threads.extend(threading.Thread(target=self._worker_loop, name=f"worker-{index + 1}")
                                for index in range(self.slots))
            for thread in self.threads:
                thread.start()

    def close(self):
        with self.changed:
            if self.stopping:
                return
            self.stopping = True
            self.changed.notify_all()
        for thread in self.threads:
            thread.join()
        with self.changed:
            self.db.close()
            if self.service_lock is not None:
                self.service_lock.close()

    def submit(self, text, files=(), source="chat", thread=None, source_id=None,
               reply_to=None, watch=None):
        if not isinstance(text, str) or not text.strip():
            raise ValueError("消息正文不能为空")
        event_id = uuid.uuid4().hex
        # Sources own verification and snapshotting of their attachments.
        paths = [str(Path(path).expanduser().resolve()) for path in files]
        if watch is not None:
            watch = str(Path(watch).expanduser().resolve())
        if source == "directory" and thread is not None:
            thread = str(Path(thread).expanduser().resolve())
        source_id = event_id if source_id is None else str(source_id)
        with self.changed, self.db:
            if self.stopping:
                raise RuntimeError("服务已经关闭")
            existing = self.db.execute(
                "SELECT id FROM events WHERE source = ? AND source_id = ?", (source, source_id)
            ).fetchone()
            if existing:
                return existing["id"]
            if reply_to is not None and (source != "chat" or not self._task_exists(reply_to)):
                raise ValueError("找不到可直接回复的任务")
            self.db.execute("""
                INSERT INTO events (id, source, source_id, thread, reply_to, watch, text, files)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (event_id, source, source_id, thread, reply_to, watch, text, json.dumps(paths)))
            targets = [reply_to] if reply_to else self._subscribers(thread)
            # Directory updates only enter existing subscriptions.
            routed = bool(targets) or source == "directory"
            if routed:
                for task_id in targets:
                    self._deliver(task_id, event_id, text, watch if source == "chat" else None)
                self.db.execute("UPDATE events SET routed = 1 WHERE id = ?", (event_id,))
            if source == "chat" or targets:
                self._post("user", text, targets[0] if len(targets) == 1 else None,
                           [Path(path).name for path in paths], source=source, message_id=event_id)
            self.changed.notify_all()
        return event_id

    def _subscribers(self, thread):
        if thread is None:
            return []
        rows = self.db.execute("SELECT task_id FROM subscriptions WHERE thread = ? ORDER BY task_id",
                               (thread,))
        return [row["task_id"] for row in rows]

    def watch_directories(self):
        with self.changed:
            rows = self.db.execute("SELECT DISTINCT thread FROM subscriptions ORDER BY thread")
            return [row["thread"] for row in rows]

    def snapshot(self):
        with self.changed:
            return {"tasks": self._tasks(), "messages": self._messages()}

    def _task_exists(self, task_id):
        return self.db.execute("SELECT 1 FROM tasks WHERE id = ?", (task_id,)).fetchone() is not None

    def _tasks(self, limit=-1):
        rows = self.db.execute("""
            SELECT id, title, goal, status, summary, question, files FROM tasks
            ORDER BY updated DESC LIMIT ?
        """, (limit,)).fetchall()
        tasks = {row["id"]: {**dict(row), "files": json.loads(row["files"]), "watch": []}
                 for row in rows}
        for row in self.db.execute("SELECT task_id, thread FROM subscriptions ORDER BY thread"):
            if row["task_id"] in tasks:
                tasks[row["task_id"]]["watch"].append(row["thread"])
        return list(tasks.values())

    def _messages(self, limit=-1, before=None):
        rows = self.db.execute("""
            SELECT id, role, text, task_id, files, source FROM messages
            WHERE ? IS NULL OR seq < (SELECT seq FROM messages WHERE id = ?)
            ORDER BY seq DESC LIMIT ?
        """, (before, before, limit)).fetchall()
        return [{**dict(row), "files": json.loads(row["files"])} for row in reversed(rows)]

    def _post(self, role, text, task_id=None, files=(), *, source=None, message_id=None):
        self.db.execute("""
            INSERT INTO messages (id, role, text, task_id, files, source) VALUES (?, ?, ?, ?, ?, ?)
        """, (message_id or uuid.uuid4().hex, role, text, task_id, json.dumps(files), source))

    def _ask(self, event, text):
        message_id = uuid.uuid4().hex
        self._post("assistant", text, source="router", message_id=message_id)
        self.db.execute("INSERT INTO clarifications (message_id, event_id) VALUES (?, ?)",
                        (message_id, event["id"]))

    def _reply_context(self, event_id):
        rows = self.db.execute("""
            SELECT earlier.text AS request, earlier.files, earlier.watch, question.text AS question
            FROM clarifications AS c
            JOIN events AS earlier ON earlier.id = c.event_id
            JOIN messages AS question ON question.id = c.message_id
            WHERE earlier.id = (
                SELECT id FROM events
                WHERE source = 'chat' AND seq < (SELECT seq FROM events WHERE id = ?)
                ORDER BY seq DESC LIMIT 1
            )
            AND question.seq < (SELECT seq FROM messages WHERE id = ?)
            ORDER BY question.seq
        """, (event_id, event_id))
        return [{**dict(row), "files": [Path(path).name for path in json.loads(row["files"])]}
                for row in rows]

    @staticmethod
    def _event(row):
        event = {field: row[field] for field in ("id", "text", "source", "thread", "reply_to", "watch")}
        event["files"] = json.loads(row["files"])
        return event

    def _route_loop(self):
        while True:
            with self.changed:
                if self.stopping:
                    return
                row = self.db.execute("SELECT * FROM events WHERE routed = 0 ORDER BY seq LIMIT 1").fetchone()
                if row is None:
                    self.changed.wait()
                    continue
                event = self._event(row)
                # Include clarification questions and preceding discussion as routing context.
                event["recent"] = self._messages(limit=16, before=event["id"])
                event["reply_context"] = self._reply_context(event["id"])
                tasks = self._tasks(limit=40)
            try:
                decisions = self.assess(event, tasks)
                with self.changed, self.db:
                    self._apply_decisions(event, decisions)
                    self.db.execute("UPDATE events SET routed = 1 WHERE id = ?", (event["id"],))
                    self.changed.notify_all()
            except _AmbiguousAttachments as error:
                with self.changed, self.db:
                    self._ask(event, str(error))
                    self.db.execute("UPDATE events SET routed = 1 WHERE id = ?", (event["id"],))
                    self.changed.notify_all()
            except Exception as error:
                with self.changed, self.db:
                    self._post("assistant", f"任务分派失败：{error}", source="router")
                    self.db.execute("UPDATE events SET routed = 1 WHERE id = ?", (event["id"],))
                    self.changed.notify_all()

    def _apply_decisions(self, event, decisions):
        if not isinstance(decisions, list) or not decisions:
            raise ValueError("Router 需要返回至少一项决定")
        targets = {}
        for decision in decisions:
            if not isinstance(decision, dict):
                raise ValueError("Router 返回的决定格式无效")
            action = decision.get("action")
            if action == "ignore":
                continue
            if action == "ask":
                self._ask(event, self._nonempty(decision, "text"))
                continue
            if action == "new":
                task_id = uuid.uuid4().hex
                self.db.execute("INSERT INTO tasks (id, title, goal, updated) VALUES (?, ?, ?, ?)",
                                (task_id, self._nonempty(decision, "title"),
                                 self._nonempty(decision, "goal"), time.time()))
            elif action == "continue":
                task_id = self._nonempty(decision, "task_id")
                if not self._task_exists(task_id):
                    raise ValueError("Router 指向了不存在的任务")
            else:
                raise ValueError("Router 返回了无法识别的动作")
            text = decision.get("text") or event["text"]
            if not isinstance(text, str):
                raise ValueError("Router 需要为任务提供消息正文")
            targets.setdefault(task_id, []).append(text)
        if len(targets) > 1:
            if event["watch"]:
                raise _AmbiguousAttachments("这个目录要和哪一项任务关联？请单独发那一项，并保留目录选择。")
            if event["files"]:
                raise _AmbiguousAttachments("这些文件各自属于哪项任务？请把每项要求和对应文件一起发来。")
        for task_id, parts in targets.items():
            self._deliver(task_id, event["id"], "\n\n".join(dict.fromkeys(parts)), event["watch"])
        if len(targets) == 1:
            self.db.execute("UPDATE messages SET task_id = ? WHERE id = ?",
                            (next(iter(targets)), event["id"]))

    @staticmethod
    def _nonempty(record, field):
        value = record.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"缺少必填字段 {field}")
        return value.strip()

    def _deliver(self, task_id, event_id, text, watch):
        self.db.execute("""
            INSERT INTO deliveries (task_id, event_id, text) VALUES (?, ?, ?)
            ON CONFLICT(task_id, event_id) DO NOTHING
        """, (task_id, event_id, text))
        if watch:
            self.db.execute("INSERT OR IGNORE INTO subscriptions (task_id, thread) VALUES (?, ?)",
                            (task_id, watch))
        self.db.execute("""
            UPDATE tasks SET status = 'queued', question = '', updated = ?
            WHERE id = ? AND status != 'running'
        """, (time.time(), task_id))

    def _claim(self):
        with self.db:
            row = self.db.execute("""
                SELECT id, title, goal, session_id, summary FROM tasks
                WHERE status = 'queued' ORDER BY updated ASC, id LIMIT 1
            """).fetchone()
            if not row:
                return None
            task_id = row["id"]
            self.db.execute("UPDATE tasks SET status = 'running', updated = ? WHERE id = ?",
                            (time.time(), task_id))
            rows = self.db.execute("""
                SELECT e.id, e.source, e.thread, e.reply_to, e.watch, e.files, d.text
                FROM deliveries AS d JOIN events AS e ON e.id = d.event_id
                WHERE d.task_id = ? AND d.done = 0 ORDER BY e.seq
            """, (task_id,)).fetchall()
            return {**dict(row), "workspace": self.state_dir / "jobs" / task_id,
                    "inputs": [self._event(item) for item in rows]}

    def _worker_loop(self):
        while True:
            with self.changed:
                if self.stopping:
                    return
                job = self._claim()
                if not job:
                    self.changed.wait()
                    continue
            try:
                job["workspace"].mkdir(parents=True, exist_ok=True)
                report = self._report(job, self.execute(job))
            except Exception as error:
                report = {"status": "failed", "summary": str(error), "question": "",
                          "files": [], "session_id": job["session_id"]}
            with self.changed, self.db:
                self._complete(job, report)
                self.changed.notify_all()

    def _report(self, job, report):
        status = report.get("status")
        if status not in ("ready", "waiting"):
            raise ValueError("Worker 需要返回 ready 或 waiting")
        summary = self._nonempty(report, "summary")
        question = report.get("question", "")
        if status == "waiting" and not question:
            question = summary
        if not isinstance(question, str):
            raise ValueError("Worker 的问题需要是文字")
        files = report.get("files", [])
        if not isinstance(files, list):
            raise ValueError("Worker 的交付文件需要是路径列表")
        workspace = job["workspace"].resolve()
        deliverables = []
        for name in files:
            if not isinstance(name, str) or Path(name).is_absolute():
                raise ValueError("交付文件需要使用相对于工作目录的路径")
            resolved = (workspace / name).resolve()
            if not resolved.is_relative_to(workspace) or not resolved.is_file():
                raise ValueError(f"未找到工作目录内的交付文件 {name}")
            deliverables.append(resolved.relative_to(workspace).as_posix())
        session_id = report.get("session_id", job["session_id"])
        if session_id is not None and not isinstance(session_id, str):
            raise ValueError("Worker 会话标识需要是文字")
        return {"status": status, "summary": summary, "question": question,
                "files": list(dict.fromkeys(deliverables)), "session_id": session_id}

    def _complete(self, job, report):
        task_id = job["id"]
        status = report["status"]
        batch = {event["id"] for event in job["inputs"]}
        if status != "failed":
            self.db.executemany("UPDATE deliveries SET done = 1 WHERE task_id = ? AND event_id = ?",
                                ((task_id, event["id"]) for event in job["inputs"]))
        pending = self.db.execute("SELECT event_id FROM deliveries WHERE task_id = ? AND done = 0",
                                  (task_id,))
        if any(row["event_id"] not in batch for row in pending):
            self.db.execute("""
                UPDATE tasks SET status = 'queued', session_id = ?, updated = ? WHERE id = ?
            """, (report["session_id"], time.time(), task_id))
            return
        self.db.execute("""
            UPDATE tasks SET status = ?, summary = ?, question = ?, files = ?, session_id = ?, updated = ?
            WHERE id = ?
        """, (status, report["summary"], report["question"], json.dumps(report["files"]),
              report["session_id"], time.time(), task_id))
        text = report["summary"]
        if status == "failed":
            text = f"执行失败：{text}"
        elif report["question"] and report["question"] != text:
            text = f"{text}\n\n{report['question']}"
        self._post("assistant", text, task_id, report["files"], source="worker")
