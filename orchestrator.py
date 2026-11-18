"""Persistent event routing and isolated task scheduling."""

import fcntl
import json
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path


class _AmbiguousAttachments(ValueError):
    pass


FLOW_HISTORY_PER_KIND = 16


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
            CREATE TABLE IF NOT EXISTS source_policies (
                source TEXT PRIMARY KEY,
                goal TEXT NOT NULL,
                mode TEXT NOT NULL CHECK(mode IN ('off', 'observe', 'auto')),
                version INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS external_events (
                event_id TEXT PRIMARY KEY REFERENCES events(id),
                decision TEXT NOT NULL DEFAULT 'pending',
                task_id TEXT REFERENCES tasks(id),
                allow_auto INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS external_threads (
                source TEXT NOT NULL REFERENCES source_policies(source),
                thread TEXT NOT NULL,
                task_id TEXT NOT NULL REFERENCES tasks(id),
                PRIMARY KEY(source, thread)
            );
            CREATE INDEX IF NOT EXISTS pending_events ON events(seq) WHERE routed = 0;
            CREATE INDEX IF NOT EXISTS pending_deliveries ON deliveries(task_id) WHERE done = 0;
            CREATE INDEX IF NOT EXISTS subscriptions_by_thread ON subscriptions(thread);
            CREATE INDEX IF NOT EXISTS worker_returns ON messages(seq)
                WHERE role = 'assistant' AND source = 'worker' AND task_id IS NOT NULL;
        """)
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            columns = {row["name"] for row in self.db.execute("PRAGMA table_info(external_events)")}
            if "allow_auto" not in columns:
                self.db.execute(
                    "ALTER TABLE external_events ADD COLUMN allow_auto INTEGER NOT NULL DEFAULT 0"
                )

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

    @staticmethod
    def _external_source(source):
        normalized = source.strip().lower() if isinstance(source, str) else ""
        if (not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", normalized)
                or normalized in ("chat", "directory")):
            raise ValueError("外部来源名称无效")
        return normalized

    def policies(self):
        with self.changed:
            rows = self.db.execute("SELECT source, goal, mode FROM source_policies ORDER BY source")
            return [dict(row) for row in rows]

    def _policy(self, source):
        row = self.db.execute(
            "SELECT source, goal, mode, version FROM source_policies WHERE source = ?", (source,)
        ).fetchone()
        return dict(row) if row else None

    def save_policy(self, source, goal, mode):
        source = self._external_source(source)
        goal = self._nonempty({"goal": goal}, "goal")
        if mode not in ("off", "observe", "auto"):
            raise ValueError("规则模式必须为 off、observe 或 auto")
        with self.changed, self.db:
            if self.stopping:
                raise RuntimeError("服务已经关闭")
            self.db.execute("""
                INSERT INTO source_policies (source, goal, mode) VALUES (?, ?, ?)
                ON CONFLICT(source) DO UPDATE SET goal = excluded.goal, mode = excluded.mode,
                    version = source_policies.version + 1
                WHERE source_policies.goal != excluded.goal OR source_policies.mode != excluded.mode
            """, (source, goal, mode))
        return {"source": source, "goal": goal, "mode": mode}

    def submit_external(self, source, source_id, thread, text, files=()):
        source = self._external_source(source)
        source_id = self._nonempty({"source_id": source_id}, "source_id")
        thread = self._nonempty({"thread": thread}, "thread")
        if len(source_id) > 256 or len(thread) > 256:
            raise ValueError("来源事件编号和消息串最多 256 个字符")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("消息正文不能为空")
        event_id = uuid.uuid4().hex
        paths = [str(Path(path).expanduser().resolve()) for path in files]
        with self.changed, self.db:
            if self.stopping:
                raise RuntimeError("服务已经关闭")
            existing = self.db.execute(
                "SELECT id FROM events WHERE source = ? AND source_id = ?", (source, source_id)
            ).fetchone()
            if existing:
                return existing["id"]
            self.db.execute("""
                INSERT INTO events (id, source, source_id, thread, text, files)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (event_id, source, source_id, thread, text, json.dumps(paths)))
            policy = self._policy(source)
            allow_auto = bool(policy and policy["mode"] == "auto")
            self.db.execute("INSERT INTO external_events (event_id, allow_auto) VALUES (?, ?)",
                            (event_id, allow_auto))
            self._prepare_external({
                "id": event_id, "source": source, "thread": thread, "text": text, "files": paths,
            }, allow_auto)
            self.changed.notify_all()
        return event_id

    def _finish_external(self, event_id, decision, task_id=None):
        self.db.execute("UPDATE external_events SET decision = ?, task_id = ? WHERE event_id = ?",
                        (decision, task_id, event_id))
        self.db.execute("UPDATE events SET routed = 1 WHERE id = ?", (event_id,))

    def _deliver_external(self, event, task_id, decision):
        self._deliver(task_id, event["id"], event["text"], None)
        self._post("user", event["text"], task_id, [Path(path).name for path in event["files"]],
                   source=event["source"], message_id=event["id"])
        self._finish_external(event["id"], decision, task_id)

    def _prepare_external(self, event, allow_auto):
        """Apply the programmatic gate and subscription before using the Router."""
        policy = self._policy(event["source"])
        if not policy or policy["mode"] == "off":
            self._finish_external(event["id"], "ignored")
            return None
        if policy["mode"] == "auto" and allow_auto:
            target = self.db.execute("""
                SELECT task.id FROM external_threads AS thread
                JOIN tasks AS task ON task.id = thread.task_id
                WHERE thread.source = ? AND thread.thread = ? AND task.goal = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM events AS earlier
                        WHERE earlier.source = thread.source AND earlier.thread = thread.thread
                            AND earlier.routed = 0
                            AND earlier.seq < (SELECT seq FROM events WHERE id = ?)
                    )
            """, (event["source"], event["thread"], policy["goal"], event["id"])).fetchone()
            if target:
                self._deliver_external(event, target["id"], "delivered")
                return None
        return policy

    def watch_directories(self):
        with self.changed:
            rows = self.db.execute("SELECT DISTINCT thread FROM subscriptions ORDER BY thread")
            return [row["thread"] for row in rows]

    def retry(self, task_id):
        """Queue the existing inputs again after a worker failure."""
        with self.changed, self.db:
            if self.stopping:
                raise RuntimeError("服务已经关闭")
            task = self.db.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if task is None:
                raise ValueError("找不到需要重试的任务")
            if task["status"] != "failed":
                raise ValueError("只有失败的任务可以重试")
            pending = self.db.execute(
                "SELECT 1 FROM deliveries WHERE task_id = ? AND done = 0 LIMIT 1", (task_id,)
            ).fetchone()
            if pending is None:
                raise ValueError("这个任务没有可重试的输入")
            self.db.execute(
                "UPDATE tasks SET status = 'queued', question = '', updated = ? WHERE id = ?",
                (time.time(), task_id),
            )
            self.changed.notify_all()

    def snapshot(self):
        with self.changed:
            events = self.db.execute("""
                SELECT event.id, event.source, event.source_id, event.thread, external.decision, external.task_id
                FROM external_events AS external JOIN events AS event ON event.id = external.event_id
                ORDER BY event.seq DESC LIMIT 50
            """).fetchall()
            return {"tasks": self._tasks(), "messages": self._messages(),
                    "events": [dict(row) for row in reversed(events)],
                    "flow": {"slots": self.slots, "packets": self._flow_packets()}}

    @staticmethod
    def _flow_route(row):
        if row["reply_to"] or row["source"] == "directory" or row["decision"] == "delivered":
            return "direct"
        return "router"

    def _flow_packets(self):
        """Recent inputs, deliveries, and worker replies in each stream's persisted order."""
        inputs = self.db.execute("""
            SELECT event.id, event.source, event.reply_to, event.routed, external.decision,
                COALESCE(external.task_id, message.task_id) AS task_id
            FROM events AS event
            LEFT JOIN external_events AS external ON external.event_id = event.id
            LEFT JOIN messages AS message ON message.id = event.id
            ORDER BY event.seq DESC LIMIT ?
        """, (FLOW_HISTORY_PER_KIND,)).fetchall()
        deliveries = self.db.execute("""
            SELECT delivery.event_id, delivery.task_id, task.title,
                event.source, event.reply_to, external.decision
            FROM deliveries AS delivery
            JOIN events AS event ON event.id = delivery.event_id
            JOIN tasks AS task ON task.id = delivery.task_id
            LEFT JOIN external_events AS external ON external.event_id = event.id
            ORDER BY delivery.rowid DESC LIMIT ?
        """, (FLOW_HISTORY_PER_KIND,)).fetchall()
        returns = self.db.execute("""
            SELECT message.id, message.task_id, task.title
            FROM messages AS message JOIN tasks AS task ON task.id = message.task_id
            WHERE message.role = 'assistant' AND message.source = 'worker'
                AND message.task_id IS NOT NULL
            ORDER BY message.seq DESC LIMIT ?
        """, (FLOW_HISTORY_PER_KIND,)).fetchall()

        packets = []
        source_labels = {"chat": "对话输入", "directory": "目录更新"}
        for row in reversed(inputs):
            label = source_labels.get(row["source"], row["source"] if row["decision"] else "来源事件")
            packets.append({
                "id": f"input:{row['id']}", "kind": "input", "event_id": row["id"],
                "task_id": row["task_id"], "via": self._flow_route(row), "label": label,
                "status": row["decision"] or ("routed" if row["routed"] else "pending"),
            })
        for row in reversed(deliveries):
            packets.append({
                "id": f"dispatch:{row['event_id']}:{row['task_id']}", "kind": "dispatch",
                "event_id": row["event_id"], "task_id": row["task_id"],
                "via": self._flow_route(row), "label": row["title"][:100],
            })
        for row in reversed(returns):
            packets.append({
                "id": f"return:{row['id']}", "kind": "return",
                "task_id": row["task_id"], "label": row["title"][:100],
            })
        return packets

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
                row = self.db.execute("""
                    SELECT event.*, external.event_id IS NOT NULL AS is_external, external.allow_auto
                    FROM events AS event LEFT JOIN external_events AS external ON external.event_id = event.id
                    WHERE event.routed = 0 ORDER BY event.seq LIMIT 1
                """).fetchone()
                if row is None:
                    self.changed.wait()
                    continue
                event = self._event(row)
                external = bool(row["is_external"])
                if external:
                    event["source_id"] = row["source_id"]
                else:
                    event["recent"] = self._messages(limit=16, before=event["id"])
                    event["reply_context"] = self._reply_context(event["id"])
                    tasks = self._tasks(limit=40)
            if external:
                self._route_external(event, bool(row["allow_auto"]))
                continue
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

    def _route_external(self, event, allow_auto):
        with self.changed, self.db:
            policy = self._prepare_external(event, allow_auto)
            self.changed.notify_all()
        if policy is None:
            return

        mode = policy["mode"] if allow_auto else "observe"
        candidate = {**event, "policy": {"source": policy["source"], "goal": policy["goal"], "mode": mode}}
        try:
            title = self._external_title(self.assess(candidate, []))
            decision = "observed" if title else "ignored"
        except Exception:
            title, decision = None, "failed"

        with self.changed, self.db:
            if self._policy(event["source"]) != policy:
                # The event remains pending and will be gated again by the new rule.
                return
            if title and mode == "auto":
                task_id = uuid.uuid4().hex
                self.db.execute("INSERT INTO tasks (id, title, goal, updated) VALUES (?, ?, ?, ?)",
                                (task_id, title, policy["goal"], time.time()))
                self.db.execute("""
                    INSERT INTO external_threads (source, thread, task_id) VALUES (?, ?, ?)
                    ON CONFLICT(source, thread) DO UPDATE SET task_id = excluded.task_id
                """, (event["source"], event["thread"], task_id))
                self._deliver_external(event, task_id, "accepted")
            else:
                self._finish_external(event["id"], decision)
            self.changed.notify_all()

    def _external_title(self, decisions):
        if not isinstance(decisions, list) or not decisions:
            raise ValueError("Router 需要返回至少一项决定")
        title = None
        for decision in decisions:
            if not isinstance(decision, dict):
                raise ValueError("Router 返回的决定格式无效")
            action = decision.get("action")
            if action == "new":
                accepted_title = self._nonempty(decision, "title")
                title = title or accepted_title
            elif action not in ("ignore", "ask"):
                raise ValueError("外部事件不能由模型关联至其他任务")
        return title

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
