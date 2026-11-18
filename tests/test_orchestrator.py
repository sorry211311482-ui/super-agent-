import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path

from inbox import Inbox
from orchestrator import Orchestrator


class OrchestratorTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.state = self.root / "state"

    def wait_for(self, condition):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            result = condition()
            if result:
                return result
            time.sleep(0.01)
        self.fail("等待异步任务超时")

    @staticmethod
    def task(service, title, status):
        return next((task for task in service.snapshot()["tasks"]
                     if task["title"] == title and task["status"] == status), None)

    def test_two_workers_and_directory_wakeup_after_restart(self):
        quote_started = threading.Event()
        finish_quote = threading.Event()
        routed = []
        executions = []

        def route(event, tasks):
            routed.append(event["text"])
            return [{"action": "new", "title": event["text"], "goal": event["text"]}]

        def execute(job):
            executions.append((job["id"], job["session_id"], job["inputs"]))
            if job["title"] == "比价" and not job["session_id"]:
                quote_started.set()
                if not finish_quote.wait(timeout=5):
                    raise TimeoutError("比价 Worker 未收到释放信号")
                return {"status": "waiting", "summary": "等 B 家运费", "session_id": "quote"}
            return {"status": "ready", "summary": "已交付",
                    "session_id": job["session_id"] or "listing"}

        service = Orchestrator(self.state, route, execute, slots=2)
        upload = self.root / "inbox" / "P27"
        upload.mkdir(parents=True)
        service.start()
        try:
            service.submit("比价", watch=upload)
            self.assertTrue(quote_started.wait(timeout=5))
            service.submit("Listing 审核")
            self.wait_for(lambda: self.task(service, "Listing 审核", "ready"))
            self.assertIsNotNone(self.task(service, "比价", "running"))
            finish_quote.set()
            original = self.wait_for(lambda: self.task(service, "比价", "waiting"))
        finally:
            finish_quote.set()
            service.close()

        service = Orchestrator(self.state, route, execute, slots=2)
        inbox = Inbox(upload.parent, self.state, service.submit,
                      subscriptions=service.watch_directories)
        service.start()
        try:
            update = upload / "B 家运费.txt"
            update.write_text("180 元", encoding="utf-8")
            inbox.scan()
            resumed = self.wait_for(lambda: self.task(service, "比价", "ready"))
            self.assertEqual(resumed["id"], original["id"])
            self.assertEqual(routed, ["比价", "Listing 审核"])
            task_id, session, inputs = executions[-1]
            self.assertEqual((task_id, session), (original["id"], "quote"))
            self.assertEqual(inputs[0]["source"], "directory")
            self.assertEqual(Path(inputs[0]["files"][0]).read_text(encoding="utf-8"), "180 元")
        finally:
            service.close()

        service = Orchestrator(self.state, route, execute, slots=2)
        inbox = Inbox(upload.parent, self.state, service.submit,
                      subscriptions=service.watch_directories)
        try:
            inbox.scan()
            inbox.scan()
            self.assertEqual(self.task(service, "比价", "ready")["id"], original["id"])
            events = [m for m in service.snapshot()["messages"] if m["source"] == "directory"]
            self.assertEqual(len(events), 1)
        finally:
            service.close()

    def test_interrupted_claim_reuses_durable_input(self):
        def route(event, tasks):
            return [{"action": "new", "title": "比价", "goal": "比价"}]

        def execute(job):
            return {"status": "ready", "summary": job["inputs"][-1]["text"],
                    "session_id": "quote"}

        service = Orchestrator(self.state, route, execute)
        service.start()
        try:
            service.submit("比价")
            task = self.wait_for(lambda: self.task(service, "比价", "ready"))
        finally:
            service.close()
        service = Orchestrator(self.state, route, execute)
        try:
            service.submit("B 家运费 180", reply_to=task["id"])
            with closing(sqlite3.connect(self.state / "tasks.sqlite3")) as database, database:
                database.execute("UPDATE tasks SET status = 'running' WHERE id = ?", (task["id"],))
            service.start()
            recovered = self.wait_for(lambda: self.task(service, "比价", "ready"))
            self.assertEqual(recovered["id"], task["id"])
            self.assertEqual(recovered["summary"], "B 家运费 180")
        finally:
            service.close()
