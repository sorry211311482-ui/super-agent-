import tempfile
import threading
import time
import unittest
from pathlib import Path

from orchestrator import Orchestrator


class ExternalEventTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = Path(directory.name)
        self.candidates = []
        self.jobs = []

    def service(self, assess=None, *, start=True):
        def execute(job):
            self.jobs.append(job)
            return {"status": "ready", "summary": job["inputs"][-1]["text"], "session_id": "worker"}

        service = Orchestrator(self.state, assess or self.assess, execute)
        self.addCleanup(service.close)
        if start:
            service.start()
        return service

    def assess(self, event, tasks):
        self.candidates.append((event, tasks))
        if event["text"] == "无关事件":
            return [{"action": "ignore"}]
        return [{"action": "new", "title": "整理报价", "goal": "不在授权范围的目标", "text": "替换原文"}]

    def wait_for(self, condition):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            found = condition()
            if found:
                return found
            time.sleep(0.01)
        self.fail("等待外部事件超时")

    def wait_event(self, service, event_id, decision):
        return self.wait_for(lambda: next((
            event for event in service.snapshot()["events"]
            if (event["id"], event["decision"]) == (event_id, decision)
        ), None))

    def wait_task(self, service, task_id, summary):
        return self.wait_for(lambda: next((
            task for task in service.snapshot()["tasks"]
            if (task["id"], task["status"], task["summary"]) == (task_id, "ready", summary)
        ), None))

    def test_default_off_and_reserved_sources(self):
        service = self.service()
        unknown = service.submit_external(" Quotes ", "one", "order-1", "报价")
        self.assertEqual(service.save_policy("quotes", "整理报价", "off"), {
            "source": "quotes", "goal": "整理报价", "mode": "off",
        })
        disabled = service.submit_external("quotes", "two", "order-1", "补充报价")
        self.assertEqual(unknown, service.submit_external("QUOTES", "one", "order-1", "重复投递"))
        self.wait_event(service, unknown, "ignored")
        self.wait_event(service, disabled, "ignored")
        self.assertEqual(service.snapshot()["tasks"], [])
        self.assertEqual(self.candidates, [])
        with self.assertRaisesRegex(ValueError, "外部来源名称"):
            service.submit_external("chat", "one", "order-1", "消息")
        with self.assertRaisesRegex(ValueError, "外部来源名称"):
            service.save_policy("directory", "整理报价", "auto")
        with self.assertRaisesRegex(ValueError, "256"):
            service.submit_external("quotes", "x" * 257, "order-1", "新报价")

    def test_observe_then_auto_and_followup(self):
        service = self.service()
        goal = "比较 P27 报价并出具文件"
        service.save_policy("quotes", goal, "observe")
        original = service.submit_external("quotes", "one", "order-1", "新报价")
        self.wait_event(service, original, "observed")
        ignored = service.submit_external("quotes", "two", "order-2", "无关事件")
        self.wait_event(service, ignored, "ignored")
        self.assertEqual(service.snapshot()["tasks"], [])

        service.save_policy("quotes", goal, "auto")
        initial = service.submit_external("quotes", "three", "order-1", "有可核验报价")
        accepted = self.wait_event(service, initial, "accepted")
        task_id = accepted["task_id"]
        task = self.wait_task(service, task_id, "有可核验报价")
        self.assertEqual(task["goal"], goal)
        self.assertEqual(self.candidates[-1][0]["policy"], {
            "source": "quotes", "goal": goal, "mode": "auto",
        })
        self.assertTrue(all(not tasks and "recent" not in event for event, tasks in self.candidates))
        self.assertIsNone(self.candidates[-1][0]["watch"])

        supplement = service.submit_external("quotes", "four", "order-1", "补充运费 180 元")
        self.assertEqual(self.wait_event(service, supplement, "delivered")["task_id"], task_id)
        self.wait_task(service, task_id, "补充运费 180 元")
        self.assertEqual(supplement, service.submit_external("quotes", "four", "order-1", "重复投递"))
        self.assertEqual(len(self.candidates), 3)
        self.assertEqual(len(self.jobs), 2)
        self.assertEqual(self.jobs[-1]["session_id"], "worker")
        self.assertEqual(service.watch_directories(), [])

        foreign = service.submit_external("other-source", "four", "order-1", "同名消息串")
        self.assertIsNone(self.wait_event(service, foreign, "ignored")["task_id"])

        revised_goal = "只汇总已审核供应商的报价"
        service.save_policy("quotes", revised_goal, "auto")
        next_event = service.submit_external("quotes", "five", "order-1", "范围变化")
        next_task_id = self.wait_event(service, next_event, "accepted")["task_id"]
        self.assertNotEqual(next_task_id, task_id)
        self.assertEqual(self.wait_task(service, next_task_id, "范围变化")["goal"], revised_goal)

    def test_rule_change_during_assessment_uses_current_mode(self):
        evaluating = threading.Event()
        resume = threading.Event()

        def assess(event, tasks):
            evaluating.set()
            if not resume.wait(3):
                raise TimeoutError()
            return self.assess(event, tasks)

        service = self.service(assess)
        service.save_policy("quotes", "整理报价", "auto")
        event_id = service.submit_external("quotes", "one", "order-1", "新报价")
        try:
            self.assertTrue(evaluating.wait(3))
            service.save_policy("quotes", "整理报价", "off")
        finally:
            resume.set()
        self.wait_event(service, event_id, "ignored")
        self.assertEqual(service.snapshot()["tasks"], [])
        self.assertEqual(self.jobs, [])

    def test_observed_event_cannot_gain_execution_on_policy_change(self):
        evaluating = threading.Event()
        resume = threading.Event()

        def assess(event, tasks):
            evaluating.set()
            if not resume.wait(3):
                raise TimeoutError()
            return self.assess(event, tasks)

        service = self.service(assess)
        service.save_policy("quotes", "整理报价", "observe")
        event_id = service.submit_external("quotes", "one", "order-1", "新报价")
        try:
            self.assertTrue(evaluating.wait(3))
            service.save_policy("quotes", "整理报价", "auto")
        finally:
            resume.set()
        self.wait_event(service, event_id, "observed")
        self.assertEqual([event["policy"]["mode"] for event, _ in self.candidates],
                         ["observe", "observe"])
        self.assertEqual(service.snapshot()["tasks"], [])
        self.assertEqual(self.jobs, [])

    def test_fast_path_preserves_order_behind_a_pending_event(self):
        service = self.service()
        goal = "整理报价"
        service.save_policy("quotes", goal, "auto")
        original = service.submit_external("quotes", "one", "order-1", "初始报价")
        task_id = self.wait_event(service, original, "accepted")["task_id"]
        self.wait_task(service, task_id, "初始报价")
        service.close()

        service = self.service(start=False)
        service.save_policy("quotes", "整理其他报价", "auto")
        earlier = service.submit_external("quotes", "two", "order-1", "第一次更新")
        service.save_policy("quotes", goal, "auto")
        later = service.submit_external("quotes", "three", "order-1", "第二次更新")
        self.wait_event(service, earlier, "pending")
        self.wait_event(service, later, "pending")
        service.start()
        self.wait_task(service, task_id, "第二次更新")
        self.assertEqual([event["id"] for job in self.jobs[1:] for event in job["inputs"]],
                         [earlier, later])
        self.assertEqual(len(self.candidates), 1)
