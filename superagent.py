"""本地任务入口。Router 和 Worker 只通过 Engine 收发消息。"""
import argparse
import hashlib
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from engine import Engine
from workers import checklist, deepseek_route, deepseek_worker, local_route


def show(result):
    ticket = result["ticket_id"]
    owner = f" · {result['worker']} · 任务 {ticket}" if ticket else ""
    print(f"[{result['thread']} · {result['status']}{owner}]")
    if result["response"]:
        print(result["response"])


def scan_notes(engine, folder):
    """把目录里每次保存的 Markdown 当成事件，同一文件的后续版本沿用原任务。"""
    folder = folder.resolve(strict=True)
    if not folder.is_dir():
        raise ValueError(f"需要会议记录目录：{folder}")
    for path in sorted(folder.glob("*.md")):
        if not path.is_file() or path.is_symlink():
            continue
        # 从同一个打开的文件读取版本和内容，能够区分后来改回旧内容的版本。
        with path.open(encoding="utf-8") as source:
            version = os.fstat(source.fileno()).st_mtime_ns
            body = source.read()
        fingerprint = hashlib.sha256(body.encode("utf-8")).hexdigest()
        thread = f"file:{path}"
        engine.receive(thread, "收到一版会议记录", {"name": path.name, "text": body},
                       event_id=f"{thread}:{version}:{fingerprint}")


def demo():
    thread = "周会"
    notes = {"name": "周会.md", "text": (
        "- [ ] 小林核对订单数量\n"
        "- [x] 确认会议时间\n"
        "- [ ] 小周更新发货日期"
    )}
    # 每轮都重新打开数据库，和分三次从终端运行效果相同。
    with TemporaryDirectory() as directory:
        for prompt, attachment in [
            ("请提取会议待办", None),
            ("这是今天的会议记录", notes),
            ("只看 小周", None),
        ]:
            print(f"\n你：{prompt}")
            if attachment:
                print(attachment["text"])
            engine = Engine(Path(directory) / "demo.sqlite3")
            try:
                engine.receive(thread, prompt, attachment)
                for result in engine.run(local_route, {"checklist": checklist}):
                    show(result)
                    if result["status"] == "failed":
                        return 1
            finally:
                engine.close()
    return 0


def main():
    parser = argparse.ArgumentParser(description="向本地 SuperAgent 发任务或追加要求")
    parser.add_argument("--backend", choices=["local", "deepseek"], default="local",
                        help="local 离线运行；deepseek 会将当前任务内容发给 DeepSeek API")
    parser.add_argument("--state-dir", type=Path, default=Path(".superagent"),
                        help="会话数据库目录，默认在当前目录下")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="离线演示一个任务的三个回合")
    say = commands.add_parser("say", help="向指定对话发送消息")
    say.add_argument("thread", help="对话名，追问时沿用同一个名字")
    say.add_argument("text", help="任务或补充要求")
    say.add_argument("--file", type=Path, help="附上一份 UTF-8 文本或 Markdown 文件")
    say.add_argument("--event-id", help="外部事件标识，重发同一事件时复用")
    scan = commands.add_parser("scan", help="扫描指定目录中的 Markdown，离线提取会议待办")
    scan.add_argument("folder", type=Path)
    history = commands.add_parser("history", help="查看对话中的输入和回复")
    history.add_argument("thread")
    commands.add_parser("run", help="继续处理已入队的消息")
    args = parser.parse_args()
    if args.command == "demo":
        return demo()
    if args.command == "scan" and args.backend != "local":
        parser.error("scan 是离线待办来源，请使用默认的 local 后端")

    attachment = None
    if args.command == "say" and args.file:
        attachment = {"name": args.file.name, "text": args.file.read_text(encoding="utf-8")}
    args.state_dir.mkdir(parents=True, exist_ok=True)
    engine = Engine(args.state_dir / f"{args.backend}.sqlite3")
    try:
        if args.command == "history":
            messages = engine.history(args.thread)
            for message in messages:
                label = "你" if message["role"] == "user" else "Worker"
                print(f"{label}：{message['content']}")
                if message.get("attachment"):
                    print(f"  附件 {message['attachment']['name']}")
            if not messages:
                print("这个对话还没有任务记录。")
            return 0

        event = None
        if args.command == "say":
            event = engine.receive(args.thread, args.text, attachment, args.event_id)
        elif args.command == "scan":
            scan_notes(engine, args.folder)
        if args.backend == "local":
            results = engine.run(local_route, {"checklist": checklist})
        else:
            results = engine.run(deepseek_route, {"deepseek": deepseek_worker})
        if event and all(result["event_id"] != event for result in results):
            previous = engine.result(event)
            if previous:
                results.append(previous)
        for result in results:
            show(result)
        if not results:
            print("没有待处理消息。")
        return int(any(result["status"] == "failed" for result in results))
    finally:
        engine.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as error:
        sys.exit(str(error))
