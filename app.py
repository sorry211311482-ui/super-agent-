"""Local HTTP entry point for the SuperAgent workspace."""

import argparse
import base64
import binascii
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
import os
from pathlib import Path
import re
import shutil
from urllib.parse import quote, unquote, urlsplit


WEB = Path(__file__).resolve().parent / "web"
STATIC = {"/": "index.html", "/app.css": "app.css", "/app.js": "app.js", "/graph.js": "graph.js"}
MAX_ATTACHMENTS = 12
MAX_UPLOAD_BYTES = 15 * 1024 * 1024
MAX_REQUEST_BYTES = (MAX_UPLOAD_BYTES * 4 // 3) + 256 * 1024
DOWNLOAD = re.compile(r"^/api/tasks/([a-fA-F0-9-]{8,64})/files/(.+)$")
TASK_ACTION = re.compile(r"^/api/tasks/([a-fA-F0-9-]{8,64})/(activity|retry)$")


class RequestError(ValueError):
    def __init__(self, message, status=HTTPStatus.BAD_REQUEST):
        super().__init__(message)
        self.status = status


def build_server(orchestrator, inbox, state_dir, port, activity=None):
    state_dir = Path(state_dir).resolve()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                self._check_same_origin()
                path = urlsplit(self.path).path
                if path in STATIC:
                    file = WEB / STATIC[path]
                    self._file(file, mimetypes.guess_type(file)[0] + "; charset=utf-8")
                elif path == "/api/state":
                    self._json(HTTPStatus.OK, orchestrator.snapshot())
                elif path == "/api/config":
                    self._json(HTTPStatus.OK, {"inbox": str(inbox.root), "folders": inbox.folders()})
                elif path == "/api/policies":
                    self._json(HTTPStatus.OK, {"policies": orchestrator.policies()})
                elif (match := TASK_ACTION.fullmatch(path)) and match.group(2) == "activity":
                    task_id = match.group(1)
                    if not self._task(task_id):
                        raise RequestError("找不到这项任务", HTTPStatus.NOT_FOUND)
                    self._json(HTTPStatus.OK, {"activity": activity.read(task_id) if activity else []})
                elif match := DOWNLOAD.fullmatch(path):
                    task_id, name = match.groups()
                    self._download(task_id, unquote(name))
                else:
                    raise RequestError("找不到该页面或文件", HTTPStatus.NOT_FOUND)
            except RequestError as exc:
                self._json(exc.status, {"error": str(exc)})
            except OSError:
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "读取本地文件失败"})

        def do_POST(self):
            try:
                self._check_same_origin()
                path = urlsplit(self.path).path
                if path == "/api/messages":
                    self._json(HTTPStatus.ACCEPTED, {"event_id": self._submit(self._read_json())})
                elif path == "/api/policies":
                    config = self._read_json()
                    goal = config.get("goal")
                    if not isinstance(goal, str) or len(goal) > 4000:
                        raise RequestError("授权目标最多 4,000 字符")
                    policy = orchestrator.save_policy(config.get("source"), goal, config.get("mode"))
                    self._json(HTTPStatus.OK, {"policy": policy})
                elif path == "/api/events":
                    self._json(HTTPStatus.ACCEPTED, {"event_id": self._external(self._read_json())})
                elif (match := TASK_ACTION.fullmatch(path)) and match.group(2) == "retry":
                    self._read_json()
                    orchestrator.retry(match.group(1))
                    self._json(HTTPStatus.ACCEPTED, {"task_id": match.group(1)})
                else:
                    raise RequestError("找不到该操作", HTTPStatus.NOT_FOUND)
            except RequestError as exc:
                self._json(exc.status, {"error": str(exc)})
            except ValueError as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            except OSError:
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "无法保存消息或附件"})

        def _check_same_origin(self):
            port = self.server.server_port
            host = self.headers.get("Host", "")
            if host not in {f"127.0.0.1:{port}", f"localhost:{port}"}:
                raise RequestError("请用本地服务地址打开 SuperAgent", HTTPStatus.FORBIDDEN)
            origin = self.headers.get("Origin")
            if origin is not None and origin != f"http://{host}":
                raise RequestError("操作需要来自当前 SuperAgent 页面", HTTPStatus.FORBIDDEN)

        def _read_json(self):
            if self.headers.get_content_type() != "application/json":
                raise RequestError("需要 JSON 请求", HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1:
                    raise ValueError()
            except ValueError:
                raise RequestError("请求内容为空或无效")
            if length > MAX_REQUEST_BYTES:
                raise RequestError("附件合计最多 15 MB", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            try:
                data = json.loads(self.rfile.read(length))
            except (UnicodeError, ValueError):
                raise RequestError("无法读取消息内容")
            if not isinstance(data, dict):
                raise RequestError("消息格式无效")
            return data

        def _submit(self, data):
            text = data.get("text", "")
            watch = data.get("watch")
            reply_to = data.get("reply_to")
            attachments = data.get("attachments", [])
            request_id = data.get("request_id")
            text = self._text(text)
            if watch is not None and (not isinstance(watch, str) or not watch):
                raise RequestError("请选择一个收件目录")
            if reply_to is not None and (not isinstance(reply_to, str) or not reply_to):
                raise RequestError("回复对应的任务无效")
            if request_id is not None and (not isinstance(request_id, str) or not re.fullmatch(
                r"[a-zA-Z0-9-]{8,64}", request_id
            )):
                raise RequestError("消息编号无效")
            if not text and not attachments:
                raise RequestError("请输入任务要求或附上文件")
            if reply_to and not self._task(reply_to):
                raise RequestError("回复对应的任务已不存在", HTTPStatus.NOT_FOUND)
            watched_dir = inbox.prepare_watch(watch) if watch else None
            files = self._attachments(attachments)
            if watched_dir:
                files.extend(inbox.include_existing(watched_dir))
            message = text or "已补充附件，请查看。"
            return orchestrator.submit(message, files=files, reply_to=reply_to, watch=watched_dir,
                                       source_id=request_id)

        def _external(self, data):
            source, source_id, thread = (data.get(key) for key in ("source", "source_id", "thread"))
            if not all(isinstance(value, str) and value.strip() for value in (source, source_id, thread)):
                raise RequestError("需要来源 ID、事件 ID 和消息串 ID")
            text = self._text(data.get("text", ""))
            attachments = data.get("attachments", [])
            if not text and not attachments:
                raise RequestError("请输入事件内容或附上文件")
            return orchestrator.submit_external(source.strip(), source_id.strip(), thread.strip(),
                                                text or "收到附件。", self._attachments(attachments))

        @staticmethod
        def _text(value):
            if not isinstance(value, str) or len(value) > 16000:
                raise RequestError("消息最多 16,000 字符")
            return value.strip()

        @staticmethod
        def _attachments(attachments):
            if not isinstance(attachments, list) or len(attachments) > MAX_ATTACHMENTS:
                raise RequestError("每条消息最多附上 12 个文件")
            decoded = []
            uploaded_bytes = 0
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    raise RequestError("附件格式无效")
                name, content = attachment.get("name"), attachment.get("data")
                if not isinstance(name, str) or not isinstance(content, str):
                    raise RequestError("附件格式无效")
                try:
                    payload = base64.b64decode(content, validate=True)
                except (ValueError, binascii.Error):
                    raise RequestError("无法读取附件内容")
                uploaded_bytes += len(payload)
                if uploaded_bytes > MAX_UPLOAD_BYTES:
                    raise RequestError("附件合计最多 15 MB", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                decoded.append((name, payload))
            return [inbox.store_upload(name, payload) for name, payload in decoded]

        @staticmethod
        def _task(task_id):
            return next((item for item in orchestrator.snapshot()["tasks"] if item["id"] == task_id), None)

        def _download(self, task_id, name):
            task = self._task(task_id)
            if not task or name not in task["files"]:
                raise RequestError("找不到这个交付文件", HTTPStatus.NOT_FOUND)
            path = Path(name)
            jobs = state_dir / "jobs"
            root = jobs / task_id
            file = root / path
            if (
                path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != "outputs"
                or jobs.is_symlink() or root.is_symlink() or (root / "outputs").is_symlink()
                or not file.resolve().is_relative_to(root / "outputs")
                or file.is_symlink() or not file.is_file()
            ):
                raise RequestError("找不到这个交付文件", HTTPStatus.NOT_FOUND)
            self._file(file, "application/octet-stream", attachment=True)

        def _headers(self, status, mime, length):
            self.send_response(status)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(length))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'self'; "
                             "style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; "
                             "frame-ancestors 'none'; form-action 'self'")

        def _json(self, status, payload):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self._headers(status, "application/json; charset=utf-8", len(body))
            self.end_headers()
            self.wfile.write(body)

        def _file(self, file, mime, attachment=False):
            with file.open("rb") as stream:
                self._headers(HTTPStatus.OK, mime, os.fstat(stream.fileno()).st_size)
                if attachment:
                    self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{quote(file.name)}")
                self.end_headers()
                shutil.copyfileobj(stream, self.wfile)

        def log_request(self, code="-", size="-"):
            if str(code).startswith(("4", "5")):
                self.log_message("%s %s", self.command, code)

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def main():
    from activity import ActivityLog
    from executor import CodexAgent
    from inbox import Inbox
    from orchestrator import Orchestrator

    parser = argparse.ArgumentParser(description="启动本地 SuperAgent")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--inbox", type=Path, default=Path("inbox"))
    parser.add_argument("--state-dir", type=Path, default=Path(".superagent"))
    parser.add_argument("--slots", type=int, default=2, choices=range(1, 9), metavar="1–8")
    args = parser.parse_args()
    state_dir = args.state_dir.resolve()
    activity = ActivityLog(state_dir)
    agent = CodexAgent(on_activity=activity.record)
    orchestrator = Orchestrator(state_dir, agent.assess, agent.execute, slots=args.slots)
    inbox = Inbox(
        args.inbox.resolve(), state_dir, orchestrator.submit,
        subscriptions=orchestrator.watch_directories
    )
    with build_server(orchestrator, inbox, state_dir, args.port, activity=activity) as server:
        try:
            orchestrator.start()
            inbox.start()
            print(f"SuperAgent  http://127.0.0.1:{server.server_port}\n收件目录  {inbox.root}", flush=True)
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            inbox.close()
            orchestrator.close()


if __name__ == "__main__":
    main()
