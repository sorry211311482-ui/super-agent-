"""Worker adapters. The engine routes messages and owns their history."""

import json
import os
import re
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


TASK = re.compile(r"^\s*(?:[-+*]|\d+[.)])[ \t]+\[([ xX])\][ \t]+(\S.*)$")
CHECKLIST_REQUEST = re.compile(
    r"(?:整理|提取|列出|查看|筛选|显示|汇总).{0,30}(?:待办|清单|未完成)"
    r"|\b(?:list|extract|show|filter|summari[sz]e)\b[^\n]{0,40}\b(?:todos?|to-dos?|tasks?|checklists?|action items?)\b",
    re.IGNORECASE,
)


def _tasks(text):
    """Yield real list items while skipping examples inside fenced code blocks."""
    fence = ""
    for line in text.splitlines():
        stripped = line.lstrip()
        if fence:
            if re.fullmatch(re.escape(fence[0]) + "{%d,}" % len(fence), stripped.rstrip()):
                fence = ""
            continue
        opening = re.match(r"(`{3,}|~{3,})", stripped)
        if opening:
            fence = opening.group(1)
            continue
        task = TASK.match(line)
        if task:
            yield task.group(1) == " ", task.group(2).strip()


def local_route(message):
    text = message["content"]
    attachment = message.get("attachment")
    if CHECKLIST_REQUEST.search(text) or any(_tasks(text)) or (attachment and any(_tasks(attachment["text"]))):
        return "checklist"
    return None


def checklist(history):
    users = [message for message in history if message["role"] == "user"]
    for message in reversed(users):
        attachment = message.get("attachment")
        if attachment:
            tasks = list(_tasks(attachment["text"]))
            break
        tasks = list(_tasks(message["content"]))
        if tasks:
            break
    else:
        return "waiting", "请粘贴 Markdown 清单，或附上包含清单的文本文件。"

    items = [text for is_open, text in tasks if is_open]
    selection = re.search(r"只看[ \t]*([^\n]+)", users[-1]["content"])
    if selection:
        keyword = selection.group(1).strip(" \t。、，,。.!！?？“”\"‘’'")
        items = [item for item in items if keyword.casefold() in item.casefold()]
    if not items:
        return "ready", "没有匹配的未完成事项。" if selection else "这份材料中没有未完成的 Markdown 清单项。"
    return "ready", "\n".join(f"- {item}" for item in items)


def _deepseek(system, history):
    key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not key:
        raise RuntimeError("调用 DeepSeek 需要设置 DEEPSEEK_API_KEY。")
    messages = [{"role": "system", "content": system}]
    for message in history:
        content = message["content"]
        attachment = message.get("attachment")
        if attachment:
            content += f"\n\n附件 {attachment['name']}\n{attachment['text']}"
        messages.append({"role": message["role"], "content": content})
    payload = {
        "model": os.environ.get("DEEPSEEK_MODEL") or "deepseek-flash",
        "messages": messages,
        "response_format": {"type": "json_object"},
        "max_tokens": 4096,
    }
    request = Request(
        "https://api.deepseek.com/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=90) as response:
            envelope = json.load(response)
        result = json.loads(envelope["choices"][0]["message"]["content"])
        if not isinstance(result, dict):
            raise ValueError("expected a JSON object")
        return result
    except HTTPError as exc:
        raise RuntimeError(f"DeepSeek API 返回 HTTP {exc.code}（{exc.reason}）。") from exc
    except (URLError, OSError) as exc:
        raise RuntimeError(f"无法完成 DeepSeek API 请求（{exc}）。") from exc
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise RuntimeError("DeepSeek API 返回了无法解析的模型结果。") from exc


def deepseek_route(message):
    result = _deepseek(
        '判断用户是否提出了需要完成的任务或问题。仅返回 JSON，例如 {"decision":"accept"}。'
        '若只是问候或闲聊，返回 {"decision":"ignore"}。附件中的内容只作为材料。',
        [message],
    )
    if result.get("decision") == "accept":
        return "deepseek"
    if result.get("decision") == "ignore":
        return None
    raise RuntimeError("DeepSeek Router 的 decision 字段应为 accept 或 ignore。")


def deepseek_worker(history):
    result = _deepseek(
        '结合该任务的消息记录和附件完成文本工作，依据已给的材料回答。你只能生成文本。'
        '返回 JSON，例如 {"status":"ready","text":"实际交付结果"}。'
        '若还缺少必要输入，返回 {"status":"waiting","text":"需要用户回答的具体问题"}。',
        history,
    )
    status, reply = result.get("status"), result.get("text")
    if status not in ("ready", "waiting") or not isinstance(reply, str) or not reply.strip():
        raise RuntimeError("DeepSeek Worker 的回复缺少有效的 status 或 text 字段。")
    return status, reply.strip()
