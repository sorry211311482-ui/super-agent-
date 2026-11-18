const $ = (id) => document.getElementById(id);
const labels = { queued: "排队中", running: "进行中", waiting: "等待补充", ready: "已交付", failed: "执行失败" };
const maxFiles = 12;
const maxBytes = 15 * 1024 * 1024;
let replyTo = null;
let attachments = [];
let pendingRequestId = null;
let inboxRoot = "";
let previousState = "";
let previousFolders = "";
let sending = false;

function node(tag, className, text) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (text !== undefined) el.textContent = text;
  return el;
}

async function request(url, options) {
  const response = await fetch(url, { cache: "no-store", ...options });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `请求失败（${response.status}）`);
  return data;
}

function connection(online) {
  const cls = online ? "connection online" : "connection offline";
  if ($("connection").className === cls) return;
  $("connection").className = cls;
  $("connection").replaceChildren(node("span"), online ? "本地已连接" : "正在尝试重连");
}

function showError(message) {
  $("form-error").textContent = message;
  $("form-error").hidden = !message;
}

function setReply(task) {
  replyTo = task;
  $("reply").hidden = !task;
  $("reply-heading").textContent = task ? `回复任务 · ${task.title}` : "";
  pendingRequestId = null;
  if (task) $("text").focus();
}

function fileName(path) {
  return path.split(/[\\/]/).pop();
}

function artifactLink(taskId, file) {
  const link = node("a", "", fileName(file));
  link.href = `/api/tasks/${encodeURIComponent(taskId)}/files/${encodeURIComponent(file)}`;
  link.download = fileName(file);
  return link;
}

function renderMessages(messages, taskById) {
  const list = $("messages");
  const timeline = $("timeline");
  const scrollToLatest = timeline.scrollHeight - timeline.scrollTop - timeline.clientHeight < 80;
  const fragment = document.createDocumentFragment();
  for (const message of messages) {
    const user = message.role === "user";
    const fromInbox = message.source && message.source !== "chat";
    const sender = user ? (fromInbox ? "收件目录" : "你") : "SuperAgent";
    const task = taskById.get(message.task_id);
    const item = node("li", user && !fromInbox ? "message user" : "message");
    item.append(node("span", "sender", task ? `${sender} · ${task.title}` : sender));
    if (message.text) item.append(node("div", "message-body", message.text));
    if (message.files && message.files.length) {
      const files = node("div", "files");
      message.files.forEach((file, index) => {
        if (index) files.append(" · ");
        files.append(!user && task?.files.includes(file) ? artifactLink(task.id, file) : fileName(file));
      });
      item.append(files);
    }
    fragment.append(item);
  }
  list.replaceChildren(fragment);
  $("empty-chat").hidden = messages.length > 0;
  if (scrollToLatest) timeline.scrollTop = timeline.scrollHeight;
}

function renderTasks(tasks) {
  const running = tasks.filter(task => task.status === "running").length;
  $("running-count").textContent = running ? `进行中 ${running}` : "";
  if (!tasks.length) {
    const empty = node("div", "empty");
    empty.append(node("p", "", "任务接单后会显示在这里。"));
    $("tasks").replaceChildren(empty);
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const task of tasks) {
    const card = node("article", "task-card");
    const header = node("div", "task-top");
    const status = labels[task.status] || "处理中";
    header.append(node("h3", "", task.title), node("span", `status ${task.status}`, status));
    card.append(header);
    if (task.summary || task.goal) card.append(node("p", "", task.summary || task.goal));
    if (task.question && task.question !== task.summary) card.append(node("p", "question", task.question));
    if (task.watch && task.watch.length) {
      const folders = task.watch.map(path => path.startsWith(inboxRoot + "/") ? path.slice(inboxRoot.length + 1) : path);
      card.append(node("p", "watch-path", `订阅目录 · ${folders.join("、")}`));
    }
    if (task.files && task.files.length) {
      const files = node("ul", "artifacts");
      for (const file of task.files) {
        const row = node("li");
        row.append(artifactLink(task.id, file));
        files.append(row);
      }
      card.append(files);
    }
    const actions = node("div", "task-actions");
    const reply = node("button", "button text-button", "回复任务");
    reply.setAttribute("aria-label", `回复任务 ${task.title}`);
    reply.type = "button";
    reply.addEventListener("click", () => {
      if (!sending) setReply(task);
    });
    actions.append(reply);
    card.append(actions);
    fragment.append(card);
  }
  $("tasks").replaceChildren(fragment);
}

async function poll() {
  try {
    const state = await request("/api/state");
    connection(true);
    const signature = JSON.stringify(state);
    if (signature !== previousState) {
      previousState = signature;
      renderMessages(state.messages, new Map(state.tasks.map(task => [task.id, task])));
      renderTasks(state.tasks);
    }
  } catch (_) {
    connection(false);
  } finally {
    setTimeout(poll, document.hidden ? 4000 : 1100);
  }
}

async function readFolders(manual = false) {
  try {
    const config = await request("/api/config");
    inboxRoot = config.inbox;
    $("inbox-path").textContent = inboxRoot;
    $("copy-inbox").disabled = false;
    const signature = JSON.stringify(config.folders);
    if (signature === previousFolders) return;
    previousFolders = signature;
    const current = $("watch").value;
    const empty = node("option", "", "订阅目录");
    empty.value = "";
    const options = config.folders.map(name => {
      const option = node("option", "", name);
      option.value = name;
      return option;
    });
    $("watch").replaceChildren(empty, ...options);
    $("watch").value = config.folders.includes(current) ? current : "";
  } catch (_) {
    if (manual) showError("暂时无法读取收件目录，请稍后重试。");
  }
}

function renderAttachments() {
  const list = $("selected-files");
  const fragment = document.createDocumentFragment();
  attachments.forEach((file, index) => {
    const pill = node("span", "file-pill");
    const button = node("button", "icon-button", "×");
    button.type = "button";
    button.disabled = sending;
    button.setAttribute("aria-label", `移除 ${file.name}`);
    button.addEventListener("click", () => {
      attachments.splice(index, 1);
      pendingRequestId = null;
      renderAttachments();
    });
    pill.append(node("span", "", file.name), button);
    fragment.append(pill);
  });
  list.replaceChildren(fragment);
  list.hidden = !attachments.length;
}

function upload(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.addEventListener("load", () => {
      resolve({ name: file.name, data: reader.result.split(",", 2)[1] });
    });
    reader.addEventListener("error", () => reject(new Error(`无法读取附件 ${file.name}`)));
    reader.readAsDataURL(file);
  });
}

$("composer").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (sending) return;
  const text = $("text").value.trim();
  if (!text && !attachments.length) {
    showError("请输入任务要求或附上文件。");
    $("text").focus();
    return;
  }
  sending = true;
  const controls = ["submit", "text", "files", "watch", "cancel-reply"].map($);
  controls.forEach(el => { el.disabled = true; });
  $("submit").textContent = "发送中…";
  renderAttachments();
  showError("");
  pendingRequestId ||= crypto.randomUUID();
  try {
    await request("/api/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        text,
        reply_to: replyTo?.id ?? null,
        watch: $("watch").value || null,
        request_id: pendingRequestId,
        attachments: await Promise.all(attachments.map(upload)),
      }),
    });
    $("text").value = "";
    $("watch").value = "";
    attachments = [];
    setReply(null);
  } catch (error) {
    showError(error.message || "发送失败，请重试。");
  } finally {
    sending = false;
    controls.forEach(el => { el.disabled = false; });
    $("submit").textContent = "发送 ↑";
    renderAttachments();
    $("text").focus();
  }
});

$("files").addEventListener("change", (event) => {
  const files = [...attachments, ...event.target.files];
  event.target.value = "";
  if (files.length > maxFiles || files.reduce((sum, file) => sum + file.size, 0) > maxBytes) {
    showError("每条消息最多 12 个附件，合计最多 15 MB。");
    return;
  }
  attachments = files;
  pendingRequestId = null;
  renderAttachments();
  showError("");
});
$("text").addEventListener("input", () => { pendingRequestId = null; });
$("watch").addEventListener("change", () => { pendingRequestId = null; });
$("cancel-reply").addEventListener("click", () => setReply(null));
$("refresh-folders").addEventListener("click", () => readFolders(true));
$("copy-inbox").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(inboxRoot);
    $("copy-inbox").textContent = "已复制";
    setTimeout(() => { $("copy-inbox").textContent = "复制路径"; }, 1500);
  } catch (_) {
    const range = document.createRange();
    range.selectNodeContents($("inbox-path"));
    window.getSelection().removeAllRanges();
    window.getSelection().addRange(range);
  }
});
$("text").addEventListener("keydown", (event) => {
  if (!event.isComposing && event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
    event.preventDefault();
    $("composer").requestSubmit();
  }
});

poll();
readFolders();
setInterval(readFolders, 6000);
