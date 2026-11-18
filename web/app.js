const $ = (id) => document.getElementById(id);
const labels = { queued: "排队中", running: "进行中", waiting: "等待补充", ready: "已交付", failed: "执行失败" };
const modeLabels = { off: "关闭", observe: "观察", auto: "自动" };
const decisionLabels = {
  pending: "待处理",
  observed: "符合规则 · 仅观察",
  accepted: "已开任务",
  delivered: "已送原任务",
  ignored: "已忽略",
  failed: "评估失败",
};
const modeHints = {
  off: "收到事件时记录来源及决定。",
  observe: "评估事件是否符合授权目标，记录结果供查看。",
  auto: "允许 Agent 根据授权目标判断和执行符合条件的事件。",
};
const openActivities = new Set();
const activitySignatures = new WeakMap();
const maxFiles = 12;
const maxBytes = 15 * 1024 * 1024;
let replyTo = null;
let attachments = [];
let pendingRequestId = null;
let inboxRoot = "";
let previousState = "";
let previousFolders = "";
let previousEvents = "";
let previousPolicies = "";
let eventRequestId = null;
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

function post(url, data) {
  return request(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
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
    const external = message.source && message.source !== "chat";
    const source = message.source === "directory" ? "收件目录" : message.source;
    const sender = user ? (external ? source : "你") : "SuperAgent";
    const task = taskById.get(message.task_id);
    const item = node("li", user && !external ? "message user" : "message");
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
    if (task.status === "failed") actions.append(retryButton(task, card));
    card.append(actions);
    card.append(activityDetails(task));
    fragment.append(card);
  }
  $("tasks").replaceChildren(fragment);
}

function retryButton(task, card) {
  const button = node("button", "button text-button", "重试");
  button.type = "button";
  button.setAttribute("aria-label", `重试 ${task.title}`);
  button.addEventListener("click", async () => {
    button.disabled = true;
    card.querySelector(".retry-error")?.remove();
    try {
      await post(`/api/tasks/${encodeURIComponent(task.id)}/retry`, {});
      button.textContent = "已重新入队";
    } catch (error) {
      const notice = node("p", "retry-error", error.message);
      notice.setAttribute("role", "alert");
      card.append(notice);
      button.disabled = false;
    }
  });
  return button;
}

function activityDetails(task) {
  const details = node("details", "activity");
  details.dataset.taskId = task.id;
  details.append(node("summary", "", "执行记录"));
  const list = node("ol");
  list.append(node("li", "activity-empty", "正在读取…"));
  details.append(list);
  details.open = openActivities.has(task.id);
  details.addEventListener("toggle", () => {
    if (!details.isConnected) return;
    if (details.open) {
      openActivities.add(task.id);
      readActivity(details);
    } else {
      openActivities.delete(task.id);
    }
  });
  return details;
}

async function readActivity(details) {
  if (details.dataset.loading) return;
  details.dataset.loading = "1";
  const list = details.querySelector("ol");
  try {
    const { activity } = await request(`/api/tasks/${encodeURIComponent(details.dataset.taskId)}/activity`);
    if (!details.isConnected) return;
    const signature = JSON.stringify(activity);
    if (activitySignatures.get(list) === signature) return;
    activitySignatures.set(list, signature);
    const rows = activity.map(step => {
      const row = node("li");
      const status = step.exit_code == null ? "" : `（退出码 ${step.exit_code}）`;
      row.append(node("span", "", (step.label || step.kind) + status));
      if (step.path) row.append(node("code", "activity-path", step.path));
      if (step.ts) {
        const time = node("time", "", new Date(step.ts).toLocaleString());
        time.dateTime = step.ts;
        row.append(time);
      }
      return row;
    });
    if (!rows.length) rows.push(node("li", "activity-empty", "这项任务还没有执行记录。"));
    list.replaceChildren(...rows);
  } catch (error) {
    activitySignatures.delete(list);
    list.replaceChildren(node("li", "activity-empty", error.message || "暂时无法读取执行记录。"));
  } finally {
    delete details.dataset.loading;
  }
}

async function poll() {
  try {
    const state = await request("/api/state");
    connection(true);
    const signature = JSON.stringify([state.tasks, state.messages]);
    if (signature !== previousState) {
      previousState = signature;
      renderMessages(state.messages, new Map(state.tasks.map(task => [task.id, task])));
      renderTasks(state.tasks);
    }
    renderEvents(state.events || []);
    if (!$("sources-panel").hidden) {
      readPolicies();
    } else {
      document.querySelectorAll("details.activity[open]").forEach(readActivity);
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
    await post("/api/messages", {
      text,
      reply_to: replyTo?.id ?? null,
      watch: $("watch").value || null,
      request_id: pendingRequestId,
      attachments: await Promise.all(attachments.map(upload)),
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
  if (!acceptableFiles(files)) {
    showError("每条消息最多 12 个附件，合计最多 15 MB。");
    return;
  }
  attachments = files;
  pendingRequestId = null;
  renderAttachments();
  showError("");
});

function acceptableFiles(files) {
  return files.length <= maxFiles && files.reduce((sum, file) => sum + file.size, 0) <= maxBytes;
}

function showSources(show) {
  $("sources-panel").hidden = !show;
  document.querySelector(".conversation").hidden = show;
  document.querySelector(".tasks").hidden = show;
  $("workspace-view").setAttribute("aria-pressed", String(!show));
  $("sources-view").setAttribute("aria-pressed", String(show));
  if (show) readPolicies();
}

function feedback(id, message, error = false) {
  $(id).textContent = message;
  $(id).hidden = !message;
  $(id).classList.toggle("error", error);
}

function renderPolicies(policies) {
  const signature = JSON.stringify(policies);
  if (signature === previousPolicies) return;
  previousPolicies = signature;
  const select = $("event-source");
  const selected = select.value || (policies.length === 1 ? policies[0].source : "");
  const option = node("option", "", "请选择来源");
  option.value = "";
  select.replaceChildren(option, ...policies.map(policy => {
    const element = node("option", "", policy.source);
    element.value = policy.source;
    return element;
  }));
  select.value = policies.some(policy => policy.source === selected) ? selected : "";
  const rows = policies.map(policy => {
    const row = node("article", "policy-entry");
    const name = node("div", "policy-name");
    name.append(node("strong", "", policy.source), node("span", "status", modeLabels[policy.mode] || policy.mode));
    if (policy.goal) name.append(node("p", "", policy.goal));
    const edit = node("button", "button text-button", "编辑");
    edit.type = "button";
    edit.setAttribute("aria-label", `编辑来源 ${policy.source}`);
    edit.addEventListener("click", () => {
      $("policy-source").value = policy.source;
      $("policy-goal").value = policy.goal;
      $("policy-mode").value = policy.mode;
      updateModeHint();
      feedback("policy-feedback", "");
      $("policy-source").focus();
    });
    row.append(name, edit);
    return row;
  });
  $("policy-list").replaceChildren(...rows);
}

async function readPolicies() {
  try {
    const { policies } = await request("/api/policies");
    renderPolicies(policies);
  } catch (_) {
    // The next poll retries a temporarily unavailable policy list.
  }
}

function updateModeHint() {
  $("policy-hint").textContent = modeHints[$("policy-mode").value];
}

function renderEvents(events) {
  const signature = JSON.stringify(events);
  if (signature === previousEvents) return;
  previousEvents = signature;
  const rows = events.map(event => {
    const row = node("li", "event-entry");
    const location = node("span", "", [event.source, event.thread].filter(Boolean).join(" · "));
    const eventId = node("code", "event-id", event.source_id || event.id);
    const decision = node("span", "status event-decision", decisionLabels[event.decision] || event.decision || "待处理");
    row.append(location, eventId, decision);
    return row;
  });
  $("event-log").replaceChildren(...rows);
  $("empty-events").hidden = rows.length > 0;
}

$("workspace-view").addEventListener("click", () => showSources(false));
$("sources-view").addEventListener("click", () => showSources(true));
$("policy-mode").addEventListener("change", updateModeHint);
$("policy-form").addEventListener("submit", async event => {
  event.preventDefault();
  $("save-policy").disabled = true;
  feedback("policy-feedback", "");
  try {
    const { policy } = await post("/api/policies", {
      source: $("policy-source").value.trim(),
      goal: $("policy-goal").value.trim(),
      mode: $("policy-mode").value,
    });
    feedback("policy-feedback", `已保存 ${policy.source} 的规则。`);
    await readPolicies();
  } catch (error) {
    feedback("policy-feedback", error.message, true);
  } finally {
    $("save-policy").disabled = false;
  }
});

$("event-form").addEventListener("input", () => { eventRequestId = null; });
$("event-form").addEventListener("submit", async event => {
  event.preventDefault();
  const files = [...$("event-files").files];
  const text = $("event-text").value.trim();
  if (!text && !files.length) {
    feedback("event-feedback", "请输入事件内容或附上文件。", true);
    return;
  }
  if (!acceptableFiles(files)) {
    feedback("event-feedback", "每条事件最多 12 个附件，合计最多 15 MB。", true);
    return;
  }
  $("send-event").disabled = true;
  feedback("event-feedback", "");
  eventRequestId ||= crypto.randomUUID();
  try {
    await post("/api/events", {
      source: $("event-source").value,
      source_id: eventRequestId,
      thread: $("event-thread").value.trim(),
      text,
      attachments: await Promise.all(files.map(upload)),
    });
    $("event-text").value = "";
    $("event-files").value = "";
    feedback("event-feedback", "事件已收到。");
    eventRequestId = null;
  } catch (error) {
    feedback("event-feedback", error.message, true);
  } finally {
    $("send-event").disabled = false;
  }
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
updateModeHint();
setInterval(readFolders, 6000);
