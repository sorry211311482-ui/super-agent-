(() => {
  const canvas = document.getElementById("flow-canvas");
  const stage = canvas?.parentElement;
  if (!stage) return;
  const ctx = canvas.getContext("2d");
  const input = document.getElementById("flow-input");
  const router = document.getElementById("flow-router");
  const output = document.getElementById("flow-output");
  const workers = document.getElementById("flow-workers");
  const count = document.getElementById("flow-count");
  const caption = document.getElementById("flow-caption");
  const replayButton = document.getElementById("flow-replay");
  const narrow = matchMedia("(max-width: 760px)");
  const reduced = matchMedia("(prefers-reduced-motion: reduce)");
  const ink = {
    input: "#64d7ff",
    dispatch: "#a29aff",
    direct: "#64d7ff",
    return: "#60efcb",
  };
  const states = {
    queued: "已入队", running: "执行中", waiting: "等待补充", ready: "已交付",
    failed: "执行失败", idle: "空闲",
  };
  const priority = { running: 0, queued: 1, waiting: 2, ready: 3, failed: 4 };
  const seen = new Set();
  let packets = [];
  let nodes = [];
  let signalQueue = [];
  let frame = 0;
  let slots = 2;
  let model = [];
  let signature = "";
  let started = false;
  let replaying = false;

  function node(tag, className, text) {
    const element = document.createElement(tag);
    element.className = className;
    element.textContent = text;
    return element;
  }

  function workerNode(task, number) {
    const element = document.createElement(task.id ? "button" : "div");
    element.className = "flow-worker";
    element.dataset.status = task.status;
    const tag = node("span", "flow-worker-tag", "Worker");
    const title = node("span", "flow-worker-title", task.title || "可接单");
    const status = node("span", "flow-worker-status", states[task.status]);
    element.append(tag, title, status);
    if (task.id) {
      element.type = "button";
      element.title = task.title;
      element.setAttribute("aria-label", `查看 Worker ${number} 的任务 ${task.title}`);
      element.addEventListener("click", () => {
        document.dispatchEvent(new CustomEvent("superagent:select-task", { detail: task.id }));
      });
    }
    return element;
  }

  function renderWorkers(preferred = []) {
    const pinned = new Set([...preferred, ...signalQueue.map(signal => signal.packet.task_id)]);
    const sorted = model.slice().sort((a, b) => {
      const rank = task => pinned.has(task.id) ? -1 : priority[task.status] ?? 5;
      return rank(a) - rank(b);
    });
    const visible = sorted.slice(0, Math.min(4, Math.max(2, slots)));
    while (visible.length < Math.min(2, slots)) visible.push({ id: "", status: "idle" });
    const next = JSON.stringify(visible.map(task => [task.id, task.title, task.status]));
    if (next !== signature) {
      signature = next;
      const elements = visible.map((task, index) => workerNode(task, index + 1));
      workers.replaceChildren(...elements);
      nodes = visible.map((task, index) => ({ task, element: elements[index] }));
    }
    const running = model.filter(task => task.status === "running").length;
    count.textContent = `${running} / ${slots} 执行中`;
  }

  function port(element, outgoing, spread = 0) {
    const rect = element.getBoundingClientRect();
    const origin = canvas.getBoundingClientRect();
    if (narrow.matches) {
      return [
        rect.left + rect.width * (0.5 + spread) - origin.left,
        (outgoing ? rect.bottom : rect.top) - origin.top,
      ];
    }
    return [
      (outgoing ? rect.right : rect.left) - origin.left,
      rect.top + rect.height * (0.5 + spread) - origin.top,
    ];
  }

  function curve(start, end) {
    const axis = narrow.matches ? 1 : 0;
    const distance = Math.max(14, (end[axis] - start[axis]) * 0.48);
    const a = start.slice();
    const b = end.slice();
    a[axis] += distance;
    b[axis] -= distance;
    return [start, a, b, end];
  }

  function bypass(element) {
    const start = port(input, true, narrow.matches ? -0.43 : -0.3);
    const end = port(element, false, narrow.matches ? -0.43 : -0.30);
    if (narrow.matches) {
      const left = Math.max(8, Math.min(start[0], end[0]) - 40);
      return [start, [left, start[1] + 10], [left, end[1] - 12], end];
    }
    const above = Math.max(10, Math.min(start[1], end[1]) - 90);
    return [start, [start[0] + 42, above], [end[0] - 50, above], end];
  }

  function point([p0, p1, p2, p3], time) {
    const inverse = 1 - time;
    return [0, 1].map(axis =>
      inverse ** 3 * p0[axis] + 3 * inverse ** 2 * time * p1[axis] +
      3 * inverse * time ** 2 * p2[axis] + time ** 3 * p3[axis]);
  }

  function path(points, color, bright = false, dashed = false) {
    ctx.save();
    ctx.beginPath();
    ctx.moveTo(...points[0]);
    ctx.bezierCurveTo(...points.slice(1).flat());
    ctx.strokeStyle = color;
    ctx.lineWidth = bright ? 1.7 : 1.1;
    if (dashed) ctx.setLineDash([3, 5]);
    ctx.stroke();
    ctx.restore();
  }

  function spark(points, time, color, stationary = false) {
    ctx.save();
    ctx.lineCap = "round";
    if (!stationary) {
      for (let i = 0; i < 12; i++) {
        const t = time - i * 0.009;
        if (t <= 0) break;
        ctx.globalAlpha = 0.8 * (1 - i / 12);
        ctx.strokeStyle = color;
        ctx.lineWidth = 2.4 * (1 - i / 16);
        ctx.beginPath();
        ctx.moveTo(...point(points, t));
        ctx.lineTo(...point(points, Math.max(0, t - 0.012)));
        ctx.stroke();
      }
    }
    const [x, y] = point(points, time);
    const aura = ctx.createRadialGradient(x, y, 0, x, y, 15);
    aura.addColorStop(0, color);
    aura.addColorStop(1, "transparent");
    ctx.globalAlpha = 0.55;
    ctx.fillStyle = aura;
    ctx.fillRect(x - 15, y - 15, 30, 30);
    ctx.globalAlpha = 1;
    ctx.shadowBlur = 11;
    ctx.shadowColor = color;
    ctx.fillStyle = "#f5fcff";
    ctx.beginPath();
    ctx.arc(x, y, 3, 0, 2 * Math.PI);
    ctx.fill();
    ctx.restore();
  }

  function draw(time) {
    frame = 0;
    const { width, height } = canvas.getBoundingClientRect();
    if (!width || !height || !ctx) return;
    const ratio = Math.min(devicePixelRatio || 1, 2);
    const pixelsWide = Math.round(width * ratio);
    const pixelsHigh = Math.round(height * ratio);
    if (canvas.width !== pixelsWide || canvas.height !== pixelsHigh) {
      canvas.width = pixelsWide;
      canvas.height = pixelsHigh;
    }
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    ctx.clearRect(0, 0, width, height);
    const incoming = curve(port(input, true), port(router, false));
    path(incoming, "rgba(107,193,229,.42)");
    const edges = new Map();
    nodes.forEach(({ task, element }, index) => {
      const spread = (index - (nodes.length - 1) / 2) * 0.13;
      const dispatch = curve(port(router, true, spread), port(element, false));
      const returned = curve(port(element, true), port(output, false, spread));
      const running = task.status === "running";
      path(dispatch, running ? "rgba(151,133,255,.72)" : "rgba(139,134,211,.32)", running);
      path(returned, running ? "rgba(83,227,190,.48)" : "rgba(86,181,174,.30)");
      edges.set(task.id, { dispatch, return: returned, element });
    });
    signalQueue = signalQueue.filter(signal => time < signal.end);
    for (const signal of signalQueue) {
      if (time < signal.start) continue;
      const { packet } = signal;
      const edge = edges.get(packet.task_id);
      const direct = packet.kind === "dispatch" && packet.via === "direct";
      const points = packet.kind === "input" ? incoming :
        direct && edge ? bypass(edge.element) : edge?.[packet.kind];
      if (!points) continue;
      if (direct) path(points, "rgba(100,215,255,.42)", true, true);
      const progress = (time - signal.start) / (signal.end - signal.start);
      const limit = packet.kind === "input" && packet.status === "ignored" ? 0.63 : 1;
      spark(points, (reduced.matches ? 0.88 : progress) * limit, ink[direct ? "direct" : packet.kind], reduced.matches);
    }
    if (signalQueue.length) wake();
    else if (replaying) {
      replaying = false;
      caption.textContent = "回放完成";
      renderWorkers();
      wake();
    }
  }

  function wake() {
    if (!frame) frame = requestAnimationFrame(draw);
  }

  function captionFor(packet) {
    if (packet.kind === "input") {
      return packet.status === "ignored" ? "输入已记录 · 未派发" : "收到新输入";
    }
    const title = model.find(task => task.id === packet.task_id)?.title;
    const transition = packet.kind === "return" ? "Worker 返回结果" :
      packet.via === "direct" ? "输入直达 Worker" : "Router 派发给 Worker";
    return title ? `${transition} · ${title}` : transition;
  }

  function send(batch, replay = false) {
    const signals = batch.filter(packet =>
      packet.kind !== "input" || packet.via !== "direct" ||
        !batch.some(item => item.kind === "dispatch" && item.event_id === packet.event_id));
    if (!signals.length) return;
    if (replaying || replay) signalQueue = [];
    replaying = replay;
    renderWorkers(signals.map(packet => packet.task_id).filter(Boolean));
    const now = performance.now();
    const offsets = { input: 0, dispatch: 900, return: 1800 };
    const kinds = new Set(signals.map(packet => packet.kind));
    const firstOffset = Math.min(...[...kinds].map(kind => offsets[kind]));
    const counts = {};
    for (const packet of signals.slice(-12)) {
      counts[packet.kind] = (counts[packet.kind] || 0) + 1;
      const offset = reduced.matches ? 0 : offsets[packet.kind] - firstOffset + (counts[packet.kind] - 1) * 110;
      const start = now + offset;
      signalQueue.push({ packet, start, end: start + (reduced.matches ? 380 : 900) });
    }
    caption.textContent = replay ? "正在回放最近信号" : captionFor(signals.at(-1));
    wake();
  }

  function replay() {
    const dispatch = [...packets].reverse().find(packet => packet.kind === "dispatch");
    if (!dispatch) {
      send(packets.slice(-3), true);
      return;
    }
    const incoming = packets.filter(packet => packet.kind === "input" && packet.event_id === dispatch.event_id);
    const branches = packets.filter(packet => packet.kind === "dispatch" && packet.event_id === dispatch.event_id);
    const taskIds = new Set(branches.map(packet => packet.task_id));
    const returned = [...packets].reverse().find(packet =>
      packet.kind === "return" && taskIds.has(packet.task_id) &&
      model.some(task => task.id === packet.task_id && task.status === "ready"));
    send([...incoming, ...branches, ...(returned ? [returned] : [])], true);
  }

  window.superagentFlow = {
    update(state) {
      model = state.tasks || [];
      slots = Math.max(1, Number(state.flow?.slots) || 2);
      packets = state.flow?.packets || [];
      const fresh = packets.filter(packet => !seen.has(packet.id));
      packets.forEach(packet => seen.add(packet.id));
      while (seen.size > 256) seen.delete(seen.values().next().value);
      replayButton.disabled = !packets.length;
      renderWorkers(fresh.map(packet => packet.task_id).filter(Boolean));
      if (started && fresh.length) send(fresh);
      if (!started && packets.length) replay();
      started = true;
      wake();
    },
  };
  replayButton.addEventListener("click", replay);
  new ResizeObserver(wake).observe(stage);
  reduced.addEventListener("change", wake);
  narrow.addEventListener("change", wake);
  renderWorkers();
  wake();
})();
