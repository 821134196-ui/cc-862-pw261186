/* Dashboard frontend: poll /api/state once per second and render. */
"use strict";

const $ = (id) => document.getElementById(id);
let lastAuditLen = 0;

async function post(url, body = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({ ok: false, error: "bad json" }));
  if (!data.ok) console.warn(url, data.error);
  return data;
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

function pct(part, total) {
  if (!total) return 0;
  return Math.min(100, Math.round((part / total) * 100));
}

function meterClass(ratio) {
  return ratio >= 0.9 ? "hot" : ratio >= 0.6 ? "warn" : "";
}

// ----------------------------------------------------------------- rendering
function renderServerTiles(s) {
  const tiles = [
    { label: "在线节点", value: s.live_nodes, hint: `墓碑 ${s.tombstones} 个` },
    { label: "Head revision", value: s.head_revision,
      hint: `保留日志 ${s.retained_events} 条` },
    { label: "压缩水位线", value: s.compacted_through,
      hint: `压缩 ${s.compactions} 次` },
    { label: "全局积压", value: s.total_queued_items,
      hint: `预算 ${s.memory_budget_items} 项 · ${s.subscriptions}/${s.subscription_cap} 连接` },
  ];
  $("server-tiles").innerHTML = tiles.map((t) => `
    <section class="card tile">
      <div class="label">${esc(t.label)}</div>
      <div class="value">${t.value}</div>
      <div class="hint">${esc(t.hint)}</div>
    </section>`).join("");
  $("pill-head").textContent = s.head_revision;
  $("pill-watermark").textContent = s.compacted_through;
}

function renderWatermark(s) {
  const head = Math.max(1, s.head_revision);
  const compacted = pct(s.compacted_through, head);
  const retained = pct(s.retained_events, head);
  const bar = $("wm-bar");
  bar.children[0].style.width = compacted + "%";
  bar.children[1].style.width = retained + "%";
}

function renderNodes(nodes) {
  $("node-tbody").innerHTML = nodes.map((n) => `
    <tr>
      <td class="name">${esc(n.name)}</td>
      <td class="mono">${esc(n.address)}</td>
      <td>v${n.version}</td>
      <td>${esc(n.zone)}</td>
      <td>${n.age}s</td>
    </tr>`).join("") || `<tr><td colspan="5" class="muted">（空）</td></tr>`;
}

function renderTombstones(tombstones) {
  $("tomb-tbody").innerHTML = tombstones.map((t) => `
    <tr><td class="name">${esc(t.name)}</td><td>v${t.version}</td></tr>`).join("")
    || `<tr><td colspan="2" class="muted">（无墓碑）</td></tr>`;
}

const CARD_CLASS = { fast: "c-fast", slow: "c-slow", paused: "c-paused" };

function renderClients(clients) {
  const host = $("client-cards");
  const existing = new Set([...host.children].map((c) => c.dataset.name));
  const incoming = new Set(clients.map((c) => c.name));
  if (existing.size !== incoming.size ||
      [...incoming].some((n) => !existing.has(n))) {
    host.innerHTML = "";  // structural change: rebuild
  }
  for (const c of clients) {
    let card = host.querySelector(`[data-name="${CSS.escape(c.name)}"]`);
    if (!card) {
      card = document.createElement("section");
      card.className = `card client-card ${CARD_CLASS[c.name] || "c-fast"}`;
      card.dataset.name = c.name;
      host.appendChild(card);
    }
    const ratio = c.queue_max ? c.queued / c.queue_max : 0;
    const diverge = c.divergence.length
      ? `<div class="divergence">${c.divergence.map((d) =>
          `<div>⚠ ${esc(d)}</div>`).join("")}</div>` : "";
    card.innerHTML = `
      <div class="client-head">
        <span class="who">${esc(c.name)}</span>
        <span class="spacer"></span>
        <span class="badge ${esc(c.state)}">${esc(c.state)}</span>
      </div>
      <div class="kv">
        <span class="k">已应用 revision</span><span class="v">${c.last_revision}</span>
        <span class="k">追赶差距 (lag)</span>
        <span class="v ${c.lag ? "" : "flash-good"}">${c.lag}</span>
        <span class="k">本地节点数</span><span class="v">${c.view_size}</span>
        <span class="k">订阅状态</span><span class="v">${esc(c.sub_status)}</span>
        <span class="k">处理延迟</span><span class="v">${c.process_delay}s/事件</span>
        <span class="k">最大端到端延迟</span><span class="v">${c.max_latency_ms}ms</span>
        <span class="k">快照 / 重置 / 丢弃</span>
        <span class="v">${c.snapshots} / ${c.resets} / ${c.drops}</span>
        <span class="k">陈旧事件拦截</span><span class="v">${c.stale_rejected}</span>
        <span class="k">重连次数</span><span class="v">${c.reconnects}</span>
      </div>
      <div class="meter ${meterClass(ratio)}">
        <i style="width:${pct(c.queued, c.queue_max)}%"></i>
      </div>
      <div class="meter-label">
        <span>服务端缓冲 ${c.queued}/${c.queue_max} · ~${c.queued_bytes}B</span>
        <span>${pct(c.queued, c.queue_max)}%</span>
      </div>
      ${diverge}
      <div class="controls">
        <button onclick="act('pause','${esc(c.name)}')"
          ${c.state === "PAUSED" ? "disabled" : ""}>暂停</button>
        <button class="primary" onclick="act('resume','${esc(c.name)}')"
          ${c.state !== "PAUSED" ? "disabled" : ""}>恢复</button>
        <button onclick="act('disconnect','${esc(c.name)}')"
          ${c.state === "DISCONNECTED" ? "disabled" : ""}>断线</button>
        <button onclick="act('reconnect','${esc(c.name)}')"
          ${c.state !== "DISCONNECTED" ? "disabled" : ""}>重连</button>
      </div>
      <div class="controls">
        <label class="field">处理延迟(s)
          <input type="number" step="0.05" min="0" value="${c.process_delay}"
            style="width:80px"
            onchange="post('/api/client/delay',{name:'${esc(c.name)}',delay:parseFloat(this.value)})">
        </label>
      </div>`;
  }
}

function renderAudit(lines) {
  const el = $("audit");
  el.innerHTML = lines.map((line) => {
    let cls = "line";
    if (line.includes("SNAPSHOT") || line.includes("snapshot")) cls += " snapshot";
    if (line.includes("STALE")) cls += " stale";
    if (line.includes("compaction") || line.includes("COMPACTED")) cls += " compact";
    return `<span class="${cls}">${esc(line)}</span>`;
  }).join("");
  el.scrollTop = el.scrollHeight;
}

// ------------------------------------------------------------------- controls
async function act(action, name) {
  await post(`/api/client/${action}`, { name });
  await refresh();
}

async function registerNode() {
  await post("/api/register", {
    name: $("in-name").value || "svc-new",
    address: $("in-addr").value || "127.0.0.1",
    zone: $("in-zone").value,
  });
  await refresh();
}

async function deregisterNode() {
  const first = $("node-tbody").querySelector("td.name");
  if (!first) return;
  await post("/api/deregister", { name: first.textContent });
  await refresh();
}

async function compactHistory() {
  const retain = parseInt($("in-retain").value, 10) || 6;
  const r = await post("/api/compact", { retain });
  if (r.ok) console.log("compacted to", r.watermark);
  await refresh();
}

let churnOn = false;
async function toggleChurn() {
  churnOn = !churnOn;
  await post("/api/churn", { enabled: churnOn });
  $("btn-churn").textContent = churnOn ? "停止自动变更" : "启动自动变更";
}

// ---------------------------------------------------------------------- poll
async function refresh() {
  try {
    const res = await fetch("/api/state", { cache: "no-store" });
    const data = await res.json();
    $("conn-pill").textContent = "实时";
    renderServerTiles(data.server);
    renderWatermark(data.server);
    renderNodes(data.nodes);
    renderTombstones(data.tombstones);
    renderClients(data.clients);
    renderAudit(data.audit);
    if (data.churn_enabled && !churnOn) {
      churnOn = true;
      $("btn-churn").textContent = "停止自动变更";
    }
  } catch (err) {
    $("conn-pill").textContent = "已断开（引擎未运行？）";
  }
}

refresh();
setInterval(refresh, 1000);
