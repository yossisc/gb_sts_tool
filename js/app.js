/**
 * Hash router — all data from /api/* (requires server.py).
 * Credentials: same-origin cookies for verified kubectl session.
 */

const app = document.getElementById("app");
const footerStatus = document.getElementById("footer-status");
const footerSession = document.getElementById("footer-session");
const k8sContextEl = document.getElementById("k8s-context");
const k8sAwsEl = document.getElementById("k8s-aws");
const navKafka = document.getElementById("nav-kafka");
const navKafkaConnect = document.getElementById("nav-kafka-connect");
const navClickhouse = document.getElementById("nav-clickhouse");
const navElasticsearch = document.getElementById("nav-elasticsearch");
const navOpensearch = document.getElementById("nav-opensearch");
const navPostgres = document.getElementById("nav-postgres");
const navCassandra = document.getElementById("nav-cassandra");

/** @type {Array<[HTMLElement | null, string]>} anchor → ``stack.components`` id */
const WORKLOAD_NAV_STACK_KEYS = [
  [navKafka, "kafka"],
  [navKafkaConnect, "kafkaconnect"],
  [navClickhouse, "clickhouse"],
  [navElasticsearch, "elasticsearch"],
  [navOpensearch, "opensearch"],
  [navPostgres, "postgresql"],
  [navCassandra, "cassandra"],
];

const VERSION = "1.0.28";

/** @type {Record<string, string> | null} */
let workloadLogContainers = null;

/** @type {{ controller: AbortController | null }} */
const workloadLogStream = { controller: null };
/** Auto-refresh timer for workload pod status panel (one active per tab). */
let workloadPodStatusTimerId = null;

const WL_STACK_COMP = {
  kafka: "kafka",
  kafkaconnect: "kafkaconnect",
  clickhouse: "clickhouse",
  elasticsearch: "elasticsearch",
  opensearch: "opensearch",
  postgresql: "postgresql",
  cassandra: "cassandra",
};

const WL_DEFAULT_POD = {
  kafka: "glassbox-kafka-0",
  kafkaconnect: "glassbox-kafkaconnect-0",
  clickhouse: "glassbox-clickhouse-0",
  elasticsearch: "glassbox-elasticsearch-master-0",
  opensearch: "glassbox-opensearch-master-0",
  postgresql: "glassbox-postgresql-0",
  cassandra: "glassbox-cassandra-0",
};

/**
 * @typedef {{ idPx: string, panelPx: string, apiPanel: string, apiExec: string, qsPod: string, workloadLogs: string, stackComp: string, podStem: string, containerLabel: string, containerEnv: string, pageName: string }} SearchPageCfg
 */
/** @type {Record<string, SearchPageCfg>} */
const SEARCH_PAGE_CFGS = {
  elasticsearch: {
    idPx: "es",
    panelPx: "es_",
    apiPanel: "/api/elasticsearch/panel",
    apiExec: "/api/elasticsearch/exec",
    qsPod: "es_pod",
    workloadLogs: "elasticsearch",
    stackComp: "elasticsearch",
    podStem: "glassbox-elasticsearch-master",
    containerLabel: "elasticsearch",
    containerEnv: "GB_STS_ES_CONTAINER",
    pageName: "Elasticsearch",
  },
  opensearch: {
    idPx: "os",
    panelPx: "os_",
    apiPanel: "/api/opensearch/panel",
    apiExec: "/api/opensearch/exec",
    qsPod: "os_pod",
    workloadLogs: "opensearch",
    stackComp: "opensearch",
    podStem: "glassbox-opensearch-master",
    containerLabel: "opensearch",
    containerEnv: "GB_STS_OS_CONTAINER",
    pageName: "OpenSearch",
  },
};

/** @type {Array<Record<string, unknown>>} */
let clustersList = [];
/** Last “List clusters” result + selection for this tab (homepage restore). */
const HOME_CLUSTER_LIST_KEY = "gb_sts_home_cluster_list_v1";
let copyBtnSeq = 0;

function esc(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function clearWorkloadPodStatusTimer() {
  if (workloadPodStatusTimerId != null) {
    clearInterval(workloadPodStatusTimerId);
    workloadPodStatusTimerId = null;
  }
}

function stopWorkloadLogStream() {
  clearWorkloadPodStatusTimer();
  if (workloadLogStream.controller) {
    workloadLogStream.controller.abort();
    workloadLogStream.controller = null;
  }
}

/**
 * @param {string} rawLine
 * @param {string} needle
 */
function logLineHtml(rawLine, needle) {
  const n = (needle || "").trim();
  if (!n) return esc(rawLine);
  const out = [];
  let i = 0;
  const L = n.length;
  while (i < rawLine.length) {
    const j = rawLine.indexOf(n, i);
    if (j < 0) {
      out.push(esc(rawLine.slice(i)));
      break;
    }
    out.push(esc(rawLine.slice(i, j)));
    out.push(`<mark class="log-line-hit">${esc(n)}</mark>`);
    i = j + L;
  }
  return out.join("");
}

/**
 * @param {HTMLElement} outEl
 * @param {string[]} rawLines
 * @param {string} filterNeedle
 */
function redrawWorkloadLogDisplay(outEl, rawLines, filterNeedle) {
  const q = (filterNeedle || "").trim();
  const rows = [];
  for (const ln of rawLines) {
    if (q && !ln.includes(q)) continue;
    rows.push(`<div class="log-line-row">${logLineHtml(ln, q)}</div>`);
  }
  outEl.innerHTML = rows.join("");
}

/**
 * @param {string} workload
 * @param {string} title
 */
function workloadLogsPanelHtml(workload, title) {
  const idBase = `wl-${workload}`;
  return `
    <section class="panel panel--span-all kafka-logs-section" data-workload-logs="${esc(workload)}">
      <div class="panel-head workload-logs-head">
        <h2 id="${esc(idBase)}-title" class="workload-logs-h2">
          <span class="workload-logs-title-text">${esc(title)}</span>
          <label class="workload-logs-filter-label"><span>Filter</span>
            <input type="text" class="text workload-logs-filter" id="${esc(idBase)}-filter" autocomplete="off" placeholder="substring…" />
          </label>
        </h2>
        <div class="panel-actions">
          <button type="button" class="btn btn-primary wl-toggle" id="${esc(idBase)}-toggle">Activate</button>
          <button type="button" class="btn wl-clear" id="${esc(idBase)}-clear">Clear</button>
        </div>
      </div>
      <p class="muted">
        Stream <code>kubectl logs</code> (<code>--tail 10</code>, <code>-f</code>). <strong>Deactivate</strong> stops the stream; leaving this page also stops it. <strong>Clear</strong> empties the buffer.
      </p>
      <div class="panel-toolbar kafka-logs-toolbar">
        <label class="field field--inline">
          <span>Pod</span>
          <select class="text" id="${esc(idBase)}-pod" aria-label="Pod for logs"></select>
        </label>
        <label class="field field--inline">
          <span>Container</span>
          <select class="text" id="${esc(idBase)}-cont" aria-label="Container for logs"></select>
        </label>
      </div>
      <div class="pre-copy-wrap kafka-logs-copy-wrap">
        <button type="button" class="btn-copy" data-copy-target="${esc(idBase)}-out" title="Copy to clipboard" aria-label="Copy logs to clipboard"><svg class="btn-copy-icon" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M16 1H4c-1.1 0-2 .9-2 2v12h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg></button>
        <div id="${esc(idBase)}-out" class="workload-logs-display kafka-logs-textarea" tabindex="0" role="log" aria-live="polite"></div>
      </div>
      <div class="panel-output-footer">
        <label class="wrap-chk"><input type="checkbox" class="js-pre-wrap" data-pre-id="${esc(idBase)}-out" checked /> wrap</label>
      </div>
    </section>`;
}

/**
 * @param {string} workload kafka|kafkaconnect|clickhouse|elasticsearch|opensearch|postgresql|cassandra
 */
function workloadPodStatusPanelHtml(workload) {
  const w = esc(workload);
  return `<section class="panel workload-pod-status" data-wps-workload="${w}">
  <div class="panel-head">
    <h2>Pod status</h2>
    <div class="panel-actions wps-toolbar">
      <button type="button" class="btn btn-primary" data-wps-refresh>Refresh</button>
      <label class="field field--inline wps-auto-wrap">
        <span>Auto-refresh</span>
        <input type="checkbox" data-wps-auto />
      </label>
      <label class="field field--inline">
        <span>Every (s)</span>
        <input type="number" class="text text-narrow" data-wps-interval min="5" max="600" value="10" step="1" />
      </label>
    </div>
  </div>
  <div class="wps-body" data-wps-out><p class="muted">Loading…</p></div>
  <p class="hint muted" data-wps-meta></p>
</section>`;
}

/**
 * @param {string} phase
 */
function workloadPodPhasePillClass(phase) {
  const p = String(phase || "").toLowerCase();
  if (p === "running") return "wps-pill wps-pill--ok";
  if (p === "pending" || p === "unknown") return "wps-pill wps-pill--warn";
  return "wps-pill wps-pill--bad";
}

/**
 * @param {HTMLElement} wrap
 * @param {unknown} j
 */
function renderWorkloadPodStatusPayload(wrap, j) {
  const out = wrap.querySelector("[data-wps-out]");
  const meta = wrap.querySelector("[data-wps-meta]");
  if (!out) return;
  if (!j || typeof j !== "object" || !j.ok) {
    out.innerHTML = `<p class="status-bad">${esc((j && j.error) || "failed")}</p>`;
    if (meta) meta.textContent = "";
    return;
  }
  const pods = Array.isArray(j.pods) ? j.pods : [];
  if (!pods.length) {
    out.innerHTML = "<p class='muted'>No matching pods in this namespace.</p>";
    if (meta) {
      meta.textContent = `Updated ${String(j.fetched_at || "N/A")} · namespace ${String(j.namespace || "N/A")}`;
    }
    return;
  }
  const parts = [];
  for (const p of pods) {
    const evs = (Array.isArray(p.events) ? p.events : [])
      .map((e) => {
        const t = String(e.type || "").toLowerCase();
        const cls = t === "warning" ? "warn" : "norm";
        return `<li class="wps-ev wps-ev--${cls}"><strong>${esc(e.reason || "N/A")}</strong> · ${esc(
          e.age || "N/A",
        )}<span class="wps-ev-msg">${esc(e.message || "N/A")}</span></li>`;
      })
      .join("");
    const restarts = Number(p.restarts);
    const rClass = Number.isFinite(restarts) && restarts > 0 ? "wps-val-warn" : "";
    parts.push(`<article class="wps-card">
  <header class="wps-card-head"><span class="wps-name">${esc(p.name || "N/A")}</span><span class="${workloadPodPhasePillClass(
    p.phase,
  )}">${esc(String(p.phase || "N/A"))}</span></header>
  <dl class="wps-dl">
    <div><dt>Ready</dt><dd><strong>${esc(String(p.ready ?? "N/A"))}</strong></dd></div>
    <div><dt>Restarts</dt><dd class="${rClass}"><strong>${esc(String(p.restarts ?? "N/A"))}</strong></dd></div>
    <div><dt>Age</dt><dd>${esc(String(p.age ?? "N/A"))}</dd></div>
    <div><dt>Node</dt><dd><code>${esc(String(p.node ?? "N/A"))}</code></dd></div>
    <div><dt>Pod CPU</dt><dd>${esc(String(p.cpu ?? "N/A"))}</dd></div>
    <div><dt>Pod memory</dt><dd>${esc(String(p.memory ?? "N/A"))}</dd></div>
    <div><dt>Node CPU</dt><dd>${esc(String(p.node_cpu ?? "N/A"))}</dd></div>
    <div><dt>Node memory</dt><dd>${esc(String(p.node_mem ?? "N/A"))}</dd></div>
  </dl>
  <div class="wps-ev-wrap"><h3 class="wps-ev-title">Recent events</h3><ul class="wps-ev-list">${evs || '<li class="muted">N/A</li>'}</ul></div>
</article>`);
  }
  out.innerHTML = `<div class="wps-grid">${parts.join("")}</div>`;
  const errBits = [];
  if (j.errors && typeof j.errors === "object") {
    for (const [k, v] of Object.entries(j.errors)) {
      if (v) errBits.push(`${k}: ${v}`);
    }
  }
  if (meta) {
    let m = `Updated ${String(j.fetched_at || "N/A")} · namespace ${String(j.namespace || "N/A")}`;
    if (errBits.length) m += ` · Partial data: ${errBits.join("; ")}`;
    meta.textContent = m;
  }
}

/**
 * @param {HTMLElement} root
 * @param {string} workload
 */
function wireWorkloadPodStatus(root, workload) {
  clearWorkloadPodStatusTimer();
  const wrap = root.querySelector(`section.workload-pod-status[data-wps-workload="${workload}"]`);
  if (!wrap) return;
  const out = wrap.querySelector("[data-wps-out]");
  const meta = wrap.querySelector("[data-wps-meta]");
  const btn = wrap.querySelector("[data-wps-refresh]");
  const auto = wrap.querySelector("[data-wps-auto]");
  const intervalInp = wrap.querySelector("[data-wps-interval]");

  async function load() {
    if (!out) return;
    out.innerHTML = "<p class='muted'>Loading…</p>";
    try {
      const u = new URL("/api/k8s/workload-pod-status", location.origin);
      u.searchParams.set("workload", workload);
      const j = await fetchJson(u.toString());
      renderWorkloadPodStatusPayload(wrap, j);
    } catch (e) {
      const d = e.detail || {};
      out.innerHTML = `<p class="status-bad">${esc(e.message || "failed")}</p>${kafkaPreWithWrapFooter(JSON.stringify(d, null, 2))}`;
      bindCopyButtons(out);
      if (meta) meta.textContent = "";
    }
  }

  function scheduleAuto() {
    clearWorkloadPodStatusTimer();
    if (!auto || !auto.checked) return;
    let sec = Number.parseInt(String(intervalInp?.value ?? "10"), 10);
    if (!Number.isFinite(sec) || sec < 5) sec = 5;
    if (sec > 600) sec = 600;
    workloadPodStatusTimerId = window.setInterval(() => void load(), sec * 1000);
  }

  btn?.addEventListener("click", () => void load());
  auto?.addEventListener("change", () => {
    clearWorkloadPodStatusTimer();
    if (auto.checked) scheduleAuto();
  });
  intervalInp?.addEventListener("change", () => {
    if (auto?.checked) {
      clearWorkloadPodStatusTimer();
      scheduleAuto();
    }
  });

  void load();
}

/**
 * @param {string} workload
 */
function workloadLogContainerDefault(workload) {
  const w = workloadLogContainers && workloadLogContainers[workload];
  if (typeof w === "string" && w) return w;
  const fb = {
    kafka: "kafka",
    kafkaconnect: "kafka",
    clickhouse: "glassbox-clickhouse",
    elasticsearch: "elasticsearch",
    opensearch: "opensearch",
    postgresql: "glassbox-postgresql",
    cassandra: "cassandra",
  };
  return fb[workload] || "kafka";
}

/**
 * @param {HTMLSelectElement | null} sel
 * @param {string} workload
 */
async function populateWorkloadLogPods(sel, workload) {
  if (!sel) return;
  const comp = WL_STACK_COMP[workload];
  let stack = null;
  try {
    stack = JSON.parse(sessionStorage.getItem("gb_sts_stack") || "null");
  } catch {
    stack = null;
  }
  if (!stack || !stack.components) {
    try {
      stack = await fetchJson("/api/k8s/stack");
    } catch {
      stack = null;
    }
  }
  const pods = stack?.ok && comp && stack.components?.[comp]?.pods;
  sel.innerHTML = "";
  if (Array.isArray(pods) && pods.length > 0) {
    const cap = workload === "kafka" ? 32 : 16;
    for (const name of pods.slice(0, cap)) {
      const o = document.createElement("option");
      o.value = String(name);
      o.textContent = String(name);
      sel.appendChild(o);
    }
    return;
  }
  const def = WL_DEFAULT_POD[workload] || WL_DEFAULT_POD.kafka;
  const o = document.createElement("option");
  o.value = def;
  o.textContent = def;
  sel.appendChild(o);
}

/**
 * @param {ParentNode} root
 * @param {string} workload
 */
function wireWorkloadLogs(root, workload) {
  const idBase = `wl-${workload}`;
  const outEl = root.querySelector(`#${idBase}-out`);
  const toggle = root.querySelector(`#${idBase}-toggle`);
  const clearBtn = root.querySelector(`#${idBase}-clear`);
  const podSel = root.querySelector(`#${idBase}-pod`);
  const contSel = root.querySelector(`#${idBase}-cont`);
  const filterInp = root.querySelector(`#${idBase}-filter`);
  if (!(outEl instanceof HTMLElement) || !(toggle instanceof HTMLButtonElement)) return;

  const contDef = workloadLogContainerDefault(workload);
  if (contSel instanceof HTMLSelectElement) {
    contSel.innerHTML = `<option value="${esc(contDef)}">${esc(contDef)}</option>`;
  }

  void populateWorkloadLogPods(podSel instanceof HTMLSelectElement ? podSel : null, workload);

  /** @type {string[]} */
  const rawLines = [];
  const maxLines = 5000;

  function redraw() {
    const q = filterInp instanceof HTMLInputElement ? filterInp.value : "";
    redrawWorkloadLogDisplay(outEl, rawLines, q);
  }

  filterInp?.addEventListener("input", () => redraw());

  clearBtn?.addEventListener("click", () => {
    rawLines.length = 0;
    redraw();
  });

  async function runStream() {
    if (workloadLogStream.controller) return;
    function pushLine(line) {
      rawLines.push(line);
      while (rawLines.length > maxLines) rawLines.shift();
    }
    const pod = podSel instanceof HTMLSelectElement ? podSel.value : WL_DEFAULT_POD[workload];
    const container = contSel instanceof HTMLSelectElement ? contSel.value : contDef;
    const u = new URL("/api/k8s/logs/stream", location.origin);
    u.searchParams.set("workload", workload);
    u.searchParams.set("pod", pod);
    u.searchParams.set("container", container);
    const ac = new AbortController();
    workloadLogStream.controller = ac;
    toggle.textContent = "Deactivate";
    try {
      const r = await fetch(u.toString(), { credentials: "same-origin", signal: ac.signal });
      if (!r.ok) {
        const txt = await r.text();
        pushLine(`[HTTP ${r.status}] ${txt.slice(0, 800)}`);
        redraw();
        return;
      }
      const reader = r.body?.getReader();
      if (!reader) {
        pushLine("[No response body]");
        redraw();
        return;
      }
      const dec = new TextDecoder();
      let carry = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        carry += dec.decode(value, { stream: true });
        let sep;
        while ((sep = carry.indexOf("\n\n")) >= 0) {
          const block = carry.slice(0, sep);
          carry = carry.slice(sep + 2);
          for (const ln of block.split("\n")) {
            if (!ln.startsWith("data: ")) continue;
            const raw = ln.slice(6);
            try {
              const o = JSON.parse(raw);
              if (o.line != null) pushLine(String(o.line));
            } catch {
              pushLine(raw);
            }
          }
          redraw();
        }
      }
    } catch (e) {
      if (e && /** @type {Error} */ (e).name !== "AbortError") {
        pushLine(`[${String(/** @type {Error} */ (e).message || e)}]`);
        redraw();
      }
    } finally {
      if (workloadLogStream.controller === ac) workloadLogStream.controller = null;
      toggle.textContent = "Activate";
    }
  }

  toggle.addEventListener("click", () => {
    if (workloadLogStream.controller) {
      workloadLogStream.controller.abort();
      return;
    }
    void runStream();
  });

  bindCopyButtons(root.querySelector(`[data-workload-logs="${workload}"]`) || root);
}

/** Rich HTML for ``panel-cmd-hint``: replace ``<T>`` / ``<G>`` with bold values when topic/group are set. */
function formatKafkaCmdHint(rawHint, topic, group) {
  const t = (topic || "").trim();
  const g = (group || "").trim();
  const parts = String(rawHint || "").split(/(<T>|<G>)/g);
  return parts
    .map((part) => {
      if (part === "<T>") {
        return t ? `<strong class="kafka-cmd-hint-value">${esc(t)}</strong>` : esc("<T>");
      }
      if (part === "<G>") {
        return g ? `<strong class="kafka-cmd-hint-value">${esc(g)}</strong>` : esc("<G>");
      }
      return esc(part);
    })
    .join("");
}

/** Highlight ``indexPattern`` wherever it appears literally in ``text`` (for ES path / curl hints). */
function formatEsIndexHighlight(text, indexPattern) {
  const p = (indexPattern || "").trim();
  const raw = String(text ?? "");
  if (!p || !raw) return esc(raw);
  const out = [];
  let i = 0;
  const L = p.length;
  while (i < raw.length) {
    const j = raw.indexOf(p, i);
    if (j < 0) {
      out.push(esc(raw.slice(i)));
      break;
    }
    out.push(esc(raw.slice(i, j)));
    out.push(`<mark class="es-index-hint-hit">${esc(p)}</mark>`);
    i = j + L;
  }
  return out.join("");
}

function preWithCopy(rawText) {
  const id = `pre-copy-${++copyBtnSeq}`;
  return `<div class="pre-copy-wrap"><button type="button" class="btn-copy" data-copy-target="${id}" title="Copy to clipboard" aria-label="Copy"><svg class="btn-copy-icon" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M16 1H4c-1.1 0-2 .9-2 2v12h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg></button><pre class="output-pre" id="${id}">${esc(rawText || "")}</pre></div>`;
}

/** Strip ANSI SGR sequences (e.g. ClickHouse PrettyCompact bold headers) for monospace alignment in HTML. */
function stripAnsiSgr(text) {
  return String(text ?? "").replace(/\u001b\[[\d;?]*[ -/]*[@-~]/g, "");
}

/** Kafka panel raw output: nowrap by default + wrap checkbox (bottom-right of block). */
function kafkaPreWithWrapFooter(rawText, opts = {}) {
  const id = `pre-copy-${++copyBtnSeq}`;
  const wrapOn = !!opts.wrapChecked;
  const base = wrapOn ? "output-pre" : "output-pre output-pre--nowrap";
  const extra = (opts.preClass || "").trim();
  const preClass = extra ? `${base} ${extra}` : base;
  const chk = wrapOn ? " checked" : "";
  return `<div class="panel-output-stack"><div class="pre-copy-wrap"><button type="button" class="btn-copy" data-copy-target="${id}" title="Copy to clipboard" aria-label="Copy"><svg class="btn-copy-icon" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M16 1H4c-1.1 0-2 .9-2 2v12h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg></button><pre class="${preClass}" id="${id}">${esc(rawText || "")}</pre></div><div class="panel-output-footer"><label class="wrap-chk"><input type="checkbox" class="js-pre-wrap" data-pre-id="${id}"${chk} /> wrap</label></div></div>`;
}

function bindCopyButtons(root) {
  root.querySelectorAll("[data-copy-target]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.getAttribute("data-copy-target");
      const pre = id ? document.getElementById(id) : null;
      let text = "";
      if (pre instanceof HTMLTextAreaElement || pre instanceof HTMLInputElement) text = pre.value || "";
      else if (pre) text = pre.textContent || "";
      try {
        await navigator.clipboard.writeText(text);
        btn.classList.add("btn-copy--done");
        setTimeout(() => btn.classList.remove("btn-copy--done"), 900);
      } catch {
        /* ignore */
      }
    });
  });
}

async function fetchJson(url, opts = {}) {
  const r = await fetch(url, {
    credentials: "same-origin",
    headers: { Accept: "application/json", ...(opts.headers || {}) },
    ...opts,
  });
  const text = await r.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`Non-JSON response (${r.status}): ${text.slice(0, 200)}`);
  }
  if (!r.ok && !data.needs_confirmation) {
    const err = new Error(data.error || `HTTP ${r.status}`);
    err.detail = data;
    err.status = r.status;
    throw err;
  }
  return data;
}

let sessionVerified = false;

function stackFromSessionStorage() {
  try {
    const raw = sessionStorage.getItem("gb_sts_stack");
    if (!raw) return null;
    const o = JSON.parse(raw);
    return o && typeof o === "object" ? o : null;
  } catch {
    return null;
  }
}

/**
 * Stack JSON from tab session or ``/api/k8s/stack`` when verified but cache missing.
 * @returns {Promise<Record<string, unknown> | null>}
 */
async function fetchStackJsonIfVerified() {
  if (!sessionVerified) return null;
  let st = stackFromSessionStorage();
  if (st && st.ok === true && st.components && typeof st.components === "object") return st;
  try {
    const j = await fetchJson("/api/k8s/stack");
    if (j && j.ok === true && j.components && typeof j.components === "object") {
      try {
        sessionStorage.setItem("gb_sts_stack", JSON.stringify(j));
      } catch {
        /* ignore */
      }
      return j;
    }
  } catch {
    /* ignore */
  }
  return st;
}

/**
 * Gray out nav links when probe reports 0 replicas for that component (stack known).
 * @param {Record<string, unknown> | null} st
 */
function applyWorkloadNavAvailability(st) {
  for (const [el, compKey] of WORKLOAD_NAV_STACK_KEYS) {
    if (!el) continue;
    const known = !!(st && st.ok === true && st.components && typeof st.components === "object");
    let rep = 0;
    if (known) {
      const entry = /** @type {Record<string, unknown>} */ (st.components)[compKey];
      rep =
        entry && typeof entry === "object" && typeof entry.replicas === "number" && Number.isFinite(entry.replicas)
          ? entry.replicas
          : 0;
    }
    const disabled = known && rep <= 0;
    if (disabled) {
      el.classList.add("nav-disabled");
      el.setAttribute("aria-disabled", "true");
      el.setAttribute("tabindex", "-1");
      el.title = "No pods for this workload in the connected namespace.";
    } else {
      el.classList.remove("nav-disabled");
      el.removeAttribute("aria-disabled");
      el.removeAttribute("tabindex");
      el.removeAttribute("title");
    }
  }
}

function clearWorkloadNavDisabledState() {
  for (const [el] of WORKLOAD_NAV_STACK_KEYS) {
    if (!el) continue;
    el.classList.remove("nav-disabled");
    el.removeAttribute("aria-disabled");
    el.removeAttribute("tabindex");
    el.removeAttribute("title");
  }
}

function hashMatchesRoute(hash, href) {
  if (hash === href) return true;
  if (!href || href === "#/") return false;
  return hash.startsWith(`${href}/`) || hash.startsWith(`${href}?`);
}

function isCurrentHashWorkloadDisabled() {
  const h = location.hash || "#/";
  const pairs = [
    ["#/kafka-connect", navKafkaConnect],
    ["#/kafka", navKafka],
    ["#/elasticsearch", navElasticsearch],
    ["#/opensearch", navOpensearch],
    ["#/clickhouse", navClickhouse],
    ["#/postgres", navPostgres],
    ["#/cassandra", navCassandra],
  ];
  for (const [prefix, el] of pairs) {
    if (hashMatchesRoute(h, prefix)) {
      return !!(el && el.classList.contains("nav-disabled"));
    }
  }
  return false;
}

async function refreshK8sStrip() {
  try {
    const j = await fetchJson("/api/k8s/context");
    if (j.ok) {
      k8sContextEl.textContent = j.current_context || "(empty)";
      k8sContextEl.classList.remove("status-bad");
      k8sContextEl.classList.add("status-ok");
      if (k8sAwsEl) {
        const bits = [];
        if (j.cloud) bits.push(String(j.cloud).toUpperCase());
        if (j.aws_profile) bits.push(`AWS_PROFILE: ${j.aws_profile}`);
        k8sAwsEl.textContent = bits.join(" · ");
      }
    } else {
      k8sContextEl.textContent = j.kubectl_stderr || "kubectl error";
      k8sContextEl.classList.add("status-bad");
      k8sContextEl.classList.remove("status-ok");
      if (k8sAwsEl) k8sAwsEl.textContent = "";
    }
  } catch (e) {
    k8sContextEl.textContent = String(e.message || e);
    k8sContextEl.classList.add("status-bad");
    if (k8sAwsEl) k8sAwsEl.textContent = "";
  }
}

async function refreshSessionUi() {
  try {
    const s = await fetchJson("/api/session/status");
    sessionVerified = !!s.verified;
    workloadLogContainers = s.workload_log_containers && typeof s.workload_log_containers === "object" ? s.workload_log_containers : null;
    if (sessionVerified && s.session) {
      navKafka.hidden = false;
      if (navKafkaConnect) navKafkaConnect.hidden = false;
      if (navClickhouse) navClickhouse.hidden = false;
      if (navElasticsearch) navElasticsearch.hidden = false;
      if (navOpensearch) navOpensearch.hidden = false;
      if (navPostgres) navPostgres.hidden = false;
      if (navCassandra) navCassandra.hidden = false;
      const ap = s.session.p ? ` · AWS ${s.session.p}` : "";
      const reg = s.session.r ? ` · ${s.session.r}` : "";
      const cl = s.session.d ? ` · ${String(s.session.d).toUpperCase()}` : "";
      footerSession.textContent = `Session: namespace ${s.session.n} · match “${s.session.c}”${cl}${reg}${ap}`;
      const st = await fetchStackJsonIfVerified();
      applyWorkloadNavAvailability(st);
      if (isCurrentHashWorkloadDisabled()) {
        location.hash = "#/";
      }
    } else {
      navKafka.hidden = true;
      if (navKafkaConnect) navKafkaConnect.hidden = true;
      if (navClickhouse) navClickhouse.hidden = true;
      if (navElasticsearch) navElasticsearch.hidden = true;
      if (navOpensearch) navOpensearch.hidden = true;
      if (navPostgres) navPostgres.hidden = true;
      if (navCassandra) navCassandra.hidden = true;
      clearWorkloadNavDisabledState();
      footerSession.textContent = "Session: not connected";
    }
  } catch {
    sessionVerified = false;
    navKafka.hidden = true;
    if (navKafkaConnect) navKafkaConnect.hidden = true;
    if (navClickhouse) navClickhouse.hidden = true;
    if (navElasticsearch) navElasticsearch.hidden = true;
    if (navOpensearch) navOpensearch.hidden = true;
    if (navPostgres) navPostgres.hidden = true;
    if (navCassandra) navCassandra.hidden = true;
    clearWorkloadNavDisabledState();
    footerSession.textContent = "Session: unknown";
  }
}

function setActiveNav() {
  const h = location.hash || "#/";
  document.querySelectorAll(".nav a[data-route]").forEach((a) => {
    const href = a.getAttribute("href") || "";
    if (hashMatchesRoute(h, href)) {
      a.classList.add("nav-active");
    } else {
      a.classList.remove("nav-active");
    }
  });
}

/** @param {{ index: number, variant: "topic" | "group" | "es_index" }} pick */
function kafkaPickCellInner(cell, ci, pick) {
  if (!pick || pick.index !== ci) return esc(cell == null ? "" : String(cell));
  const enc = encodeURIComponent(String(cell ?? ""));
  if (pick.variant === "es_index") {
    return `<button type="button" class="cell-link js-es-pick-index" data-name="${enc}" title="${esc(
      "Use this value as the index pattern",
    )}">${esc(cell == null ? "" : String(cell))}</button>`;
  }
  const cls = pick.variant === "group" ? "js-kafka-pick-group" : "js-kafka-pick-topic";
  const label = pick.variant === "group" ? "Use this consumer group" : "Use this topic for describe / offsets";
  return `<button type="button" class="cell-link ${cls}" data-name="${enc}" title="${esc(label)}">${esc(
    cell == null ? "" : String(cell),
  )}</button>`;
}

/** Rebuild ``<tr>`` from string cells; uses ``data-pick-col`` / ``data-pick-variant`` on ``table`` when set. */
function kafkaDataRowHtml(cells, table) {
  const pickCol = table.getAttribute("data-pick-col");
  const pickVar = table.getAttribute("data-pick-variant");
  const pick =
    pickCol != null && pickCol !== "" && (pickVar === "topic" || pickVar === "group" || pickVar === "es_index")
      ? { index: Number.parseInt(pickCol, 10), variant: /** @type {"topic"|"group"|"es_index"} */ (pickVar) }
      : null;
  return `<tr>${cells
    .map((c, ci) => {
      const inner = kafkaPickCellInner(c, ci, pick);
      return `<td>${inner}</td>`;
    })
    .join("")}</tr>`;
}

function renderTable(rows, opts = {}) {
  if (!rows || !rows.length) return "<p class='muted'>(no rows)</p>";
  const escCell = (c) => esc(c == null ? "" : String(c));
  const tc = opts.tableClass ? ` ${opts.tableClass}` : "";
  const thead = opts.theadFirstRow === true;
  const sortCols = Array.isArray(opts.sortableColumnIndexes) ? opts.sortableColumnIndexes : [];
  const strSortCols = Array.isArray(opts.stringSortableColumnIndexes) ? opts.stringSortableColumnIndexes : [];
  const pick = opts.pickColumn || null;
  let pickAttr = "";
  if (pick) {
    pickAttr = ` data-pick-col="${pick.index}" data-pick-variant="${pick.variant}"`;
  }
  let html = `<table class="data-table${tc}"${pickAttr}>`;
  let start = 0;
  if (thead) {
    html += "<thead><tr>";
    const hdr = rows[0];
    for (let ci = 0; ci < hdr.length; ci++) {
      const sortPart = opts.sortablePartitions && ci === 0;
      const sortCol = sortCols.includes(ci);
      const strSortCol = strSortCols.includes(ci);
      if (sortPart) {
        html += `<th class="th-sortable" data-sort-partitions title="Click to sort by partition count">${escCell(hdr[ci])}</th>`;
      } else if (sortCol) {
        html += `<th class="th-sortable" data-sort-col="${ci}" data-sort-kind="number" title="Click to sort asc/desc">${escCell(hdr[ci])}</th>`;
      } else if (strSortCol) {
        html += `<th class="th-sortable" data-sort-col="${ci}" data-sort-kind="string" title="Click to sort asc/desc">${escCell(hdr[ci])}</th>`;
      } else {
        html += `<th>${escCell(hdr[ci])}</th>`;
      }
    }
    html += "</tr></thead><tbody>";
    start = 1;
  } else {
    html += "<tbody>";
  }
  for (let r = start; r < rows.length; r++) {
    const row = rows[r];
    html += "<tr>";
    for (let ci = 0; ci < row.length; ci++) {
      const cell = row[ci];
      let cls = "";
      if (opts.lagColClasses && opts.lagColClasses[ci]) cls = ` class="${opts.lagColClasses[ci]}"`;
      html += `<td${cls}>${kafkaPickCellInner(cell, ci, pick)}</td>`;
    }
    html += "</tr>";
  }
  html += "</tbody></table>";
  return html;
}

/** Fixed-height viewport (~30 rows) with its own scrollbar — only the data table, not cmd hint / raw pre. */
function kafkaTopicDataTableScroll(tableHtml) {
  return `<div class="kafka-topic-table-scroll" tabindex="0" role="region" aria-label="Topic data table">${tableHtml}</div>`;
}

function wireTopicsPartitionSort(elOut) {
  const th = elOut.querySelector("[data-sort-partitions]");
  const table = elOut.querySelector("table");
  if (!th || !table) return;
  let mode = "none";
  th.addEventListener("click", () => {
    const tbody = table.querySelector("tbody");
    if (!tbody) return;
    const dataRows = [...tbody.querySelectorAll("tr")].map((tr) =>
      [...tr.querySelectorAll("td")].map((td) => td.textContent || ""),
    );
    mode = mode === "none" || mode === "asc" ? "desc" : "asc";
    th.title = mode === "desc" ? "Sorted: partitions high → low" : "Sorted: partitions low → high";
    dataRows.sort((a, b) => {
      const na = Number.parseInt(a[0], 10) || 0;
      const nb = Number.parseInt(b[0], 10) || 0;
      return mode === "desc" ? nb - na : na - nb;
    });
    tbody.innerHTML = dataRows.map((cells) => kafkaDataRowHtml(cells, table)).join("");
  });
}

/** Rebalancing panel: one row per topic, checkbox column for topics-to-move JSON. */
function renderRebalanceTopicTableScroll(table) {
  if (!table || table.length < 2) {
    return "<p class='muted'>(no topic sizes parsed)</p>";
  }
  let html =
    '<div class="kafka-topic-table-scroll" tabindex="0" role="region" aria-label="Topic size table"><table class="data-table kafka-rebalance-du-table"><thead><tr>';
  html += '<th class="kafka-rebalance-th-check" scope="col">Rebalance</th>';
  html += `<th>${esc(String(table[0][0]))}</th><th>${esc(String(table[0][1]))}</th></tr></thead><tbody>`;
  for (let i = 1; i < table.length; i++) {
    const row = table[i];
    const topic = row[0] == null ? "" : String(row[0]);
    const sizeMb = row[1] == null ? "" : String(row[1]);
    const al = `Rebalance ${topic}`;
    html += `<tr><td class="kafka-rebalance-td-check"><input type="checkbox" class="js-rebalance-pick" data-topic-enc="${encodeURIComponent(
      topic,
    )}" title="Include in topics-to-move JSON" aria-label="${esc(al)}" /></td>`;
    html += `<td>${esc(topic)}</td><td>${esc(sizeMb)}</td></tr>`;
  }
  html += "</tbody></table></div>";
  return html;
}

function wireKafkaTableHeaderSort(tableRoot) {
  const table = tableRoot?.querySelector?.("table") || tableRoot;
  if (!table || table.tagName !== "TABLE") return;
  const ths = table.querySelectorAll("th[data-sort-col]");
  ths.forEach((th) => {
    let mode = "none";
    th.addEventListener("click", () => {
      const col = Number.parseInt(th.getAttribute("data-sort-col") || "-1", 10);
      const kind = th.getAttribute("data-sort-kind") || "number";
      const tbody = table.querySelector("tbody");
      if (!tbody || col < 0) return;
      const dataRows = [...tbody.querySelectorAll("tr")].map((tr) =>
        [...tr.querySelectorAll("td")].map((td) => (td.textContent || "").trim()),
      );
      mode = mode === "none" || mode === "asc" ? "desc" : "asc";
      if (kind === "string") {
        th.title = mode === "desc" ? "Sorted: Z → A" : "Sorted: A → Z";
      } else {
        th.title = mode === "desc" ? "Sorted: high → low" : "Sorted: low → high";
      }
      ths.forEach((o) => {
        if (o !== th) o.title = "Click to sort asc/desc";
      });
      dataRows.sort((a, b) => {
        const sa = String(a[col] ?? "");
        const sb = String(b[col] ?? "");
        if (kind === "string") {
          const cmp = sa.localeCompare(sb, undefined, { sensitivity: "base" });
          return mode === "desc" ? -cmp : cmp;
        }
        const na = Number.parseFloat(sa.replace(/,/g, ""));
        const nb = Number.parseFloat(sb.replace(/,/g, ""));
        const va = Number.isFinite(na) ? na : 0;
        const vb = Number.isFinite(nb) ? nb : 0;
        return mode === "desc" ? vb - va : va - vb;
      });
      tbody.innerHTML = dataRows.map((cells) => kafkaDataRowHtml(cells, table)).join("");
    });
  });
}


/** @returns {Promise<boolean>} */
async function loadPanel(panelId, elOut, group, topic, fetchOpts = {}) {
  const u = new URL("/api/kafka/panel", location.origin);
  u.searchParams.set("panel", panelId);
  if (group) u.searchParams.set("group", group);
  if (topic) u.searchParams.set("topic", topic);
  if (panelId === "topics_partition_counts") {
    const mn = fetchOpts.topicsMin;
    if (mn != null && String(mn).trim() !== "") u.searchParams.set("topics_min", String(mn));
    const ts = fetchOpts.topicsSubstring;
    if (ts) u.searchParams.set("topics_substring", ts);
  }
  elOut.innerHTML = "<p class='muted'>Loading…</p>";
  try {
    const j = await fetchJson(u.toString());
    if (!j.ok) {
      const blob = [j.error || "failed", j.stderr && `stderr:\n${j.stderr}`, j.stdout && `stdout:\n${j.stdout}`]
        .filter(Boolean)
        .join("\n\n");
      elOut.innerHTML = `<p class='status-bad'>${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(blob)}`;
      bindCopyButtons(elOut);
      return false;
    }
    let inner = "";
    if (j.cmd_hint) {
      inner += `<p class="panel-cmd-hint panel-cmd-hint--rich">${formatKafkaCmdHint(j.cmd_hint, topic, group)}</p>`;
    }
    if (j.table_summary && j.table_summary.length) {
      inner += `<p class="hint">Topic summary</p>${renderTable(j.table_summary, {
        theadFirstRow: true,
        tableClass: "kafka-describe-summary-table",
      })}`;
    }
    if (j.table && j.table.length) {
      if (panelId === "topic_describe") {
        inner += `<p class="hint">Partitions</p>${kafkaTopicDataTableScroll(
          renderTable(j.table, {
            theadFirstRow: true,
            tableClass: "kafka-describe-partitions-table",
            sortableColumnIndexes: [1, 2],
          }),
        )}`;
      } else if (panelId === "group_lag" && j.table_style === "kafka_lag_describe") {
        const lagCols = ["", "", "", "", "", "col-lag", "col-consumer", "", "col-client"];
        inner += renderTable(j.table, {
          theadFirstRow: true,
          tableClass: "kafka-lag-table",
          lagColClasses: lagCols,
        });
      } else if (panelId === "topic_offsets_end") {
        inner += kafkaTopicDataTableScroll(
          renderTable(j.table, {
            theadFirstRow: true,
            sortablePartitions: false,
          }),
        );
      } else if (panelId === "topics_partition_counts") {
        inner += renderTable(j.table, {
          theadFirstRow: true,
          sortablePartitions: true,
          pickColumn: { index: 1, variant: "topic" },
        });
      } else if (panelId === "consumer_groups_list") {
        const hdr = j.table && j.table[0];
        const hasSizeCol = hdr && hdr.length >= 2;
        inner += renderTable(j.table, {
          theadFirstRow: true,
          tableClass: "kafka-consumer-groups-table",
          pickColumn: { index: 0, variant: "group" },
          stringSortableColumnIndexes: hasSizeCol ? [] : [0],
          sortableColumnIndexes: hasSizeCol ? [1] : [],
        });
      } else {
        inner += renderTable(j.table, { theadFirstRow: true });
      }
    }
    const blobOut = [j.stdout || "", j.stderr && `--- stderr ---\n${j.stderr}`].filter(Boolean).join("\n\n");
    inner += kafkaPreWithWrapFooter(blobOut, {
      wrapChecked:
        panelId === "group_state" || panelId === "group_members" || panelId === "group_members_verbose",
    });
    inner += `<p class='hint'>exit ${esc(String(j.returncode))}</p>`;
    elOut.innerHTML = inner;
    bindCopyButtons(elOut);
    if (panelId === "group_lag" && j.ok) {
      const pan = elOut.closest(".panel");
      const h2 = pan?.querySelector(".panel-head h2");
      const gn = (group || "").trim();
      if (h2) {
        h2.textContent = gn ? `Group lag (describe) - ${gn}` : "Group lag (describe)";
      }
    }
    if (panelId === "topics_partition_counts") {
      wireTopicsPartitionSort(elOut);
    } else if (panelId === "topic_describe") {
      const wrap = elOut.querySelector(".kafka-topic-table-scroll");
      if (wrap) wireKafkaTableHeaderSort(wrap);
    } else if (panelId === "consumer_groups_list") {
      wireKafkaTableHeaderSort(elOut);
    }
    return true;
  } catch (e) {
    const d = e.detail || {};
    const blob = JSON.stringify(d, null, 2);
    elOut.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>${kafkaPreWithWrapFooter(blob)}`;
    bindCopyButtons(elOut);
    return false;
  }
}

/**
 * @param {{ spanAll?: boolean, topicsFilters?: boolean, extraPanelClass?: string, actionButtonText?: string, beforeBodyHtml?: string }} opts
 */
function panelCard(title, bodyElId, opts = {}) {
  const span = opts.spanAll ? " panel--span-all" : "";
  const extra = opts.extraPanelClass ? ` ${opts.extraPanelClass}` : "";
  const actionLabel = opts.actionButtonText || "Refresh";
  const beforeBody = typeof opts.beforeBodyHtml === "string" ? opts.beforeBodyHtml : "";
  let filters = "";
  if (opts.topicsFilters) {
    filters = `
      <div class="panel-toolbar">
        <label class="field field--inline">
          <span>Partitions &gt;</span>
          <input class="text text-narrow" id="kafka-topics-min" type="number" min="0" step="1" placeholder="0" />
        </label>
        <label class="field field--inline">
          <span>Topic contains</span>
          <input class="text" id="kafka-topics-substring" type="text" placeholder="substring" maxlength="200" autocomplete="off" />
        </label>
      </div>`;
  }
  return `
    <section class="panel${span}${extra}">
      <div class="panel-head">
        <h2>${esc(title)}</h2>
        <div class="panel-actions">
          <button type="button" class="btn btn-primary" data-refresh="${esc(bodyElId)}">${esc(actionLabel)}</button>
        </div>
      </div>
      ${filters}
      ${beforeBody}
      <div id="${esc(bodyElId)}"></div>
    </section>
  `;
}

/** Index pattern fields for Elasticsearch / OpenSearch pages (``idPx`` = ``es`` or ``os``). */
function getSearchIndexPatternValue(idPx) {
  const a = document.getElementById(`${idPx}-index-pattern`)?.value?.trim() ?? "";
  if (a) return a;
  return document.getElementById(`${idPx}-index-pattern-shards`)?.value?.trim() ?? "";
}

function setSearchIndexPatternBoth(idPx, value) {
  const v = String(value ?? "");
  const a = document.getElementById(`${idPx}-index-pattern`);
  const b = document.getElementById(`${idPx}-index-pattern-shards`);
  if (a) a.value = v;
  if (b) b.value = v;
}

function wireSearchIndexPatternSync(idPx) {
  const a = document.getElementById(`${idPx}-index-pattern`);
  const b = document.getElementById(`${idPx}-index-pattern-shards`);
  if (!a || !b) return;
  const syncA = () => {
    b.value = a.value;
  };
  const syncB = () => {
    a.value = b.value;
  };
  a.addEventListener("input", syncA);
  b.addEventListener("input", syncB);
}

/**
 * @param {{ needsGroup?: boolean, needsTopic?: boolean, getFetchOpts?: () => Record<string, string>, switchToRefreshAfterOk?: boolean }} opts
 */
function wirePanelRefresh(root, bodyElId, panelId, getGroup, getTopic, opts) {
  const btn = root.querySelector(`[data-refresh="${bodyElId}"]`);
  const out = root.querySelector(`#${bodyElId}`);
  const needsG = !!opts.needsGroup;
  const needsT = !!opts.needsTopic;
  const flip = !!opts.switchToRefreshAfterOk;
  const getFetchOpts = typeof opts.getFetchOpts === "function" ? opts.getFetchOpts : () => ({});
  const actionHint = () => (btn && btn.textContent ? String(btn.textContent) : "Refresh");
  const run = async () => {
    if (needsG && !getGroup()) {
      out.innerHTML = `<p class='muted'>Enter a <strong>consumer group</strong> above, then click <strong>${actionHint()}</strong>.</p>`;
      return;
    }
    if (needsT && !getTopic()) {
      out.innerHTML = `<p class='muted'>Enter a <strong>topic</strong> above, then click <strong>${actionHint()}</strong>.</p>`;
      return;
    }
    const before = btn ? String(btn.textContent) : "";
    const ok = await loadPanel(panelId, out, getGroup(), getTopic(), getFetchOpts());
    if (flip && ok && btn && before === "Activate") btn.textContent = "Refresh";
  };
  btn.addEventListener("click", () => {
    void run();
  });
  if (!needsG && !needsT) {
    void run();
  } else {
    out.innerHTML = `<p class='muted'>Fill group/topic if needed, then click <strong>${actionHint()}</strong>.</p>`;
  }
}

function toggleAwsProfileRow() {
  const cloud = document.getElementById("cloud-select")?.value || "aws";
  const row = document.getElementById("aws-profile-row");
  if (row) row.style.display = cloud === "azure" ? "none" : "";
}

/** Home “List clusters”: only show names ending with ``-core`` (e.g. ``dev-mt-eks-core``). */
function filterCoreClusters(clusters) {
  return (clusters || []).filter((c) => String(c?.name || "").endsWith("-core"));
}

/** @param {unknown} row */
function normalizeHomeClusterRow(row) {
  const r = row && typeof row === "object" ? /** @type {Record<string, unknown>} */ (row) : {};
  return {
    name: String(r.name || ""),
    resourceGroup: String(r.resourceGroup || ""),
    location: String(r.location || ""),
    cloud: r.cloud === "azure" ? "azure" : "aws",
  };
}

/**
 * @param {HTMLSelectElement} cSel
 * @param {Array<Record<string, unknown>>} list
 * @param {HTMLElement | null} listStatus
 * @param {{ restored?: boolean }} [opts]
 */
function populateHomeClusterSelect(cSel, list, listStatus, opts) {
  const restored = !!opts?.restored;
  if (!list.length) {
    cSel.innerHTML = '<option value="">(no *-core clusters in this region)</option>';
    if (listStatus) {
      listStatus.textContent = restored
        ? "Restored list was empty (no *-core clusters)."
        : "No *-core clusters in this region.";
      listStatus.className = "list-clusters-status muted";
    }
    return;
  }
  cSel.innerHTML = '<option value="">— Select a cluster —</option>';
  list.forEach((c, i) => {
    const o = document.createElement("option");
    o.value = String(i);
    const loc = c.location ? ` (${c.location})` : "";
    const rg = c.resourceGroup ? ` [${c.resourceGroup}]` : "";
    o.textContent = `${String(c.cloud).toUpperCase()}: ${c.name}${rg}${loc}`;
    cSel.appendChild(o);
  });
  if (listStatus) {
    listStatus.textContent = restored
      ? `Restored ${list.length} *-core cluster(s) from last list in this tab.`
      : `Listed ${list.length} *-core cluster(s) (others omitted).`;
    listStatus.className = "list-clusters-status list-clusters-status--ok";
  }
}

function readHomeClusterListSession() {
  try {
    const t = sessionStorage.getItem(HOME_CLUSTER_LIST_KEY);
    if (!t) return null;
    const o = JSON.parse(t);
    if (!o || typeof o !== "object") return null;
    if (typeof o.cloud !== "string" || typeof o.region !== "string") return null;
    if (!Array.isArray(o.clusters)) return null;
    return {
      cloud: o.cloud,
      region: o.region,
      aws_profile: typeof o.aws_profile === "string" ? o.aws_profile : "",
      clusters: o.clusters.map(normalizeHomeClusterRow),
      selectedName: typeof o.selectedName === "string" ? o.selectedName : "",
    };
  } catch {
    return null;
  }
}

/**
 * @param {string} cloud
 * @param {string} region
 * @param {string} aws_profile
 * @param {Array<Record<string, unknown>>} list
 * @param {string} selectedName
 */
function saveHomeClusterListSession(cloud, region, aws_profile, list, selectedName) {
  try {
    sessionStorage.setItem(
      HOME_CLUSTER_LIST_KEY,
      JSON.stringify({
        cloud,
        region,
        aws_profile,
        clusters: list.map(normalizeHomeClusterRow),
        selectedName: selectedName || "",
      }),
    );
  } catch {
    /* ignore quota / private mode */
  }
}

function persistHomeClusterSelectedNameFromUi() {
  const cloudSel = document.getElementById("cloud-select");
  const regionSel = document.getElementById("region-select");
  const prSel = document.getElementById("aws-profile");
  const cache = readHomeClusterListSession();
  if (!cache || !cloudSel || !regionSel || !prSel) return;
  if (cache.cloud !== cloudSel.value || cache.region !== regionSel.value || cache.aws_profile !== prSel.value.trim()) return;
  const idxRaw = document.getElementById("cluster-select")?.value;
  const idx = idxRaw === "" || idxRaw == null ? NaN : Number.parseInt(String(idxRaw), 10);
  const name = !Number.isNaN(idx) && clustersList[idx] ? String(clustersList[idx].name || "") : "";
  saveHomeClusterListSession(cache.cloud, cache.region, cache.aws_profile, clustersList, name);
}

function tryRestoreHomeClusterListFromSession() {
  const cloudSel = document.getElementById("cloud-select");
  const regionSel = document.getElementById("region-select");
  const prSel = document.getElementById("aws-profile");
  const cSel = document.getElementById("cluster-select");
  const listStatus = document.getElementById("list-clusters-status");
  if (!cloudSel || !regionSel || !prSel || !cSel) return;
  const cache = readHomeClusterListSession();
  if (!cache) return;
  if (cache.cloud !== cloudSel.value || cache.region !== regionSel.value || cache.aws_profile !== prSel.value.trim()) return;
  clustersList = cache.clusters;
  populateHomeClusterSelect(/** @type {HTMLSelectElement} */ (cSel), clustersList, listStatus, { restored: true });
  if (cache.selectedName) {
    const ix = clustersList.findIndex((c) => c.name === cache.selectedName);
    if (ix >= 0) cSel.value = String(ix);
  }
}

function buildConnectPayload() {
  const region = document.getElementById("region-select")?.value?.trim();
  const idxRaw = document.getElementById("cluster-select")?.value;
  const idx = idxRaw === "" || idxRaw == null ? NaN : Number.parseInt(String(idxRaw), 10);
  if (!Number.isNaN(idx) && clustersList[idx]) {
    const c = clustersList[idx];
    if (c.cloud === "aws") {
      return { cloud: "aws", region, aws_cluster_name: c.name };
    }
    return {
      cloud: "azure",
      region,
      azure_resource_group: c.resourceGroup,
      azure_cluster_name: c.name,
    };
  }
  return null;
}

function renderHome() {
  app.innerHTML = `
    <div class="callout">
      <strong>Read-only by default.</strong> Choose cloud and region, list clusters, select one (or type an EKS name), then connect.
      Last successful connection is saved under <code>data/sensitive/last_connected.json</code> (gitignored) as defaults.
      On <strong>AWS</strong>, the server loads <code>aws_access_key_id</code> / secret / session token from <code>~/.aws/credentials</code> for the selected profile before <strong>List clusters</strong> or <strong>Connect &amp; verify</strong> (same effect as exporting them in your shell).
    </div>
    <section class="panel">
      <h2>Connect to cluster</h2>
      <div class="form-grid form-grid-wide" style="margin-top:1rem;">
        <label class="field">
          <span>Cloud</span>
          <select class="text" id="cloud-select"></select>
        </label>
        <label class="field">
          <span>Region</span>
          <select class="text" id="region-select"></select>
        </label>
        <label class="field" id="aws-profile-row">
          <span>AWS profile (preset list; EKS only)</span>
          <select class="text" id="aws-profile"><option value="">Loading…</option></select>
        </label>
      </div>
      <p style="margin-top:1rem;">
        <button type="button" class="btn" id="btn-list-clusters">List clusters</button>
      </p>
      <p id="list-clusters-status" class="list-clusters-status muted" aria-live="polite"></p>
      <label class="field" style="margin-top:0.75rem;">
        <span>Cluster from list</span>
        <select class="text" id="cluster-select"><option value="">— Click “List clusters” first —</option></select>
      </label>
      <label class="field">
        <span>Or type EKS cluster name (AWS only; runs update-kubeconfig if list not used)</span>
        <input class="text" id="cluster-typed" type="text" placeholder="e.g. dev-mt-eks-core" autocomplete="off" />
      </label>
      <label class="field">
        <span>Namespace (optional — auto-discovered if empty)</span>
        <input class="text" id="namespace-hint" type="text" placeholder="default" autocomplete="off" />
      </label>
      <p class="hint" id="aws-profiles-meta"></p>
      <ul id="aws-expiry-list" class="aws-expiry-list muted" hidden aria-label="AWS session expiry from credentials file"></ul>
      <p style="margin-top:1rem;">
        <button type="button" class="btn btn-primary" id="btn-verify">Connect &amp; verify</button>
        <button type="button" class="btn" id="btn-clear-session">Clear session</button>
      </p>
      <div id="verify-out-wrap" class="verify-out-wrap" hidden></div>
    </section>
  `;
  const cloudSel = app.querySelector("#cloud-select");
  const regionSel = app.querySelector("#region-select");
  const prSel = app.querySelector("#aws-profile");
  const metaEl = app.querySelector("#aws-profiles-meta");
  const outWrap = app.querySelector("#verify-out-wrap");

  (async () => {
    let def = {};
    try {
      def = await fetchJson("/api/k8s/defaults");
    } catch (_) {
      def = { cloud: "aws", region: "eu-west-1", regions: [], clouds: ["aws", "azure"] };
    }
    cloudSel.innerHTML = "";
    for (const c of def.clouds || ["aws", "azure"]) {
      const o = document.createElement("option");
      o.value = c;
      o.textContent = c.toUpperCase();
      cloudSel.appendChild(o);
    }
    cloudSel.value = def.cloud === "azure" ? "azure" : "aws";
    regionSel.innerHTML = "";
    for (const r of def.regions || []) {
      const o = document.createElement("option");
      o.value = r;
      o.textContent = r;
      regionSel.appendChild(o);
    }
    if (def.region) regionSel.value = def.region;
    try {
      const pr = await fetchJson("/api/aws/profiles");
      const hints = /** @type {Record<string, { aws_expiration?: string, remaining_label?: string, expires_at_utc?: string }>} */ (
        pr.session_hints || {}
      );
      prSel.innerHTML = "";
      const optNone = document.createElement("option");
      optNone.value = "";
      optNone.textContent = "(none) shell default AWS_PROFILE";
      prSel.appendChild(optNone);
      for (const name of pr.profiles || []) {
        const o = document.createElement("option");
        o.value = name;
        const h = hints[name];
        if (h && h.remaining_label) {
          o.textContent = `${name} — ${h.remaining_label}`;
          o.title = h.aws_expiration ? `aws_expiration ${h.aws_expiration}` : name;
        } else {
          o.textContent = name;
        }
        prSel.appendChild(o);
      }
      const dprof = pr.default_profile || "glassbox_AWS_MT";
      if (def.aws_profile && (pr.profiles || []).includes(def.aws_profile)) prSel.value = def.aws_profile;
      else if ((pr.profiles || []).includes(dprof)) prSel.value = dprof;
      const credPath = pr.meta?.credentials_path || "~/.aws/credentials";
      metaEl.textContent = `AWS profiles: built-in list · session times from aws_expiration in ${credPath} · defaults: data/sensitive/last_connected.json`;
      const expUl = document.getElementById("aws-expiry-list");
      if (expUl) {
        const entries = Object.keys(hints).sort((a, b) => a.localeCompare(b));
        if (entries.length) {
          expUl.hidden = false;
          expUl.innerHTML = entries
            .map((name) => {
              const hi = hints[name];
              const lab = hi?.remaining_label ? esc(String(hi.remaining_label)) : "";
              const exp = hi?.aws_expiration ? esc(String(hi.aws_expiration)) : "";
              return `<li><strong>${esc(name)}</strong> · ${lab}${exp ? ` · <code>${exp}</code>` : ""}</li>`;
            })
            .join("");
        } else {
          expUl.hidden = true;
          expUl.innerHTML = "";
        }
      }
    } catch (e) {
      prSel.innerHTML = `<option value="">${esc(e.message || "profiles")}</option>`;
      const expUl = document.getElementById("aws-expiry-list");
      if (expUl) {
        expUl.hidden = true;
        expUl.innerHTML = "";
      }
    }
    document.getElementById("namespace-hint").value = def.namespace || "";
    document.getElementById("cluster-typed").value = def.aws_cluster_name || "";
    toggleAwsProfileRow();
    tryRestoreHomeClusterListFromSession();
  })();

  cloudSel.addEventListener("change", toggleAwsProfileRow);

  app.querySelector("#cluster-select")?.addEventListener("change", () => {
    const typed = app.querySelector("#cluster-typed");
    if (typed) typed.value = "";
    persistHomeClusterSelectedNameFromUi();
  });

  const listStatus = app.querySelector("#list-clusters-status");
  app.querySelector("#btn-list-clusters").addEventListener("click", async () => {
    const cloud = cloudSel.value;
    const region = regionSel.value;
    const aws_profile = prSel.value.trim();
    const cSel = app.querySelector("#cluster-select");
    if (listStatus) {
      listStatus.textContent = "Loading clusters…";
      listStatus.className = "list-clusters-status muted";
    }
    cSel.innerHTML = "<option value=\"\">Loading…</option>";
    clustersList = [];
    try {
      const j = await fetchJson("/api/clusters/list", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cloud, region, aws_profile }),
      });
      clustersList = filterCoreClusters(j.clusters || []);
      populateHomeClusterSelect(/** @type {HTMLSelectElement} */ (cSel), clustersList, listStatus);
      let selName = "";
      const prev = readHomeClusterListSession();
      if (
        prev &&
        prev.cloud === cloud &&
        prev.region === region &&
        prev.aws_profile === aws_profile &&
        prev.selectedName &&
        clustersList.some((c) => String(c.name) === prev.selectedName)
      ) {
        selName = prev.selectedName;
      }
      if (selName) {
        const ix = clustersList.findIndex((c) => String(c.name) === selName);
        if (ix >= 0) cSel.value = String(ix);
      }
      saveHomeClusterListSession(cloud, region, aws_profile, clustersList, selName);
    } catch (e) {
      cSel.innerHTML = `<option value="">${esc(e.message || "list failed")}</option>`;
      if (listStatus) {
        listStatus.textContent = e.message || "List clusters failed.";
        listStatus.className = "list-clusters-status list-clusters-status--bad";
      }
    }
  });

  app.querySelector("#btn-verify").addEventListener("click", async () => {
    const namespace = app.querySelector("#namespace-hint").value.trim();
    const aws_profile = prSel.value.trim();
    const cloud = cloudSel.value;
    const region = regionSel.value;
    const typed = app.querySelector("#cluster-typed").value.trim();
    const connect = buildConnectPayload();
    outWrap.hidden = false;
    outWrap.classList.remove("verify-out-wrap--ok", "verify-out-wrap--bad");
    outWrap.innerHTML = `<p class="verify-status verify-status--pending">Connecting…</p>`;
    const body = {
      namespace,
      aws_profile,
      cloud,
      region,
      connect: connect || undefined,
    };
    if (!connect && typed) {
      body.cluster_name = typed;
    }
    try {
      const j = await fetchJson("/api/k8s/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (j.stack) {
        try {
          sessionStorage.setItem("gb_sts_stack", JSON.stringify(j.stack));
        } catch {
          /* ignore */
        }
      }
      const blob = JSON.stringify(j, null, 2);
      const pwdHint = j.clickhouse_password_configured
        ? ""
        : "<p class='hint'>ClickHouse: add <code>data/sensitive/.ch_password</code> (single line, gitignored) for the ClickHouse page.</p>";
      outWrap.classList.add("verify-out-wrap--ok");
      outWrap.innerHTML = `<p class="verify-status verify-status--ok">Connected — namespace <code>${esc(
        j.namespace || "",
      )}</code></p>${pwdHint}${preWithCopy(blob)}`;
      bindCopyButtons(outWrap);
      await refreshSessionUi();
      await refreshK8sStrip();
    } catch (e) {
      const d = e.detail || { error: e.message };
      outWrap.classList.add("verify-out-wrap--bad");
      const msg = esc(String(d.error || e.message || "failed"));
      outWrap.innerHTML = `<p class="verify-status verify-status--bad">${msg}</p>${preWithCopy(JSON.stringify(d, null, 2))}`;
      bindCopyButtons(outWrap);
    }
  });

  app.querySelector("#btn-clear-session").addEventListener("click", async () => {
    try {
      await fetchJson("/api/session/clear", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    } catch (_) {
      /* ignore */
    }
    try {
      sessionStorage.removeItem("gb_sts_stack");
    } catch {
      /* ignore */
    }
    outWrap.hidden = true;
    outWrap.innerHTML = "";
    await refreshSessionUi();
  });
}

function formatExecPlainHelper(j) {
  if (!j || typeof j !== "object") return String(j);
  const o = /** @type {Record<string, unknown>} */ (j);
  const lines = [];
  if (o.error) lines.push(`error: ${o.error}`);
  if (o.needs_confirmation) lines.push("needs_confirmation: true");
  if (o.stdout != null && String(o.stdout)) lines.push(`--- stdout ---\n${o.stdout}`);
  if (o.stderr != null && String(o.stderr)) lines.push(`--- stderr ---\n${o.stderr}`);
  if (o.returncode != null) lines.push(`returncode: ${o.returncode}`);
  if (o.cmd_display) lines.push(`cmd: ${o.cmd_display}`);
  const rest = { ...o };
  for (const k of ["ok", "error", "needs_confirmation", "stdout", "stderr", "returncode", "cmd_display"]) delete rest[k];
  const keys = Object.keys(rest);
  if (keys.length) lines.push(`--- other ---\n${JSON.stringify(rest, null, 2)}`);
  return lines.filter(Boolean).join("\n\n") || JSON.stringify(o, null, 2);
}

async function loadChPanel(panelId, elOut, usesEventTable) {
  const u = new URL("/api/clickhouse/panel", location.origin);
  u.searchParams.set("panel", panelId);
  u.searchParams.set("ch_pod", document.getElementById("ch-pod-select")?.value ?? "0");
  u.searchParams.set("hours", document.getElementById("ch-hours")?.value ?? "24");
  const fromTs = document.getElementById("ch-from-ts")?.value?.trim() ?? "";
  const toTs = document.getElementById("ch-to-ts")?.value?.trim() ?? "";
  if (fromTs) u.searchParams.set("from_ts", fromTs);
  if (toTs) u.searchParams.set("to_ts", toTs);
  const tenant = document.getElementById("ch-tenant-id")?.value?.trim() ?? "";
  if (tenant) u.searchParams.set("tenant_id", tenant);
  if (usesEventTable) {
    u.searchParams.set("event_table", document.getElementById("ch-event-table")?.value ?? "beacon_event");
  }
  if (panelId === "ch_explore_tables") {
    const db = document.getElementById("ch-explore-database")?.value?.trim() ?? "glassbox";
    u.searchParams.set("database", db);
  }
  elOut.innerHTML = "<p class='muted'>Loading…</p>";
  try {
    const j = await fetchJson(u.toString());
    if (!j.ok) {
      const blob = [
        j.error || "failed",
        j.stderr && `stderr:\n${stripAnsiSgr(j.stderr)}`,
        j.stdout && `stdout:\n${stripAnsiSgr(j.stdout)}`,
      ]
        .filter(Boolean)
        .join("\n\n");
      elOut.innerHTML = `<p class='status-bad'>${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(blob, {
        preClass: "output-pre--ch-pretty",
      })}`;
      bindCopyButtons(elOut);
      return;
    }
    let inner = "";
    if (j.cmd_hint) inner += `<p class="panel-cmd-hint">${esc(j.cmd_hint)}</p>`;
    if (j.sql_executed) inner += `<p class="hint ch-sql-hint">${esc(j.sql_executed)}</p>`;
    if (j.table && Array.isArray(j.table) && j.table.length) {
      const cols = Math.max(0, j.table[0].length || 0);
      const strCols = Array.from({ length: cols }, (_, i) => i);
      inner += kafkaTopicDataTableScroll(
        renderTable(j.table, {
          theadFirstRow: true,
          tableClass: "es-cat-table",
          sortableColumnIndexes: [],
          stringSortableColumnIndexes: strCols,
        }),
      );
      const errBlob = j.stderr && String(j.stderr).trim() ? `--- stderr ---\n${stripAnsiSgr(j.stderr)}` : "";
      if (errBlob) inner += kafkaPreWithWrapFooter(errBlob, { preClass: "output-pre--ch-pretty" });
    } else {
      const blobOut = [stripAnsiSgr(j.stdout || ""), j.stderr && `--- stderr ---\n${stripAnsiSgr(j.stderr)}`]
        .filter(Boolean)
        .join("\n\n");
      inner += kafkaPreWithWrapFooter(blobOut, { preClass: "output-pre--ch-pretty" });
    }
    inner += `<p class='hint'>exit ${esc(String(j.returncode))} · pod ${esc(String(j.pod || ""))}</p>`;
    elOut.innerHTML = inner;
    bindCopyButtons(elOut);
    const wrap = elOut.querySelector(".kafka-topic-table-scroll");
    if (wrap) wireKafkaTableHeaderSort(wrap);
  } catch (e) {
    const d = e.detail || {};
    const blob = JSON.stringify(d, null, 2);
    elOut.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>${kafkaPreWithWrapFooter(blob)}`;
    bindCopyButtons(elOut);
  }
}

/**
 * @param {{ usesEventTable?: boolean, needsTenant?: boolean }} opts
 */
function wireChPanelRefresh(root, bodyElId, panelId, opts) {
  const usesEventTable = !!opts.usesEventTable;
  const needsTenant = !!opts.needsTenant;
  const btn = root.querySelector(`[data-refresh="${bodyElId}"]`);
  const out = root.querySelector(`#${bodyElId}`);
  const run = () => {
    if (needsTenant && !document.getElementById("ch-tenant-id")?.value?.trim()) {
      out.innerHTML = "<p class='muted'>Enter a <strong>tenant UUID</strong> in the toolbar, then click Refresh.</p>";
      return;
    }
    loadChPanel(panelId, out, usesEventTable);
  };
  btn.addEventListener("click", run);
  if (needsTenant) {
    out.innerHTML = "<p class='muted'>Enter tenant UUID in the toolbar, then click <strong>Refresh</strong>.</p>";
  } else {
    run();
  }
}

function renderClickhouse() {
  if (!sessionVerified) {
    app.innerHTML =
      "<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The ClickHouse page requires a verified session.</div>";
    return;
  }
  app.innerHTML = `
    <p class="muted">
      Queries run via <code>kubectl exec</code> on <code>glassbox-clickhouse-&lt;n&gt;</code> container <code>glassbox-clickhouse</code> using
      <code>clickhouse-client --password</code> (password from <code>data/sensitive/.ch_password</code>).
      Output uses <code>PrettyCompact</code> formatting; default database for unqualified tables is <code>glassbox</code> (<code>GB_STS_CLICKHOUSE_DATABASE</code> on the tool server).
    </p>
    ${workloadPodStatusPanelHtml("clickhouse")}
    <section class="panel">
      <h2>ClickHouse parameters</h2>
      <div class="ch-toolbar form-grid-wide">
        <label class="field">
          <span>Pod</span>
          <select class="text" id="ch-pod-select"><option value="0">glassbox-clickhouse-0</option></select>
        </label>
        <label class="field">
          <span>Hours back (default window)</span>
          <input class="text text-narrow" id="ch-hours" type="number" min="1" max="336" value="24" />
        </label>
        <label class="field">
          <span>Event table (panels that query events)</span>
          <select class="text" id="ch-event-table">
            <option value="beacon_event">glassbox.beacon_event</option>
            <option value="mobile_event">glassbox.mobile_event</option>
          </select>
        </label>
        <label class="field">
          <span>Tenant UUID (beacon-by-tenant panel)</span>
          <input class="text" id="ch-tenant-id" type="text" placeholder="e95fe0fe-bf52-4b12-9228-ad85947bf58f" autocomplete="off" />
        </label>
        <label class="field">
          <span>From (optional — overrides hours if both set)</span>
          <input class="text" id="ch-from-ts" type="text" placeholder="2026-01-01 00:00:00" autocomplete="off" />
        </label>
        <label class="field">
          <span>To (optional)</span>
          <input class="text" id="ch-to-ts" type="text" placeholder="2026-01-02 00:00:00" autocomplete="off" />
        </label>
        <label class="field">
          <span>Catalog: database (for “Tables in database” panel)</span>
          <input class="text" id="ch-explore-database" type="text" value="glassbox" maxlength="64" autocomplete="off" />
        </label>
      </div>
      <p class="hint">
        Time filter: use <strong>Hours back</strong> for a rolling window (default last 24h), or set <strong>From</strong> and <strong>To</strong> together for an absolute range on <code>session_ts</code>.
      </p>
    </section>
    <div class="panels-grid" id="ch-panels"></div>
    <section class="panel exec-box">
      <h2>Custom ClickHouse SQL</h2>
      <p class="muted">Runs on the selected pod. Write-like SQL requires confirmation.</p>
      <textarea id="ch-exec-sql" spellcheck="false" placeholder="SELECT 1"></textarea>
      <p style="margin-top:0.75rem;">
        <button type="button" class="btn btn-primary" id="ch-exec-run">Run</button>
        <button type="button" class="btn btn-danger" id="ch-exec-run-confirmed" hidden>Run with confirmation</button>
      </p>
      <div id="ch-exec-out-wrap" class="exec-out-wrap" hidden>
        <div id="ch-exec-out-inner"></div>
        <div class="panel-output-footer">
          <label class="exec-json-chk"><input type="checkbox" id="ch-exec-json" /> json</label>
        </div>
      </div>
    </section>
    ${workloadLogsPanelHtml("clickhouse", "ClickHouse logs")}
  `;
  const grid = app.querySelector("#ch-panels");
  /** @type {Array<[string, string, string, boolean, boolean]>} */
  const chPanels = [
    ["Databases (system.databases)", "ch_explore_databases", "ch-p-db", false, false],
    ["Tables in database (system.tables)", "ch_explore_tables", "ch-p-tbl", false, false],
    ["Max session_ts", "ch_max_session_ts", "ch-p-max", true, false],
    ["Events & sessions by hour", "ch_events_sessions_by_hour", "ch-p-ev", true, false],
    ["system.clusters (analytics)", "ch_system_clusters", "ch-p-cl", false, false],
    ["system.processes", "ch_system_processes", "ch-p-pr", false, false],
    ["Beacon by tenant / host / hour", "ch_beacon_tenant_hour", "ch-p-ten", true, true],
    ["Mobile events by hour (mobile_event)", "ch_mobile_by_hour", "ch-p-mob", true, false],
    ["Beacon events by hour", "ch_beacon_by_hour", "ch-p-bh", true, false],
  ];
  let html = "";
  for (const row of chPanels) {
    const [title, , bid] = row;
    html += panelCard(title, bid);
  }
  grid.innerHTML = html;
  for (const row of chPanels) {
    const [title, pid, bid, usesEventTable, needsTenant] = row;
    void title;
    wireChPanelRefresh(grid, bid, pid, { usesEventTable, needsTenant });
  }

  (async () => {
    let stack = null;
    try {
      stack = JSON.parse(sessionStorage.getItem("gb_sts_stack") || "null");
    } catch {
      stack = null;
    }
    if (!stack || !stack.components) {
      try {
        stack = await fetchJson("/api/k8s/stack");
      } catch {
        stack = null;
      }
    }
    const rep = stack?.components?.clickhouse?.replicas;
    const sel = document.getElementById("ch-pod-select");
    if (!sel) return;
    const n = typeof rep === "number" && rep > 0 ? rep : 1;
    sel.innerHTML = "";
    for (let i = 0; i < n; i++) {
      const o = document.createElement("option");
      o.value = String(i);
      o.textContent = `glassbox-clickhouse-${i}`;
      sel.appendChild(o);
    }
  })();

  const chExecWrap = app.querySelector("#ch-exec-out-wrap");
  const chExecInner = app.querySelector("#ch-exec-out-inner");
  const chExecJson = app.querySelector("#ch-exec-json");
  /** @type {unknown} */
  let lastChExecPayload = null;

  function formatChExecPlain(j) {
    return formatExecPlainHelper(j);
  }

  function renderChExecOut(j) {
    if (!chExecInner) return;
    const asJson = chExecJson && chExecJson.checked;
    if (asJson) {
      chExecInner.innerHTML = kafkaPreWithWrapFooter(JSON.stringify(j, null, 2));
      bindCopyButtons(chExecInner);
      return;
    }
    if (j && typeof j === "object" && j.ok && j.stdout != null && String(j.stdout).trim()) {
      const out = stripAnsiSgr(String(j.stdout));
      let foot = `<p class="hint muted">exit ${esc(String(j.returncode ?? ""))}`;
      if (j.pod) foot += ` · pod ${esc(String(j.pod))}`;
      foot += "</p>";
      if (j.stderr != null && String(j.stderr).trim()) {
        foot += `<p class="hint">${esc(stripAnsiSgr(String(j.stderr)))}</p>`;
      }
      chExecInner.innerHTML = `${kafkaPreWithWrapFooter(out, { preClass: "output-pre--ch-pretty" })}${foot}`;
      bindCopyButtons(chExecInner);
      return;
    }
    chExecInner.innerHTML = kafkaPreWithWrapFooter(formatChExecPlain(j));
    bindCopyButtons(chExecInner);
  }

  const chConfBtn = app.querySelector("#ch-exec-run-confirmed");
  async function runChExec(confirmed) {
    const sql = app.querySelector("#ch-exec-sql")?.value?.trim() ?? "";
    const ch_pod = Number.parseInt(String(document.getElementById("ch-pod-select")?.value ?? "0"), 10) || 0;
    chExecWrap.hidden = false;
    if (chExecInner) chExecInner.innerHTML = "<p class='muted'>Running…</p>";
    if (chConfBtn) chConfBtn.hidden = true;
    try {
      const j = await fetchJson("/api/clickhouse/exec", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql, ch_pod, confirmed }),
      });
      lastChExecPayload = j;
      if (j.needs_confirmation) {
        renderChExecOut(j);
        if (chConfBtn) chConfBtn.hidden = false;
        return;
      }
      renderChExecOut(j);
    } catch (e) {
      const payload = e.detail || { error: e.message };
      lastChExecPayload = payload;
      if (e.status === 409 && e.detail && e.detail.needs_confirmation) {
        renderChExecOut(e.detail);
        if (chConfBtn) chConfBtn.hidden = false;
        return;
      }
      renderChExecOut(payload);
    }
  }

  if (chExecJson) {
    chExecJson.addEventListener("change", () => {
      if (lastChExecPayload != null) renderChExecOut(lastChExecPayload);
    });
  }
  app.querySelector("#ch-exec-run").addEventListener("click", () => runChExec(false));
  app.querySelector("#ch-exec-run-confirmed").addEventListener("click", () => {
    const ok = window.confirm(
      "The server flagged this SQL as potentially write-oriented.\n\nRun it anyway? Only proceed if you intend to mutate ClickHouse data.",
    );
    if (ok) runChExec(true);
  });
  wireWorkloadPodStatus(app, "clickhouse");
  wireWorkloadLogs(app, "clickhouse");
}

/** @param {SearchPageCfg} cfg */
function appendSearchEngineToolbarParams(cfg, u) {
  const px = cfg.idPx;
  u.searchParams.set(cfg.qsPod, document.getElementById(`${px}-pod-select`)?.value ?? "0");
  u.searchParams.set("idx_health", document.querySelector(`input[name="${px}-idx-health"]:checked`)?.value ?? "all");
  u.searchParams.set("idx_state", document.querySelector(`input[name="${px}-idx-state"]:checked`)?.value ?? "all");
  const sub = document.getElementById(`${px}-idx-substring`)?.value?.trim() ?? "";
  if (sub) u.searchParams.set("idx_substring", sub);
  const shardSub = document.getElementById(`${px}-shards-substring`)?.value?.trim() ?? "";
  if (shardSub) u.searchParams.set("shards_substring", shardSub);
  const alloc = document.getElementById(`${px}-alloc-body`)?.value ?? "{}";
  u.searchParams.set("alloc_body", alloc);
  const ip = getSearchIndexPatternValue(px);
  if (ip) u.searchParams.set("index_pattern", ip);
}

/**
 * @param {SearchPageCfg} cfg
 * @returns {Promise<boolean>}
 */
async function loadSearchEnginePanel(cfg, panelId, elOut) {
  const u = new URL(cfg.apiPanel, location.origin);
  u.searchParams.set("panel", panelId);
  appendSearchEngineToolbarParams(cfg, u);
  const indexPat = getSearchIndexPatternValue(cfg.idPx);
  elOut.innerHTML = "<p class='muted'>Loading…</p>";
  const pfx = cfg.panelPx;
  try {
    const j = await fetchJson(u.toString());
    if (!j.ok) {
      const blob = [j.error || "failed", j.stderr && `stderr:\n${j.stderr}`, j.stdout && `stdout:\n${j.stdout}`]
        .filter(Boolean)
        .join("\n\n");
      elOut.innerHTML = `<p class='status-bad'>${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(blob)}`;
      bindCopyButtons(elOut);
      return false;
    }
    let inner = "";
    if (j.cmd_hint) {
      inner += `<p class="panel-cmd-hint panel-cmd-hint--rich">${formatEsIndexHighlight(j.cmd_hint, indexPat)}</p>`;
    }
    if (j.path_executed) {
      inner += `<p class="hint ch-sql-hint es-path-hint">${formatEsIndexHighlight(
        `${String(j.http_method || "GET")} ${String(j.path_executed)}`,
        indexPat,
      )}</p>`;
    }
    if (j.table && j.table.length) {
      const pickIdx =
        panelId === `${pfx}cat_indices` ||
        panelId === `${pfx}cat_indices_named` ||
        panelId === `${pfx}cat_shards` ||
        panelId === `${pfx}cat_shards_named`
          ? { index: 0, variant: "es_index" }
          : null;
      const sortNum =
        panelId === `${pfx}cat_indices` || panelId === `${pfx}cat_indices_named`
          ? [3, 4]
          : panelId === `${pfx}cat_shards` || panelId === `${pfx}cat_shards_named`
            ? [4, 5]
            : [];
      const strIdx = panelId === `${pfx}cat_shards` || panelId === `${pfx}cat_shards_named` ? [0, 1] : [];
      inner += kafkaTopicDataTableScroll(
        renderTable(j.table, {
          theadFirstRow: true,
          tableClass: "es-cat-table",
          pickColumn: pickIdx || undefined,
          sortableColumnIndexes: sortNum,
          stringSortableColumnIndexes: strIdx.length ? strIdx : [],
        }),
      );
    }
    const blobOut = [j.stdout || "", j.stderr && `--- stderr ---\n${j.stderr}`].filter(Boolean).join("\n\n");
    inner += kafkaPreWithWrapFooter(blobOut);
    inner += `<p class='hint'>HTTP via pod ${esc(String(j.pod || ""))}</p>`;
    elOut.innerHTML = inner;
    bindCopyButtons(elOut);
    if (
      [cfg.panelPx + "cat_indices", cfg.panelPx + "cat_indices_named", cfg.panelPx + "cat_shards", cfg.panelPx + "cat_shards_named"].includes(
        panelId,
      )
    ) {
      const wrap = elOut.querySelector(".kafka-topic-table-scroll");
      if (wrap) wireKafkaTableHeaderSort(wrap);
    }
    return true;
  } catch (e) {
    const d = e.detail || {};
    const blob = JSON.stringify(d, null, 2);
    elOut.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>${kafkaPreWithWrapFooter(blob)}`;
    bindCopyButtons(elOut);
    return false;
  }
}

/**
 * @param {SearchPageCfg} cfg
 * @param {{ needsIndexPattern?: boolean, skipInitialLoad?: boolean, switchToRefreshAfterOk?: boolean }} opts
 */
function wireSearchEnginePanelRefresh(cfg, root, bodyElId, panelId, opts = {}) {
  const needsIndex = !!opts.needsIndexPattern;
  const skipInitial = !!opts.skipInitialLoad;
  const flip = !!opts.switchToRefreshAfterOk;
  const btn = root.querySelector(`[data-refresh="${bodyElId}"]`);
  const out = root.querySelector(`#${bodyElId}`);
  if (!btn || !out) return;
  const actionHint = () => (btn.textContent ? String(btn.textContent) : "Refresh");
  const run = async () => {
    if (needsIndex && !getSearchIndexPatternValue(cfg.idPx)) {
      out.innerHTML = `<p class='muted'>Enter an <strong>index pattern</strong> (Indices toolbar or Cat shards panel), then click <strong>${actionHint()}</strong>.</p>`;
      return;
    }
    const before = String(btn.textContent);
    const ok = await loadSearchEnginePanel(cfg, panelId, out);
    if (flip && ok && before === "Activate") btn.textContent = "Refresh";
  };
  btn.addEventListener("click", () => void run());
  if (skipInitial) {
    out.innerHTML = `<p class='muted'>Set options in the <strong>Indices</strong> or <strong>Shards</strong> toolbar if needed, then click <strong>Activate</strong>.</p>`;
  } else {
    void run();
  }
}

/**
 * @param {"elasticsearch" | "opensearch"} engineKey
 */
function renderSearchEnginePage(engineKey) {
  const cfg = SEARCH_PAGE_CFGS[engineKey];
  if (!cfg) return;
  const px = cfg.idPx;
  const p = cfg.panelPx;

  if (!sessionVerified) {
    app.innerHTML = `<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The ${esc(
      cfg.pageName,
    )} page requires a verified session.</div>`;
    return;
  }

  app.innerHTML = `
    <p class="muted">
      Uses <code>kubectl exec</code> on <code>${esc(cfg.podStem)}-&lt;n&gt;</code> container <code>${esc(cfg.containerLabel)}</code> (override with env <code>${esc(
    cfg.containerEnv,
  )}</code> if your chart differs)
      and <code>curl</code> to <code>http://127.0.0.1:9200</code> — same HTTP API style as Elasticsearch; no port-forward.
    </p>
    ${workloadPodStatusPanelHtml(cfg.stackComp)}
    <section class="panel">
      <h2>${esc(cfg.pageName)} parameters</h2>
      <div class="ch-toolbar form-grid-wide">
        <label class="field">
          <span>Pod</span>
          <select class="text" id="${px}-pod-select"><option value="0">${esc(cfg.podStem)}-0</option></select>
        </label>
      </div>
    </section>

    <section class="kafka-subject" aria-labelledby="${px}-subject-cluster">
      <h2 class="kafka-subject-title" id="${px}-subject-cluster">Cluster</h2>
      <div class="panels-grid" id="${px}-grid-cluster"></div>
    </section>

    <section class="kafka-subject" aria-labelledby="${px}-subject-indices">
      <h2 class="kafka-subject-title" id="${px}-subject-indices">Indices</h2>
      <section class="panel es-indices-toolbar-panel">
        <h3 class="es-subject-toolbar-title">Cat indices — filters</h3>
        <p class="hint" style="margin-top:0;">Applied to <strong>Cat indices (filtered JSON)</strong> and recovery table row filter. Red rows are unhealthy indices.</p>
        <div class="es-radio-row">
          <span class="es-radio-label">Health:</span>
          <label><input type="radio" name="${px}-idx-health" value="all" checked /> all</label>
          <label><input type="radio" name="${px}-idx-health" value="green" /> green</label>
          <label><input type="radio" name="${px}-idx-health" value="yellow" /> yellow</label>
          <label><input type="radio" name="${px}-idx-health" value="red" /> red</label>
        </div>
        <div class="es-radio-row">
          <span class="es-radio-label">State:</span>
          <label><input type="radio" name="${px}-idx-state" value="all" checked /> all</label>
          <label><input type="radio" name="${px}-idx-state" value="open" /> open</label>
          <label><input type="radio" name="${px}-idx-state" value="close" /> closed</label>
        </div>
        <label class="field" style="margin-top:0.5rem;">
          <span>Index name contains</span>
          <input class="text" id="${px}-idx-substring" type="text" placeholder="substring or * pattern fragment" maxlength="200" autocomplete="off" />
        </label>
        <label class="field" style="margin-top:0.75rem;">
          <span>Index pattern (named indices, highlights, custom GET)</span>
          <input class="text" id="${px}-index-pattern" type="text" placeholder="glassbox_singledim_agguser_week_2915" autocomplete="off" />
        </label>
      </section>
      <div class="panels-grid" id="${px}-grid-indices"></div>
    </section>

    <section class="kafka-subject" aria-labelledby="${px}-subject-shards">
      <h2 class="kafka-subject-title" id="${px}-subject-shards">Shards</h2>
      <section class="panel es-shards-toolbar-panel">
        <h3 class="es-subject-toolbar-title">Shard tables — row filter</h3>
        <p class="hint">Optional substring match on shard / recovery / shard-store table rows (server-side, case-insensitive).</p>
        <label class="field">
          <span>Shard row contains</span>
          <input class="text" id="${px}-shards-substring" type="text" placeholder="substring" maxlength="200" autocomplete="off" />
        </label>
      </section>
      <div class="panels-grid" id="${px}-grid-shards"></div>
    </section>

    <section class="kafka-subject" aria-labelledby="${px}-subject-stats">
      <h2 class="kafka-subject-title" id="${px}-subject-stats">Stats</h2>
      <div class="panels-grid" id="${px}-grid-stats"></div>
    </section>

    <section class="kafka-subject" aria-labelledby="${px}-subject-others">
      <h2 class="kafka-subject-title" id="${px}-subject-others">Others</h2>
      <div class="panels-grid" id="${px}-grid-others"></div>
    </section>

    <section class="panel exec-box">
      <h2>Custom GET path</h2>
      <p class="muted">Read-only GET only. Path must start with <code>/</code>. Some paths require confirmation if not on the conservative allowlist.</p>
      <label class="field">
        <span>Path and query (e.g. <code>/_cluster/health?pretty</code>)</span>
        <input class="text" id="${px}-exec-path" type="text" placeholder="/_cluster/health?pretty" autocomplete="off" />
      </label>
      <p style="margin-top:0.75rem;">
        <button type="button" class="btn btn-primary" id="${px}-exec-run">Run GET</button>
        <button type="button" class="btn btn-danger" id="${px}-exec-run-confirmed" hidden>Run with confirmation</button>
      </p>
      <div id="${px}-exec-out-wrap" class="exec-out-wrap" hidden>
        <div id="${px}-exec-out-inner"></div>
        <div class="panel-output-footer">
          <label class="exec-json-chk"><input type="checkbox" id="${px}-exec-json" /> json</label>
        </div>
      </div>
    </section>
    ${workloadLogsPanelHtml(cfg.workloadLogs, `${cfg.pageName} logs`)}
  `;

  /**
   * @param {HTMLElement | null} grid
   * @param {Array<[string, string, string]>} rows
   * @param {Record<string, { needsIndexPattern?: boolean, skipInitialLoad?: boolean, switchToRefreshAfterOk?: boolean }>} refreshOpts
   * @param {(pid: string, bid: string) => Record<string, string>} [getCardExtras]
   */
  function fillSearchSubjectGrid(grid, rows, refreshOpts, getCardExtras) {
    if (!grid) return;
    let html = "";
    for (const row of rows) {
      const [title, pid, bid] = row;
      const ro = refreshOpts[pid] || {};
      const useActivate = !!(ro.skipInitialLoad && ro.switchToRefreshAfterOk);
      const extras = typeof getCardExtras === "function" ? getCardExtras(pid, bid) : {};
      html += panelCard(title, bid, { actionButtonText: useActivate ? "Activate" : "Refresh", ...extras });
    }
    grid.innerHTML = html;
    for (const row of rows) {
      const [, pid, bid] = row;
      wireSearchEnginePanelRefresh(cfg, grid, bid, pid, refreshOpts[pid] || {});
    }
  }

  const allocBodyHtml = `
      <label class="field" style="margin-top:0;">
        <span>Allocation explain JSON body (<code>POST /_cluster/allocation/explain</code>)</span>
        <textarea id="${px}-alloc-body" spellcheck="false" rows="3" class="text" style="font-family:var(--mono); min-height:4rem;">{}</textarea>
      </label>`;
  const catShardsToolbarHtml = `
      <div class="panel-toolbar es-cat-shards-toolbar">
        <label class="field">
          <span>Index pattern (shard-by-index, highlights)</span>
          <input class="text" id="${px}-index-pattern-shards" type="text" placeholder="Synced with Indices toolbar" autocomplete="off" />
        </label>
      </div>`;

  const clusterPanels = [
    ["Cluster root (version)", `${p}root`, `${px}-p-root`],
    ["Cluster health", `${p}cluster_health`, `${px}-p-health`],
    ["Cat nodes", `${p}cat_nodes`, `${px}-p-nodes`],
    ["Cluster settings", `${p}cluster_settings`, `${px}-p-set`],
    ["Cat allocation", `${p}cat_allocation`, `${px}-p-alloc`],
    ["Allocation explain (POST)", `${p}allocation_explain`, `${px}-p-expl`],
  ];
  const indicesPanels = [
    ["Cat indices (filtered JSON)", `${p}cat_indices`, `${px}-p-idx`],
    ["Cat indices / named index", `${p}cat_indices_named`, `${px}-p-idxn`],
    ["Cat recovery (JSON)", `${p}cat_recovery`, `${px}-p-rec`],
  ];
  const shardsPanels = [
    ["Cat shards", `${p}cat_shards`, `${px}-p-shards`],
    ["Cat shard stores (JSON)", `${p}cat_shard_stores`, `${px}-p-sstores`],
    ["Cluster health (shard level)", `${p}cluster_health_shards`, `${px}-p-chs`],
    ["Cat shards / index pattern", `${p}cat_shards_named`, `${px}-p-shn`],
  ];
  const statsPanels = [
    ["Cluster stats", `${p}cluster_stats`, `${px}-p-cstats`],
    ["Nodes stats", `${p}nodes_stats`, `${px}-p-nstats`],
    ["Cat pending tasks", `${p}cat_pending_tasks`, `${px}-p-cpt`],
    ["Cluster pending tasks", `${p}cluster_pending_tasks`, `${px}-p-cpnd`],
    ["Cat thread pool", `${p}cat_thread_pool`, `${px}-p-tp`],
    ["ILM status", `${p}ilm_status`, `${px}-p-ilm`],
  ];
  const othersPanels = [
    ["Cat templates", `${p}cat_templates`, `${px}-p-tpl`],
    ["Cat aliases", `${p}cat_aliases`, `${px}-p-al`],
    ["Cluster state (metadata slice)", `${p}cluster_state_metadata`, `${px}-p-csm`],
    ["Nodes info", `${p}nodes_info`, `${px}-p-ni`],
  ];

  const clusterRefresh = {
    [`${p}allocation_explain`]: { skipInitialLoad: true, switchToRefreshAfterOk: true },
  };
  const indicesRefresh = {
    [`${p}cat_indices`]: { skipInitialLoad: true, switchToRefreshAfterOk: true },
    [`${p}cat_indices_named`]: { needsIndexPattern: true, skipInitialLoad: true, switchToRefreshAfterOk: true },
    [`${p}cat_recovery`]: {},
  };
  const shardsRefresh = {
    [`${p}cat_shards_named`]: { needsIndexPattern: true, skipInitialLoad: true, switchToRefreshAfterOk: true },
  };

  fillSearchSubjectGrid(app.querySelector(`#${px}-grid-cluster`), clusterPanels, clusterRefresh, (pid, bid) =>
    bid === `${px}-p-expl` ? { beforeBodyHtml: allocBodyHtml } : {},
  );
  fillSearchSubjectGrid(app.querySelector(`#${px}-grid-indices`), indicesPanels, indicesRefresh);
  fillSearchSubjectGrid(app.querySelector(`#${px}-grid-shards`), shardsPanels, shardsRefresh, (pid, bid) =>
    bid === `${px}-p-shards` ? { beforeBodyHtml: catShardsToolbarHtml } : {},
  );
  fillSearchSubjectGrid(app.querySelector(`#${px}-grid-stats`), statsPanels, {});
  fillSearchSubjectGrid(app.querySelector(`#${px}-grid-others`), othersPanels, {});

  app.dataset.searchEngine = px;
  if (!app.dataset.searchIndexPickDelegation) {
    app.dataset.searchIndexPickDelegation = "1";
    app.addEventListener("click", (ev) => {
      const b = ev.target.closest("button.js-es-pick-index");
      if (!b) return;
      const idPx = app.dataset.searchEngine || "es";
      const enc = b.getAttribute("data-name");
      if (enc != null) {
        try {
          setSearchIndexPatternBoth(idPx, decodeURIComponent(enc));
        } catch {
          /* ignore */
        }
      }
    });
  }

  wireSearchIndexPatternSync(px);

  (async () => {
    let stack = null;
    try {
      stack = JSON.parse(sessionStorage.getItem("gb_sts_stack") || "null");
    } catch {
      stack = null;
    }
    if (!stack || !stack.components) {
      try {
        stack = await fetchJson("/api/k8s/stack");
      } catch {
        stack = null;
      }
    }
    const rep = stack?.components?.[cfg.stackComp]?.replicas;
    const sel = document.getElementById(`${px}-pod-select`);
    if (!sel) return;
    const n = typeof rep === "number" && rep > 0 ? rep : 1;
    sel.innerHTML = "";
    for (let i = 0; i < n; i++) {
      const o = document.createElement("option");
      o.value = String(i);
      o.textContent = `${cfg.podStem}-${i}`;
      sel.appendChild(o);
    }
  })();

  const execWrap = app.querySelector(`#${px}-exec-out-wrap`);
  const execInner = app.querySelector(`#${px}-exec-out-inner`);
  const execJson = app.querySelector(`#${px}-exec-json`);
  const confBtn = app.querySelector(`#${px}-exec-run-confirmed`);
  /** @type {unknown} */
  let lastExecPayload = null;

  function renderSearchExecOut(j) {
    if (!execInner) return;
    const asJson = execJson && execJson.checked;
    const ip = getSearchIndexPatternValue(px);
    let head = "";
    if (!asJson && j && typeof j === "object" && j.path_executed) {
      head = `<p class="hint ch-sql-hint es-path-hint">${formatEsIndexHighlight(
        `${String(j.http_method || "GET")} ${String(j.path_executed)}`,
        ip,
      )}</p>`;
    }
    const text = asJson ? JSON.stringify(j, null, 2) : formatExecPlainHelper(j);
    execInner.innerHTML = head + kafkaPreWithWrapFooter(text);
    bindCopyButtons(execInner);
  }

  async function runSearchExec(confirmed) {
    let path = document.getElementById(`${px}-exec-path`)?.value?.trim() ?? "";
    if (!path) {
      if (execInner) execInner.innerHTML = "<p class='status-bad'>Enter a path (e.g. <code>/_cluster/health?pretty</code>).</p>";
      if (execWrap) execWrap.hidden = false;
      return;
    }
    if (!path.startsWith("/")) path = `/${path}`;
    const podVal = Number.parseInt(String(document.getElementById(`${px}-pod-select`)?.value ?? "0"), 10) || 0;
    const body = /** @type {Record<string, unknown>} */ ({ path, method: "GET", confirmed });
    body[cfg.qsPod] = podVal;
    if (execWrap) execWrap.hidden = false;
    if (execInner) execInner.innerHTML = "<p class='muted'>Running…</p>";
    if (confBtn) confBtn.hidden = true;
    try {
      const j = await fetchJson(cfg.apiExec, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      lastExecPayload = j;
      if (j.needs_confirmation) {
        renderSearchExecOut(j);
        if (confBtn) confBtn.hidden = false;
        return;
      }
      renderSearchExecOut(j);
    } catch (e) {
      const payload = e.detail || { error: e.message };
      lastExecPayload = payload;
      if (e.status === 409 && e.detail && e.detail.needs_confirmation) {
        renderSearchExecOut(e.detail);
        if (confBtn) confBtn.hidden = false;
        return;
      }
      renderSearchExecOut(payload);
    }
  }

  if (execJson) {
    execJson.addEventListener("change", () => {
      if (lastExecPayload != null) renderSearchExecOut(lastExecPayload);
    });
  }
  app.querySelector(`#${px}-exec-run`)?.addEventListener("click", () => runSearchExec(false));
  app.querySelector(`#${px}-exec-run-confirmed`)?.addEventListener("click", () => {
    const ok = window.confirm(
      "The server flagged this GET path as potentially sensitive.\n\nRun it anyway? Only proceed if you accept the risk.",
    );
    if (ok) runSearchExec(true);
  });
  wireWorkloadPodStatus(app, cfg.stackComp);
  wireWorkloadLogs(app, cfg.workloadLogs);
}

function renderKafkaConnect() {
  if (!sessionVerified) {
    app.innerHTML =
      "<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The Kafka Connect page requires a verified session.</div>";
    return;
  }
  app.innerHTML = `
    <p class="muted">
      Uses <code>kubectl exec</code> on <code>glassbox-kafkaconnect-&lt;n&gt;</code> container <code>kafka</code> (override with env <code>GB_STS_KAFKA_CONNECT_CONTAINER</code> if your chart differs)
      and <code>curl</code> to <code>http://127.0.0.1:8083</code> inside the pod.
    </p>
    ${workloadPodStatusPanelHtml("kafkaconnect")}
    <section class="panel">
      <h2>Kafka Connect parameters</h2>
      <div class="ch-toolbar form-grid-wide">
        <label class="field">
          <span>Pod</span>
          <select class="text" id="kc-pod-select"><option value="0">glassbox-kafkaconnect-0</option></select>
        </label>
        <label class="field" style="min-width:22rem;">
          <span>Connector name (for connector-specific panels)</span>
          <input class="text" id="kc-connector-name" type="text" placeholder="s3sink-cottages" autocomplete="off" />
        </label>
      </div>
    </section>
    <section class="kafka-subject" aria-labelledby="kc-subject-overview">
      <h2 class="kafka-subject-title" id="kc-subject-overview">Overview</h2>
      <div class="panels-grid" id="kc-grid-overview"></div>
    </section>
    <section class="kafka-subject" aria-labelledby="kc-subject-connector">
      <h2 class="kafka-subject-title" id="kc-subject-connector">Connector details</h2>
      <div class="panels-grid" id="kc-grid-connector"></div>
    </section>
    <section class="kafka-subject" aria-labelledby="kc-subject-infra">
      <h2 class="kafka-subject-title" id="kc-subject-infra">Infra</h2>
      <div class="panels-grid" id="kc-grid-infra"></div>
    </section>
    ${workloadLogsPanelHtml("kafkaconnect", "Kafka Connect logs")}
  `;

  const overviewPanels = [
    ["Root (worker info)", "kc_root", "kc-p-root", false],
    ["Connectors list", "kc_connectors", "kc-p-connectors", false],
    ["Connectors status", "kc_connectors_status", "kc-p-status", false],
    ["Connectors info", "kc_connectors_info", "kc-p-info", false],
    ["Connectors info + status", "kc_connectors_full", "kc-p-full", false],
  ];
  const connectorPanels = [
    ["Connector status", "kc_connector_status", "kc-p-c-status", true],
    ["Connector config", "kc_connector_config", "kc-p-c-config", true],
    ["Connector tasks", "kc_connector_tasks", "kc-p-c-tasks", true],
    ["Connector topics", "kc_connector_topics", "kc-p-c-topics", true],
  ];
  const infraPanels = [
    ["Connector plugins", "kc_plugins", "kc-p-plugins", false],
    ["Admin loggers", "kc_admin_loggers", "kc-p-loggers", false],
  ];

  function panelFilterInputHtml(panelBodyId) {
    return `<div class="panel-toolbar"><label class="field"><span>Filter rows / JSON output</span><input class="text" id="${panelBodyId}-filter" type="text" placeholder="substring" maxlength="200" autocomplete="off" /></label></div>`;
  }

  function fillKcGrid(grid, rows) {
    if (!grid) return;
    const fullWidthOverviewBodies = new Set(["kc-p-status", "kc-p-info", "kc-p-full"]);
    let html = "";
    for (const [title, , bodyId] of rows) {
      html += panelCard(title, bodyId, {
        beforeBodyHtml: panelFilterInputHtml(bodyId),
        spanAll: fullWidthOverviewBodies.has(bodyId),
      });
    }
    grid.innerHTML = html;
  }

  fillKcGrid(app.querySelector("#kc-grid-overview"), overviewPanels);
  fillKcGrid(app.querySelector("#kc-grid-connector"), connectorPanels);
  fillKcGrid(app.querySelector("#kc-grid-infra"), infraPanels);

  function decorateKafkaConnectStatus(rootEl) {
    if (!rootEl) return;
    for (const td of rootEl.querySelectorAll("td")) {
      const raw = (td.textContent || "").trim().toUpperCase();
      td.classList.remove("status-kc-good", "status-kc-warn", "status-kc-bad");
      if (!raw) continue;
      if (raw === "GREEN" || raw.includes("RUNNING")) td.classList.add("status-kc-good");
      else if (raw === "YELLOW" || raw.includes("PAUSED") || raw.includes("UNASSIGNED") || raw.includes("RESTARTING"))
        td.classList.add("status-kc-warn");
      else if (raw === "RED" || raw.includes("FAILED") || raw.includes("ERROR") || raw.includes("DESTROYED"))
        td.classList.add("status-kc-bad");
    }
  }

  async function loadKcPanel(panelId, bodyId, needsConnector) {
    const out = document.getElementById(bodyId);
    if (!out) return;
    const connector = document.getElementById("kc-connector-name")?.value?.trim() ?? "";
    if (needsConnector && !connector) {
      out.innerHTML = "<p class='muted'>Enter a <strong>connector name</strong> above, then click <strong>Refresh</strong>.</p>";
      return;
    }
    const panelFilter = document.getElementById(`${bodyId}-filter`)?.value?.trim() ?? "";
    const kcPod = document.getElementById("kc-pod-select")?.value ?? "0";
    const u = new URL("/api/kafkaconnect/panel", location.origin);
    u.searchParams.set("panel", panelId);
    u.searchParams.set("kc_pod", kcPod);
    if (connector) u.searchParams.set("connector", connector);
    if (panelFilter) u.searchParams.set("panel_filter", panelFilter);
    out.innerHTML = "<p class='muted'>Loading…</p>";
    try {
      const j = await fetchJson(u.toString());
      if (!j.ok) {
        const blob = [j.error || "failed", j.stderr && `stderr:\n${j.stderr}`, j.stdout && `stdout:\n${j.stdout}`]
          .filter(Boolean)
          .join("\n\n");
        out.innerHTML = `<p class='status-bad'>${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(blob)}`;
        bindCopyButtons(out);
        return;
      }
      let inner = "";
      if (j.cmd_hint) inner += `<p class="panel-cmd-hint panel-cmd-hint--rich">${esc(j.cmd_hint)}</p>`;
      if (j.path_executed) inner += `<p class="hint ch-sql-hint">${esc(`${String(j.http_method || "GET")} ${String(j.path_executed)}`)}</p>`;
      if (j.table && j.table.length) {
        const cols = Math.max(0, j.table[0].length || 0);
        const strCols = Array.from({ length: cols }, (_, i) => i);
        inner += kafkaTopicDataTableScroll(
          renderTable(j.table, {
            theadFirstRow: true,
            tableClass: "es-cat-table",
            sortableColumnIndexes: [],
            stringSortableColumnIndexes: strCols,
          }),
        );
      }
      const blobOut = [j.stdout || "", j.stderr && `--- stderr ---\n${j.stderr}`].filter(Boolean).join("\n\n");
      inner += kafkaPreWithWrapFooter(blobOut);
      inner += `<p class='hint'>HTTP via pod ${esc(String(j.pod || ""))}</p>`;
      out.innerHTML = inner;
      bindCopyButtons(out);
      const wrap = out.querySelector(".kafka-topic-table-scroll");
      if (wrap) wireKafkaTableHeaderSort(wrap);
      decorateKafkaConnectStatus(out);
    } catch (e) {
      out.innerHTML = `<p class='status-bad'>${esc(e.message || "failed")}</p>${kafkaPreWithWrapFooter(
        JSON.stringify(e.detail || {}, null, 2),
      )}`;
      bindCopyButtons(out);
    }
  }

  function wireKcPanels(grid, rows) {
    if (!grid) return;
    for (const [, panelId, bodyId, needsConnector] of rows) {
      const btn = grid.querySelector(`[data-refresh="${bodyId}"]`);
      btn?.addEventListener("click", () => {
        void loadKcPanel(panelId, bodyId, needsConnector);
      });
      void loadKcPanel(panelId, bodyId, needsConnector);
    }
  }

  wireKcPanels(app.querySelector("#kc-grid-overview"), overviewPanels);
  wireKcPanels(app.querySelector("#kc-grid-connector"), connectorPanels);
  wireKcPanels(app.querySelector("#kc-grid-infra"), infraPanels);

  (async () => {
    let stack = null;
    try {
      stack = JSON.parse(sessionStorage.getItem("gb_sts_stack") || "null");
    } catch {
      stack = null;
    }
    if (!stack || !stack.components) {
      try {
        stack = await fetchJson("/api/k8s/stack");
      } catch {
        stack = null;
      }
    }
    const rep = stack?.components?.kafkaconnect?.replicas;
    const sel = document.getElementById("kc-pod-select");
    if (!sel) return;
    const n = typeof rep === "number" && rep > 0 ? rep : 1;
    sel.innerHTML = "";
    for (let i = 0; i < n; i++) {
      const o = document.createElement("option");
      o.value = String(i);
      o.textContent = `glassbox-kafkaconnect-${i}`;
      sel.appendChild(o);
    }
  })();

  wireWorkloadPodStatus(app, "kafkaconnect");
  wireWorkloadLogs(app, "kafkaconnect");
}

function renderElasticsearch() {
  renderSearchEnginePage("elasticsearch");
}

function renderOpensearch() {
  renderSearchEnginePage("opensearch");
}

const PG_HISTORY_KEY = "gb_sts_pg_queries_v1";
const CASS_HISTORY_KEY = "gb_sts_cassandra_queries_v1";

/**
 * @param {string} key
 */
function readRotatingQueries(key) {
  try {
    const a = JSON.parse(sessionStorage.getItem(key) || "[]");
    return Array.isArray(a) ? a.filter((x) => typeof x === "string") : [];
  } catch {
    return [];
  }
}

/**
 * @param {string} key
 * @param {string} sql
 */
function pushRotatingQuery(key, sql, max = 10) {
  const s = (sql || "").trim();
  if (!s) return;
  let arr = readRotatingQueries(key);
  arr = arr.filter((x) => x !== s);
  arr.push(s);
  arr = arr.slice(-max);
  sessionStorage.setItem(key, JSON.stringify(arr));
}

function renderPostgres() {
  if (!sessionVerified) {
    app.innerHTML =
      "<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The PostgreSQL page requires a verified session.</div>";
    return;
  }
  app.innerHTML = `
    <p class="muted">
      Default posture is <strong>read-only</strong>: fixed panels run approved <code>SELECT</code>s only.
      <strong>Custom SQL</strong> is classified server-side; mutating statements require explicit confirmation.
      Password for <code>psql -U clarisite -d glassbox</code> comes from <code>data/sensitive/.pg_password</code> (gitignored), same pattern as ClickHouse.
      The default kubectl container name is <code>glassbox-postgresql</code>; for stock Bitnami charts use operator env <code>GB_STS_PG_CONTAINER=postgresql</code> when starting the tool server.
      If exec fails with <code>psql: command not found</code>, set <code>GB_STS_PSQL</code> to the in-pod absolute path to <code>psql</code> (often <code>/opt/bitnami/postgresql/bin/psql</code>) and restart <code>server.py</code>.
    </p>
    ${workloadPodStatusPanelHtml("postgresql")}
    <section class="panel">
      <h2>Pod</h2>
      <p class="muted">Choose the PostgreSQL StatefulSet pod (stack probe lists non-HA and HA chart names).</p>
      <label class="field">
        <span>Pod</span>
        <select class="text" id="pg-pod-select"></select>
      </label>
    </section>
    <section class="panel">
      <h2>Catalog (read-only)</h2>
      <p class="muted">Fixed introspection queries; same <code>kubectl exec</code> path as tables and custom SQL.</p>
      <div class="panel-toolbar kafka-logs-toolbar">
        <button type="button" class="btn btn-primary" id="pg-catalog-databases">List databases</button>
        <button type="button" class="btn btn-primary" id="pg-catalog-schemas">List schemas (current DB)</button>
      </div>
      <div id="pg-catalog-out"><p class="muted">Use the buttons above for <code>pg_database</code> / <code>information_schema.schemata</code>.</p></div>
    </section>
    <section class="panel">
      <h2>Tables (information_schema)</h2>
      <div class="panel-toolbar kafka-logs-toolbar">
        <button type="button" class="btn" id="pg-schema-reload">Reload schema list</button>
        <label class="field field--inline">
          <span>Schema</span>
          <select class="text" id="pg-schema"><option value="">(load schemas first)</option></select>
        </label>
        <button type="button" class="btn btn-primary" id="pg-tables-refresh">List tables</button>
      </div>
      <div id="pg-tables-out"><p class="muted">Load schemas, pick one, then click <strong>List tables</strong>.</p></div>
    </section>
    <section class="panel exec-box">
      <h2>Custom SQL</h2>
      <p class="muted">Successful runs (return code 0, no <code>needs_confirmation</code> pending) rotate into the last-10 list for this browser tab.</p>
      <p class="hint">Recent OK queries (newest first)</p>
      <ul id="pg-query-history" class="query-history-list"></ul>
      <textarea id="pg-exec-sql" spellcheck="false" rows="7" placeholder="SELECT current_database();"></textarea>
      <p style="margin-top:0.75rem;">
        <button type="button" class="btn btn-primary" id="pg-exec-run">Run</button>
        <button type="button" class="btn btn-danger" id="pg-exec-run-confirmed" hidden>Run with confirmation</button>
      </p>
      <div id="pg-exec-out-wrap" class="exec-out-wrap" hidden>
        <div id="pg-exec-out-inner"></div>
        <div class="panel-output-footer">
          <label class="exec-json-chk"><input type="checkbox" id="pg-exec-json" /> json</label>
        </div>
      </div>
    </section>
    ${workloadLogsPanelHtml("postgresql", "PostgreSQL logs")}
  `;

  function renderPgHistoryUi() {
    const ul = document.getElementById("pg-query-history");
    if (!ul) return;
    const items = readRotatingQueries(PG_HISTORY_KEY).slice().reverse();
    ul.innerHTML = items.length
      ? items
          .map(
            (q) =>
              `<li><button type="button" class="btn query-history-btn js-pg-hist" data-q="${esc(
                encodeURIComponent(q),
              )}">${esc(q.length > 140 ? `${q.slice(0, 140)}…` : q)}</button></li>`,
          )
          .join("")
      : "<li class='muted'>(none yet)</li>";
  }

  (async () => {
    let stack = null;
    try {
      stack = JSON.parse(sessionStorage.getItem("gb_sts_stack") || "null");
    } catch {
      stack = null;
    }
    if (!stack?.components) {
      try {
        stack = await fetchJson("/api/k8s/stack");
      } catch {
        stack = null;
      }
    }
    const sel = document.getElementById("pg-pod-select");
    if (!sel) return;
    const pods = stack?.components?.postgresql?.pods;
    sel.innerHTML = "";
    if (Array.isArray(pods) && pods.length) {
      for (const name of pods.slice(0, 16)) {
        const o = document.createElement("option");
        o.value = String(name);
        o.textContent = String(name);
        sel.appendChild(o);
      }
    } else {
      const o = document.createElement("option");
      o.value = "glassbox-postgresql-0";
      o.textContent = "glassbox-postgresql-0";
      sel.appendChild(o);
    }
    await loadPgSchemasIntoSelect();
  })();

  async function loadPgSchemasIntoSelect() {
    const sel = document.getElementById("pg-schema");
    const pg_pod = document.getElementById("pg-pod-select")?.value ?? "";
    if (!sel) return;
    const prev = sel.value;
    const u = new URL("/api/postgres/panel", location.origin);
    u.searchParams.set("panel", "schemas");
    u.searchParams.set("pg_pod", pg_pod);
    try {
      const j = await fetchJson(u.toString());
      if (!j.ok) throw new Error(j.error || j.stderr || "failed");
      const names = String(j.stdout || "")
        .split("\n")
        .map((s) => s.trim())
        .filter(Boolean);
      sel.innerHTML = "";
      for (const n of names) {
        const o = document.createElement("option");
        o.value = n;
        o.textContent = n;
        sel.appendChild(o);
      }
      if (prev && names.includes(prev)) sel.value = prev;
      else if (names.includes("glassbox")) sel.value = "glassbox";
      else if (names.includes("public")) sel.value = "public";
      else if (names.length) sel.selectedIndex = 0;
    } catch {
      sel.innerHTML =
        "<option value='glassbox'>glassbox</option><option value='public'>public</option><option value='clarisite_management'>clarisite_management</option><option value='tenant_management'>tenant_management</option>";
    }
  }

  async function loadPgCatalogPanel(which) {
    const out = document.getElementById("pg-catalog-out");
    const pg_pod = document.getElementById("pg-pod-select")?.value ?? "";
    if (!out) return;
    out.innerHTML = "<p class='muted'>Loading…</p>";
    const u = new URL("/api/postgres/panel", location.origin);
    u.searchParams.set("panel", which);
    u.searchParams.set("pg_pod", pg_pod);
    try {
      const j = await fetchJson(u.toString());
      if (!j.ok) {
        out.innerHTML = `<p class="status-bad">${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(
          [j.stdout, j.stderr].filter(Boolean).join("\n"),
          { preClass: "output-pre--pg-tsv" },
        )}`;
        bindCopyButtons(out);
        return;
      }
      const blob = [j.stdout || "", j.stderr && `stderr:\n${j.stderr}`].filter(Boolean).join("\n\n");
      out.innerHTML = `<p class='hint'>${esc(which)} · pod ${esc(String(j.pod || ""))}</p>${kafkaPreWithWrapFooter(blob, {
        preClass: "output-pre--pg-tsv",
      })}`;
      bindCopyButtons(out);
    } catch (e) {
      out.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>`;
    }
  }

  document.getElementById("pg-pod-select")?.addEventListener("change", () => {
    void loadPgSchemasIntoSelect();
  });
  document.getElementById("pg-schema-reload")?.addEventListener("click", () => void loadPgSchemasIntoSelect());
  document.getElementById("pg-catalog-databases")?.addEventListener("click", () => void loadPgCatalogPanel("databases"));
  document.getElementById("pg-catalog-schemas")?.addEventListener("click", () => void loadPgCatalogPanel("schemas"));

  renderPgHistoryUi();
  document.getElementById("pg-query-history")?.addEventListener("click", (ev) => {
    const b = ev.target.closest("button.js-pg-hist");
    if (!b) return;
    const enc = b.getAttribute("data-q");
    const ta = document.getElementById("pg-exec-sql");
    if (enc && ta) {
      try {
        ta.value = decodeURIComponent(enc);
      } catch {
        /* ignore */
      }
    }
  });

  async function loadPgTables() {
    const out = document.getElementById("pg-tables-out");
    const schema = document.getElementById("pg-schema")?.value?.trim() ?? "";
    const pg_pod = document.getElementById("pg-pod-select")?.value ?? "";
    if (!out) return;
    if (!schema) {
      out.innerHTML = "<p class='status-bad'>Choose a schema (use <strong>Reload schema list</strong> if the list is empty).</p>";
      return;
    }
    out.innerHTML = "<p class='muted'>Loading…</p>";
    const u = new URL("/api/postgres/panel", location.origin);
    u.searchParams.set("panel", "tables");
    u.searchParams.set("schema", schema);
    u.searchParams.set("pg_pod", pg_pod);
    try {
      const j = await fetchJson(u.toString());
      if (!j.ok) {
        out.innerHTML = `<p class="status-bad">${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(
          [j.stdout, j.stderr].filter(Boolean).join("\n"),
          { preClass: "output-pre--pg-tsv" },
        )}`;
        bindCopyButtons(out);
        return;
      }
      const blob = [j.stdout || "", j.stderr && `stderr:\n${j.stderr}`].filter(Boolean).join("\n\n");
      out.innerHTML = `<p class='hint'>pod ${esc(String(j.pod || ""))}</p>${kafkaPreWithWrapFooter(blob, {
        preClass: "output-pre--pg-tsv",
      })}`;
      bindCopyButtons(out);
    } catch (e) {
      const d = e.detail || {};
      out.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>${kafkaPreWithWrapFooter(JSON.stringify(d, null, 2))}`;
      bindCopyButtons(out);
    }
  }

  document.getElementById("pg-tables-refresh")?.addEventListener("click", () => void loadPgTables());

  const pgExecWrap = document.getElementById("pg-exec-out-wrap");
  const pgExecInner = document.getElementById("pg-exec-out-inner");
  const pgExecJson = document.getElementById("pg-exec-json");
  const pgConfBtn = document.getElementById("pg-exec-run-confirmed");
  /** @type {unknown} */
  let lastPgExecPayload = null;

  function renderPgExecOut(j) {
    if (!pgExecInner) return;
    const asJson = pgExecJson && pgExecJson.checked;
    if (asJson) {
      pgExecInner.innerHTML = kafkaPreWithWrapFooter(JSON.stringify(j, null, 2));
      bindCopyButtons(pgExecInner);
      return;
    }
    if (j && typeof j === "object" && j.ok && j.stdout != null && String(j.stdout).trim()) {
      const out = String(j.stdout);
      let foot = `<p class="hint muted">exit ${esc(String(j.returncode ?? ""))}`;
      if (j.pod) foot += ` · pod ${esc(String(j.pod))}`;
      foot += "</p>";
      if (j.stderr != null && String(j.stderr).trim()) {
        foot += `<p class="hint">${esc(String(j.stderr))}</p>`;
      }
      pgExecInner.innerHTML = `${kafkaPreWithWrapFooter(out, { preClass: "output-pre--pg-tsv" })}${foot}`;
      bindCopyButtons(pgExecInner);
      return;
    }
    pgExecInner.innerHTML = kafkaPreWithWrapFooter(formatExecPlainHelper(j));
    bindCopyButtons(pgExecInner);
  }

  async function runPgExec(confirmed) {
    const sql = document.getElementById("pg-exec-sql")?.value?.trim() ?? "";
    const pg_pod = document.getElementById("pg-pod-select")?.value ?? "";
    if (!pgExecWrap || !pgExecInner) return;
    pgExecWrap.hidden = false;
    pgExecInner.innerHTML = "<p class='muted'>Running…</p>";
    if (pgConfBtn) pgConfBtn.hidden = true;
    try {
      const j = await fetchJson("/api/postgres/exec", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql, pg_pod, confirmed }),
      });
      lastPgExecPayload = j;
      if (j.needs_confirmation) {
        renderPgExecOut(j);
        if (pgConfBtn) pgConfBtn.hidden = false;
        return;
      }
      renderPgExecOut(j);
      if (j.ok && sql) {
        pushRotatingQuery(PG_HISTORY_KEY, sql);
        renderPgHistoryUi();
      }
    } catch (e) {
      const payload = e.detail || { error: e.message };
      lastPgExecPayload = payload;
      if (e.status === 409 && e.detail && e.detail.needs_confirmation) {
        renderPgExecOut(e.detail);
        if (pgConfBtn) pgConfBtn.hidden = false;
        return;
      }
      renderPgExecOut(payload);
    }
  }

  pgExecJson?.addEventListener("change", () => {
    if (lastPgExecPayload != null) renderPgExecOut(lastPgExecPayload);
  });
  document.getElementById("pg-exec-run")?.addEventListener("click", () => void runPgExec(false));
  document.getElementById("pg-exec-run-confirmed")?.addEventListener("click", () => {
    const ok = window.confirm(
      "The server flagged this SQL as potentially write-oriented.\n\nRun it anyway? Only proceed if you intend to mutate PostgreSQL data.",
    );
    if (ok) void runPgExec(true);
  });
  wireWorkloadPodStatus(app, "postgresql");
  wireWorkloadLogs(app, "postgresql");
}

function renderCassandra() {
  if (!sessionVerified) {
    app.innerHTML =
      "<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The Cassandra page requires a verified session.</div>";
    return;
  }
  app.innerHTML = `
    <p class="muted">
      Read-only by default: keyspace / table panels run fixed <code>DESCRIBE</code> / <code>SELECT</code> statements.
      <strong>Custom CQL</strong> is restricted and vetted server-side; writes require confirmation.
      The server looks for <code>cqlsh</code> under typical paths (e.g. Bitnami <code>/opt/bitnami/cassandra/bin/cqlsh</code>) before <code>PATH</code>.
      If your image stores it elsewhere, set operator env <code>GB_STS_CQLSH</code> to the <strong>in-pod</strong> absolute path before starting <code>server.py</code>.
      CQL target uses <code>GB_STS_CASSANDRA_CQL_HOST</code> if set, else chart env <code>CASSANDRA_HOST</code> / <code>POD_IP</code> / <code>CASSANDRA_BROADCAST_ADDRESS</code>, else <code>127.0.0.1</code>. Port: <code>GB_STS_CASSANDRA_CQL_PORT</code> or <code>CASSANDRA_CQL_PORT_NUMBER</code> or <code>9042</code>.
    </p>
    ${workloadPodStatusPanelHtml("cassandra")}
    <section class="panel">
      <h2>Pod</h2>
      <label class="field">
        <span>Pod</span>
        <select class="text" id="cass-pod-select"></select>
      </label>
    </section>
    <section class="panel">
      <h2>Keyspaces</h2>
      <button type="button" class="btn btn-primary" id="cass-keyspaces-refresh">Refresh</button>
      <div id="cass-keyspaces-out" style="margin-top:0.75rem;"><p class="muted">Click Refresh for <code>DESCRIBE KEYSPACES</code>.</p></div>
    </section>
    <section class="panel">
      <h2>Describe keyspace</h2>
      <label class="field">
        <span>Keyspace name</span>
        <input class="text" id="cass-desc-ks" type="text" placeholder="glassbox" autocomplete="off" />
      </label>
      <p style="margin-top:0.75rem;"><button type="button" class="btn btn-primary" id="cass-desc-refresh">Describe</button></p>
      <div id="cass-desc-out"><p class="muted">Output appears here.</p></div>
    </section>
    <section class="panel">
      <h2>Tables in keyspace</h2>
      <label class="field">
        <span>Keyspace name</span>
        <input class="text" id="cass-tables-ks" type="text" placeholder="glassbox" autocomplete="off" />
      </label>
      <p style="margin-top:0.75rem;"><button type="button" class="btn btn-primary" id="cass-tables-refresh">List tables</button></p>
      <div id="cass-tables-out"><p class="muted">Runs <code>SELECT table_name FROM system_schema.tables …</code></p></div>
    </section>
    <section class="panel exec-box">
      <h2>Custom CQL</h2>
      <p class="muted">Last 10 successful statements (exit 0) are kept in <code>sessionStorage</code> for this tab.</p>
      <p class="hint">Recent OK CQL (newest first)</p>
      <ul id="cass-query-history" class="query-history-list"></ul>
      <textarea id="cass-exec-cql" spellcheck="false" rows="7" placeholder="DESCRIBE KEYSPACES;"></textarea>
      <p style="margin-top:0.75rem;">
        <button type="button" class="btn btn-primary" id="cass-exec-run">Run</button>
        <button type="button" class="btn btn-danger" id="cass-exec-run-confirmed" hidden>Run with confirmation</button>
      </p>
      <div id="cass-exec-out-wrap" class="exec-out-wrap" hidden>
        <div id="cass-exec-out-inner"></div>
        <div class="panel-output-footer">
          <label class="exec-json-chk"><input type="checkbox" id="cass-exec-json" /> json</label>
        </div>
      </div>
    </section>
    ${workloadLogsPanelHtml("cassandra", "Cassandra logs")}
  `;

  function renderCassHistoryUi() {
    const ul = document.getElementById("cass-query-history");
    if (!ul) return;
    const items = readRotatingQueries(CASS_HISTORY_KEY).slice().reverse();
    ul.innerHTML = items.length
      ? items
          .map(
            (q) =>
              `<li><button type="button" class="btn query-history-btn js-cass-hist" data-q="${esc(
                encodeURIComponent(q),
              )}">${esc(q.length > 140 ? `${q.slice(0, 140)}…` : q)}</button></li>`,
          )
          .join("")
      : "<li class='muted'>(none yet)</li>";
  }

  (async () => {
    let stack = null;
    try {
      stack = JSON.parse(sessionStorage.getItem("gb_sts_stack") || "null");
    } catch {
      stack = null;
    }
    if (!stack?.components) {
      try {
        stack = await fetchJson("/api/k8s/stack");
      } catch {
        stack = null;
      }
    }
    const sel = document.getElementById("cass-pod-select");
    if (!sel) return;
    const pods = stack?.components?.cassandra?.pods;
    sel.innerHTML = "";
    if (Array.isArray(pods) && pods.length) {
      for (const name of pods.slice(0, 16)) {
        const o = document.createElement("option");
        o.value = String(name);
        o.textContent = String(name);
        sel.appendChild(o);
      }
    } else {
      const o = document.createElement("option");
      o.value = "glassbox-cassandra-0";
      o.textContent = "glassbox-cassandra-0";
      sel.appendChild(o);
    }
  })();

  renderCassHistoryUi();
  document.getElementById("cass-query-history")?.addEventListener("click", (ev) => {
    const b = ev.target.closest("button.js-cass-hist");
    if (!b) return;
    const enc = b.getAttribute("data-q");
    const ta = document.getElementById("cass-exec-cql");
    if (enc && ta) {
      try {
        ta.value = decodeURIComponent(enc);
      } catch {
        /* ignore */
      }
    }
  });

  function cassPod() {
    return document.getElementById("cass-pod-select")?.value ?? "";
  }

  async function loadCassPanel(panel, extra = {}) {
    const u = new URL("/api/cassandra/panel", location.origin);
    u.searchParams.set("panel", panel);
    u.searchParams.set("cass_pod", cassPod());
    if (extra.keyspace) u.searchParams.set("keyspace", extra.keyspace);
    return fetchJson(u.toString());
  }

  document.getElementById("cass-keyspaces-refresh")?.addEventListener("click", async () => {
    const out = document.getElementById("cass-keyspaces-out");
    if (!out) return;
    out.innerHTML = "<p class='muted'>Loading…</p>";
    try {
      const j = await loadCassPanel("keyspaces");
      if (!j.ok) {
        out.innerHTML = `<p class="status-bad">${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(
          [j.stdout, j.stderr].filter(Boolean).join("\n"),
        )}`;
        bindCopyButtons(out);
        return;
      }
      const blob = [j.stdout || "", j.stderr && `stderr:\n${j.stderr}`].filter(Boolean).join("\n\n");
      out.innerHTML = `<p class='hint'>${esc(String(j.cql_executed || ""))}</p>${kafkaPreWithWrapFooter(blob)}`;
      bindCopyButtons(out);
    } catch (e) {
      out.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>`;
    }
  });

  document.getElementById("cass-desc-refresh")?.addEventListener("click", async () => {
    const out = document.getElementById("cass-desc-out");
    const ks = document.getElementById("cass-desc-ks")?.value?.trim() ?? "";
    if (!out) return;
    if (!ks) {
      out.innerHTML = "<p class='status-bad'>Enter a keyspace name.</p>";
      return;
    }
    out.innerHTML = "<p class='muted'>Loading…</p>";
    try {
      const j = await loadCassPanel("desc_keyspace", { keyspace: ks });
      if (!j.ok) {
        out.innerHTML = `<p class="status-bad">${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(
          [j.stdout, j.stderr].filter(Boolean).join("\n"),
        )}`;
        bindCopyButtons(out);
        return;
      }
      const blob = [j.stdout || "", j.stderr && `stderr:\n${j.stderr}`].filter(Boolean).join("\n\n");
      out.innerHTML = `<p class='hint'>${esc(String(j.cql_executed || ""))}</p>${kafkaPreWithWrapFooter(blob)}`;
      bindCopyButtons(out);
    } catch (e) {
      out.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>`;
    }
  });

  document.getElementById("cass-tables-refresh")?.addEventListener("click", async () => {
    const out = document.getElementById("cass-tables-out");
    const ks = document.getElementById("cass-tables-ks")?.value?.trim() ?? "";
    if (!out) return;
    if (!ks) {
      out.innerHTML = "<p class='status-bad'>Enter a keyspace name.</p>";
      return;
    }
    out.innerHTML = "<p class='muted'>Loading…</p>";
    try {
      const j = await loadCassPanel("tables", { keyspace: ks });
      if (!j.ok) {
        out.innerHTML = `<p class="status-bad">${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(
          [j.stdout, j.stderr].filter(Boolean).join("\n"),
        )}`;
        bindCopyButtons(out);
        return;
      }
      const blob = [j.stdout || "", j.stderr && `stderr:\n${j.stderr}`].filter(Boolean).join("\n\n");
      out.innerHTML = `<p class='hint'>${esc(String(j.cql_executed || ""))}</p>${kafkaPreWithWrapFooter(blob)}`;
      bindCopyButtons(out);
    } catch (e) {
      out.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>`;
    }
  });

  const cassExecWrap = document.getElementById("cass-exec-out-wrap");
  const cassExecInner = document.getElementById("cass-exec-out-inner");
  const cassExecJson = document.getElementById("cass-exec-json");
  const cassConfBtn = document.getElementById("cass-exec-run-confirmed");
  /** @type {unknown} */
  let lastCassExecPayload = null;

  function renderCassExecOut(j) {
    if (!cassExecInner) return;
    const asJson = cassExecJson && cassExecJson.checked;
    const text = asJson ? JSON.stringify(j, null, 2) : formatExecPlainHelper(j);
    cassExecInner.innerHTML = kafkaPreWithWrapFooter(text);
    bindCopyButtons(cassExecInner);
  }

  async function runCassExec(confirmed) {
    const cql = document.getElementById("cass-exec-cql")?.value?.trim() ?? "";
    if (!cassExecWrap || !cassExecInner) return;
    cassExecWrap.hidden = false;
    cassExecInner.innerHTML = "<p class='muted'>Running…</p>";
    if (cassConfBtn) cassConfBtn.hidden = true;
    try {
      const j = await fetchJson("/api/cassandra/exec", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cql, cass_pod: cassPod(), confirmed }),
      });
      lastCassExecPayload = j;
      if (j.needs_confirmation) {
        renderCassExecOut(j);
        if (cassConfBtn) cassConfBtn.hidden = false;
        return;
      }
      renderCassExecOut(j);
      if (j.ok && cql) {
        pushRotatingQuery(CASS_HISTORY_KEY, cql);
        renderCassHistoryUi();
      }
    } catch (e) {
      const payload = e.detail || { error: e.message };
      lastCassExecPayload = payload;
      if (e.status === 409 && e.detail && e.detail.needs_confirmation) {
        renderCassExecOut(e.detail);
        if (cassConfBtn) cassConfBtn.hidden = false;
        return;
      }
      renderCassExecOut(payload);
    }
  }

  cassExecJson?.addEventListener("change", () => {
    if (lastCassExecPayload != null) renderCassExecOut(lastCassExecPayload);
  });
  document.getElementById("cass-exec-run")?.addEventListener("click", () => void runCassExec(false));
  document.getElementById("cass-exec-run-confirmed")?.addEventListener("click", () => {
    const ok = window.confirm(
      "The server flagged this CQL as potentially write-oriented.\n\nRun it anyway? Only proceed if you intend to mutate Cassandra data.",
    );
    if (ok) void runCassExec(true);
  });
  wireWorkloadPodStatus(app, "cassandra");
  wireWorkloadLogs(app, "cassandra");
}

function renderKafka() {
  if (!sessionVerified) {
    app.innerHTML =
      "<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The Kafka page is hidden until verification succeeds.</div>";
    return;
  }
  app.innerHTML = `
    <p class="muted">Pod <code>glassbox-kafka-0</code> — commands run in the <code>kafka</code> container with a random high <code>JMX_PORT</code> per request to avoid port collisions.</p>
    ${workloadPodStatusPanelHtml("kafka")}

    <section class="kafka-subject" aria-labelledby="kafka-subject-brokers">
      <h2 class="kafka-subject-title" id="kafka-subject-brokers">Brokers</h2>
      <div class="panels-grid" id="kafka-grid-brokers"></div>
    </section>

    <section class="kafka-subject" aria-labelledby="kafka-subject-topics">
      <h2 class="kafka-subject-title" id="kafka-subject-topics">Topics</h2>
      <section class="panel kafka-subject-params">
        <label class="field">
          <span>Topic (for describe / end offsets)</span>
          <input class="text" id="kafka-topic" type="text" placeholder="beacon_offline" autocomplete="off" />
        </label>
      </section>
      <div class="panels-grid" id="kafka-grid-topics"></div>
    </section>

    <section class="kafka-subject" aria-labelledby="kafka-subject-groups">
      <h2 class="kafka-subject-title" id="kafka-subject-groups">Groups</h2>
      <section class="panel kafka-subject-params">
        <label class="field">
          <span>Consumer group (for list pick, lag, state, members)</span>
          <input class="text" id="kafka-group" type="text" placeholder="beacon_offline_group" autocomplete="off" />
        </label>
      </section>
      <div id="kafka-grid-groups"></div>
    </section>

    <section class="panel panel--span-all kafka-rebalance-section">
      <div class="panel-head">
        <h2>Rebalancing</h2>
        <div class="panel-actions">
          <button type="button" class="btn btn-primary" id="rebalance-analyze-btn">Activate</button>
        </div>
      </div>
      <p class="muted">
        Read-only scan of <code>/bitnami/kafka/data/*</code> in the Kafka pod (<code>du -sk</code>); sizes are summed per topic and sorted largest first. Commands are copy-paste only; this UI does not run partition reassignment.
      </p>
      <div class="panel-toolbar kafka-rebalance-toolbar">
        <label class="field field--inline">
          <span>Number of brokers</span>
          <input class="text text-narrow" id="rebalance-broker-count" type="number" min="1" max="32" step="1" value="3" />
        </label>
      </div>
      <p class="muted" id="rebalance-placeholder">Click <strong>Activate</strong> to scan data directories and show helper commands.</p>
      <div id="rebalance-scan-out" hidden></div>
      <div id="rebalance-commands" hidden></div>
    </section>

    ${workloadLogsPanelHtml("kafka", "Kafka logs")}

    <section class="panel exec-box">
      <h2>Custom command (broker shell)</h2>
      <p class="muted">Runs in the <code>kafka</code> container after a random <code>JMX_PORT</code> export. Write-like commands require confirmation.</p>
      <textarea id="exec-cmd" spellcheck="false" placeholder="/opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --list | head"></textarea>
      <p style="margin-top:0.75rem;">
        <button type="button" class="btn btn-primary" id="exec-run">Run</button>
        <button type="button" class="btn btn-danger" id="exec-run-confirmed" hidden>Run with confirmation</button>
      </p>
      <div id="exec-out-wrap" class="exec-out-wrap" hidden>
        <div id="exec-out-inner"></div>
        <div class="panel-output-footer">
          <label class="exec-json-chk"><input type="checkbox" id="exec-json" /> json</label>
        </div>
      </div>
    </section>
  `;
  const gridBrokers = app.querySelector("#kafka-grid-brokers");
  const gridTopics = app.querySelector("#kafka-grid-topics");
  const gridGroups = app.querySelector("#kafka-grid-groups");

  /** @type {Array<[string, string, string, boolean, boolean, boolean?, boolean?]>} */
  const brokerPanels = [
    ["Broker API versions", "broker_versions", "p-broker-ver", false, false],
    ["Broker default configs (describe)", "broker_default_configs", "p-broker-cfg", false, false],
    ["Partition leadership balance", "leader_balance", "p-balance", false, false],
  ];
  const topicPanels = [
    ["Topics × partition counts", "topics_partition_counts", "p-topics", false, false, false, true],
    ["Topic describe (head)", "topic_describe", "p-t-desc", false, true, false, false, "", "activate"],
    ["Topic end offsets (GetOffsetShell)", "topic_offsets_end", "p-t-off", false, true, false, false, "", "activate"],
  ];
  /** Left ~⅓: consumer list only. Right ~⅔: state → members → verbose. Lag: full-width row below. */
  const groupPanelsLeft = [["Consumer groups (list)", "consumer_groups_list", "p-groups", false, false]];
  const groupPanelsLag = [["Group lag (describe)", "group_lag", "p-lag", true, false, true, false, "", "activate"]];
  const groupPanelsRight = [
    ["Group state (--state --verbose)", "group_state", "p-g-state", true, false, false, false, "", "activate"],
    ["Group members", "group_members", "p-g-mem", true, false, false, false, "panel--kafka-group-members-span", "activate"],
    [
      "Group members + partition assignment",
      "group_members_verbose",
      "p-g-memv",
      true,
      false,
      false,
      false,
      "panel--kafka-group-members-span",
      "activate",
    ],
  ];

  /** @param {HTMLElement | null} gridEl */
  function fillKafkaGrid(gridEl, rows) {
    if (!gridEl) return;
    let html = "";
    for (const row of rows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls, panelOpts] = row;
      const cardOpts = {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      };
      if (panelOpts === "activate") cardOpts.actionButtonText = "Activate";
      html += panelCard(title, bid, cardOpts);
    }
    gridEl.innerHTML = html;
  }

  /** @param {HTMLElement | null} rootEl */
  function fillKafkaGroupsLayout(rootEl, leftRows, rightRows, lagRows) {
    if (!rootEl) return;
    let leftHtml = "";
    for (const row of leftRows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls, panelOpts] = row;
      const cardOpts = {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      };
      if (panelOpts === "activate") cardOpts.actionButtonText = "Activate";
      leftHtml += panelCard(title, bid, cardOpts);
    }
    let rightHtml = "";
    for (const row of rightRows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls, panelOpts] = row;
      const cardOpts = {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      };
      if (panelOpts === "activate") cardOpts.actionButtonText = "Activate";
      rightHtml += panelCard(title, bid, cardOpts);
    }
    let lagHtml = "";
    for (const row of lagRows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls, panelOpts] = row;
      const cardOpts = {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      };
      if (panelOpts === "activate") cardOpts.actionButtonText = "Activate";
      lagHtml += panelCard(title, bid, cardOpts);
    }
    rootEl.innerHTML = `<div class="kafka-groups-wrapper">
      <div class="kafka-groups-layout">
        <div class="kafka-groups-left kafka-groups-left-grid">${leftHtml}</div>
        <div class="kafka-groups-right">${rightHtml}</div>
      </div>
      <div class="kafka-groups-lag-row">${lagHtml}</div>
    </div>`;
  }

  fillKafkaGrid(gridBrokers, brokerPanels);
  fillKafkaGrid(gridTopics, topicPanels);
  fillKafkaGroupsLayout(gridGroups, groupPanelsLeft, groupPanelsRight, groupPanelsLag);

  if (!app.dataset.kafkaPickDelegation) {
    app.dataset.kafkaPickDelegation = "1";
    app.addEventListener("click", (ev) => {
      const tp = ev.target.closest("button.js-kafka-pick-topic");
      if (tp) {
        const enc = tp.getAttribute("data-name");
        const inp = document.getElementById("kafka-topic");
        if (inp && enc != null) inp.value = decodeURIComponent(enc);
        return;
      }
      const gp = ev.target.closest("button.js-kafka-pick-group");
      if (gp) {
        const enc = gp.getAttribute("data-name");
        const inp = document.getElementById("kafka-group");
        if (inp && enc != null) inp.value = decodeURIComponent(enc);
      }
    });
  }

  function rebalanceBrokerCountFromStorage() {
    try {
      const st = JSON.parse(sessionStorage.getItem("gb_sts_stack") || "null");
      const r = st?.components?.kafka?.replicas;
      if (typeof r === "number" && r >= 1) return Math.min(32, r);
    } catch {
      /* ignore */
    }
    return 3;
  }

  const rebInp = app.querySelector("#rebalance-broker-count");
  if (rebInp) rebInp.value = String(rebalanceBrokerCountFromStorage());

  let rebalanceBrokerUserTouched = false;
  let rebalanceLastScanOk = false;
  let rebalanceBtnRefreshMode = false;

  function rebalanceTopicsForJson() {
    const scanOut = document.getElementById("rebalance-scan-out");
    if (!scanOut) return [];
    const topics = [];
    scanOut.querySelectorAll(".js-rebalance-pick:checked").forEach((el) => {
      const enc = el.getAttribute("data-topic-enc");
      if (enc) topics.push(decodeURIComponent(enc));
    });
    return topics;
  }

  function rebalanceBrokerListCsv() {
    const raw = document.getElementById("rebalance-broker-count")?.value ?? "3";
    const v = Number.parseInt(String(raw).trim(), 10);
    const n = Number.isFinite(v) ? Math.min(32, Math.max(1, v)) : 3;
    return Array.from({ length: n }, (_, i) => String(i)).join(",");
  }

  function rebalanceCmdWriteJson() {
    const topics = rebalanceTopicsForJson();
    const payload = { version: 1, topics: topics.map((t) => ({ topic: t })) };
    const inner = JSON.stringify(payload);
    return `echo ${JSON.stringify(inner)} > /tmp/topics-to-move.json`;
  }

  function renderRebalanceCommands() {
    const wrap = document.getElementById("rebalance-commands");
    if (!wrap) return;
    wrap.hidden = false;
    const br = rebalanceBrokerListCsv();
    const kb = "/opt/bitnami/kafka/bin/kafka-reassign-partitions.sh";
    const c2 = `${kb} --bootstrap-server 127.0.0.1:9092 --topics-to-move-json-file /tmp/topics-to-move.json --broker-list ${br} --generate`;
    const c2b = `${c2} | tail -1 > /tmp/reassignment_file.json`;
    const c3 = `${kb} --bootstrap-server 127.0.0.1:9092 --reassignment-json-file /tmp/reassignment_file.json --execute`;
    wrap.innerHTML = `<h3 class="hint">1. Write topics JSON</h3>
${preWithCopy(rebalanceCmdWriteJson())}
<h3 class="hint">2. Generate reassignment plan</h3>
${preWithCopy(c2)}
<p class="muted">Capture only the reassignment JSON (last line):</p>
${preWithCopy(c2b)}
<h3 class="hint">3. Execute (review <code>/tmp/reassignment_file.json</code> first)</h3>
${preWithCopy(c3)}`;
    bindCopyButtons(wrap);
  }

  function refreshRebalanceCommandsIfNeeded() {
    if (rebalanceLastScanOk) renderRebalanceCommands();
  }

  rebInp?.addEventListener("input", () => {
    rebalanceBrokerUserTouched = true;
    refreshRebalanceCommandsIfNeeded();
  });

  document.getElementById("rebalance-scan-out")?.addEventListener("change", (ev) => {
    const t = ev.target;
    if (t instanceof HTMLInputElement && t.classList.contains("js-rebalance-pick")) {
      refreshRebalanceCommandsIfNeeded();
    }
  });

  async function runRebalanceAnalyze() {
    const btn = document.getElementById("rebalance-analyze-btn");
    const placeholder = document.getElementById("rebalance-placeholder");
    const scanOut = document.getElementById("rebalance-scan-out");
    const cmdWrap = document.getElementById("rebalance-commands");
    if (!btn || !placeholder || !scanOut || !cmdWrap) return;
    btn.disabled = true;
    scanOut.hidden = false;
    scanOut.innerHTML = "<p class='muted'>Scanning…</p>";
    cmdWrap.hidden = true;
    cmdWrap.innerHTML = "";
    rebalanceLastScanOk = false;
    try {
      const j = await fetchJson("/api/kafka/rebalance/analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      if (!rebalanceBrokerUserTouched && typeof j.default_num_brokers === "number") {
        const inp = document.getElementById("rebalance-broker-count");
        if (inp) inp.value = String(Math.min(32, Math.max(1, j.default_num_brokers)));
      }
      let scanHtml = "";
      if (j.table && j.table.length > 1) {
        scanHtml += `${renderRebalanceTopicTableScroll(j.table)}`;
      } else {
        scanHtml += "<p class='muted'>(no topic sizes parsed)</p>";
      }
      if (!j.ok) {
        scanOut.innerHTML = `<p class="status-bad">${esc(String(j.error || j.stderr || "scan failed"))}</p>${scanHtml}`;
        return;
      }
      placeholder.hidden = true;
      scanOut.innerHTML = scanHtml;
      rebalanceLastScanOk = true;
      renderRebalanceCommands();
      if (!rebalanceBtnRefreshMode) {
        rebalanceBtnRefreshMode = true;
        btn.textContent = "Refresh";
      }
    } catch (e) {
      const d = /** @type {Record<string, unknown>} */ (e.detail || {});
      const extra = d.error != null ? `<p class="muted">${esc(String(d.error))}</p>` : "";
      scanOut.innerHTML = `<p class="status-bad">${esc(e.message)}</p>${extra}`;
    } finally {
      btn.disabled = false;
    }
  }

  document.getElementById("rebalance-analyze-btn")?.addEventListener("click", () => runRebalanceAnalyze());

  const g = () => app.querySelector("#kafka-group")?.value.trim() ?? "";
  const t = () => app.querySelector("#kafka-topic")?.value.trim() ?? "";
  const topicsFetchOpts = () => ({
    topicsMin: document.getElementById("kafka-topics-min")?.value ?? "",
    topicsSubstring: document.getElementById("kafka-topics-substring")?.value ?? "",
  });

  function wireKafkaRows(gridEl, rows) {
    if (!gridEl) return;
    for (const row of rows) {
      const [, pid, bid, needsGroup, needsTopic, , topicsFilters, , panelOpts] = row;
      const opts = { needsGroup, needsTopic };
      if (topicsFilters) opts.getFetchOpts = topicsFetchOpts;
      if (panelOpts === "activate") opts.switchToRefreshAfterOk = true;
      wirePanelRefresh(gridEl, bid, pid, g, t, opts);
    }
  }

  wireKafkaRows(gridBrokers, brokerPanels);
  wireKafkaRows(gridTopics, topicPanels);
  wireKafkaRows(gridGroups, [...groupPanelsLeft, ...groupPanelsRight, ...groupPanelsLag]);

  wireWorkloadPodStatus(app, "kafka");
  wireWorkloadLogs(app, "kafka");

  const execWrap = app.querySelector("#exec-out-wrap");
  const execInner = app.querySelector("#exec-out-inner");
  const execJsonChk = app.querySelector("#exec-json");
  /** @type {unknown} */
  let lastExecPayload = null;

  function formatExecPlain(j) {
    return formatExecPlainHelper(j);
  }

  function renderExecOut(j) {
    if (!execInner) return;
    const asJson = execJsonChk && execJsonChk.checked;
    const text = asJson ? JSON.stringify(j, null, 2) : formatExecPlain(j);
    execInner.innerHTML = kafkaPreWithWrapFooter(text);
    bindCopyButtons(execInner);
  }

  const btnRun = app.querySelector("#exec-run");
  const btnConf = app.querySelector("#exec-run-confirmed");
  async function runExec(confirmed) {
    const command = app.querySelector("#exec-cmd").value.trim();
    execWrap.hidden = false;
    if (execInner) execInner.innerHTML = "<p class='muted'>Running…</p>";
    else execWrap.innerHTML = "<p class='muted'>Running…</p>";
    btnConf.hidden = true;
    try {
      const j = await fetchJson("/api/kafka/exec", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command, confirmed }),
      });
      lastExecPayload = j;
      if (j.needs_confirmation) {
        renderExecOut(j);
        btnConf.hidden = false;
        return;
      }
      renderExecOut(j);
    } catch (e) {
      const payload = e.detail || { error: e.message };
      lastExecPayload = payload;
      if (e.status === 409 && e.detail && e.detail.needs_confirmation) {
        renderExecOut(e.detail);
        btnConf.hidden = false;
        return;
      }
      renderExecOut(payload);
    }
  }

  if (execJsonChk) {
    execJsonChk.addEventListener("change", () => {
      if (lastExecPayload != null) renderExecOut(lastExecPayload);
    });
  }

  btnRun.addEventListener("click", () => runExec(false));
  btnConf.addEventListener("click", () => {
    const ok = window.confirm(
      "The server flagged this command as potentially destructive or write-oriented.\n\nRun it anyway? Only proceed if you fully intend to mutate Kafka or disk state.",
    );
    if (ok) runExec(true);
  });
}

function route() {
  stopWorkloadLogStream();
  if (isCurrentHashWorkloadDisabled()) {
    location.hash = "#/";
    return;
  }
  setActiveNav();
  const h = location.hash || "#/";
  if (hashMatchesRoute(h, "#/kafka-connect")) {
    renderKafkaConnect();
  } else if (hashMatchesRoute(h, "#/kafka")) {
    renderKafka();
  } else if (hashMatchesRoute(h, "#/elasticsearch")) {
    renderElasticsearch();
  } else if (hashMatchesRoute(h, "#/opensearch")) {
    renderOpensearch();
  } else if (hashMatchesRoute(h, "#/clickhouse")) {
    renderClickhouse();
  } else if (hashMatchesRoute(h, "#/postgres")) {
    renderPostgres();
  } else if (hashMatchesRoute(h, "#/cassandra")) {
    renderCassandra();
  } else {
    renderHome();
  }
}

function boot() {
  footerStatus.textContent = `v${VERSION}`;
  refreshK8sStrip();
  refreshSessionUi().then(route);
  setInterval(refreshK8sStrip, 20000);
  setInterval(refreshSessionUi, 30000);
  document.addEventListener("change", (ev) => {
    const t = ev.target;
    if (!(t instanceof HTMLInputElement) || !t.classList.contains("js-pre-wrap")) return;
    const id = t.getAttribute("data-pre-id");
    const pre = id ? document.getElementById(id) : null;
    if (!pre || !(pre instanceof HTMLElement)) return;
    if (t.checked) pre.classList.remove("output-pre--nowrap");
    else pre.classList.add("output-pre--nowrap");
  });
}

window.addEventListener("hashchange", () => {
  refreshSessionUi().then(route);
});

boot();
