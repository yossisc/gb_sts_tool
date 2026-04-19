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
const navClickhouse = document.getElementById("nav-clickhouse");
const navElasticsearch = document.getElementById("nav-elasticsearch");

const VERSION = "1.0.12";

/** @type {Array<Record<string, unknown>>} */
let clustersList = [];
let copyBtnSeq = 0;

function esc(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
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

function preWithCopy(rawText) {
  const id = `pre-copy-${++copyBtnSeq}`;
  return `<div class="pre-copy-wrap"><button type="button" class="btn-copy" data-copy-target="${id}" title="Copy to clipboard" aria-label="Copy"><svg class="btn-copy-icon" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M16 1H4c-1.1 0-2 .9-2 2v12h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg></button><pre class="output-pre" id="${id}">${esc(rawText || "")}</pre></div>`;
}

/** Kafka panel raw output: nowrap by default + wrap checkbox (bottom-right of block). */
function kafkaPreWithWrapFooter(rawText, opts = {}) {
  const id = `pre-copy-${++copyBtnSeq}`;
  const wrapOn = !!opts.wrapChecked;
  const preClass = wrapOn ? "output-pre" : "output-pre output-pre--nowrap";
  const chk = wrapOn ? " checked" : "";
  return `<div class="panel-output-stack"><div class="pre-copy-wrap"><button type="button" class="btn-copy" data-copy-target="${id}" title="Copy to clipboard" aria-label="Copy"><svg class="btn-copy-icon" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true"><path fill="currentColor" d="M16 1H4c-1.1 0-2 .9-2 2v12h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg></button><pre class="${preClass}" id="${id}">${esc(rawText || "")}</pre></div><div class="panel-output-footer"><label class="wrap-chk"><input type="checkbox" class="js-pre-wrap" data-pre-id="${id}"${chk} /> wrap</label></div></div>`;
}

function bindCopyButtons(root) {
  root.querySelectorAll("[data-copy-target]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.getAttribute("data-copy-target");
      const pre = id ? document.getElementById(id) : null;
      const text = pre ? pre.textContent || "" : "";
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
    if (sessionVerified && s.session) {
      navKafka.hidden = false;
      if (navClickhouse) navClickhouse.hidden = false;
      if (navElasticsearch) navElasticsearch.hidden = false;
      const ap = s.session.p ? ` · AWS ${s.session.p}` : "";
      const reg = s.session.r ? ` · ${s.session.r}` : "";
      const cl = s.session.d ? ` · ${String(s.session.d).toUpperCase()}` : "";
      footerSession.textContent = `Session: namespace ${s.session.n} · match “${s.session.c}”${cl}${reg}${ap}`;
    } else {
      navKafka.hidden = true;
      if (navClickhouse) navClickhouse.hidden = true;
      if (navElasticsearch) navElasticsearch.hidden = true;
      footerSession.textContent = "Session: not connected";
    }
  } catch {
    sessionVerified = false;
    navKafka.hidden = true;
    if (navClickhouse) navClickhouse.hidden = true;
    if (navElasticsearch) navElasticsearch.hidden = true;
    footerSession.textContent = "Session: unknown";
  }
}

function setActiveNav() {
  const h = location.hash || "#/";
  document.querySelectorAll(".nav a[data-route]").forEach((a) => {
    const href = a.getAttribute("href") || "";
    if (h === href || (href !== "#/" && h.startsWith(href))) {
      a.classList.add("nav-active");
    } else {
      a.classList.remove("nav-active");
    }
  });
}

/** @param {{ index: number, variant: "topic" | "group" }} pick */
function kafkaPickCellInner(cell, ci, pick) {
  if (!pick || pick.index !== ci) return esc(cell == null ? "" : String(cell));
  const enc = encodeURIComponent(String(cell ?? ""));
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
    pickCol != null && pickCol !== "" && (pickVar === "topic" || pickVar === "group")
      ? { index: Number.parseInt(pickCol, 10), variant: /** @type {"topic"|"group"} */ (pickVar) }
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
      return;
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
          pickColumn: { index: 0, variant: "group" },
          stringSortableColumnIndexes: hasSizeCol ? [] : [0],
          sortableColumnIndexes: hasSizeCol ? [1] : [],
        });
      } else {
        inner += renderTable(j.table, { theadFirstRow: true });
      }
    }
    const blobOut = [j.stdout || "", j.stderr && `--- stderr ---\n${j.stderr}`].filter(Boolean).join("\n\n");
    inner += kafkaPreWithWrapFooter(blobOut, { wrapChecked: panelId === "group_state" });
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
  } catch (e) {
    const d = e.detail || {};
    const blob = JSON.stringify(d, null, 2);
    elOut.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>${kafkaPreWithWrapFooter(blob)}`;
    bindCopyButtons(elOut);
  }
}

/**
 * @param {{ spanAll?: boolean, topicsFilters?: boolean, extraPanelClass?: string }} opts
 */
function panelCard(title, bodyElId, opts = {}) {
  const span = opts.spanAll ? " panel--span-all" : "";
  const extra = opts.extraPanelClass ? ` ${opts.extraPanelClass}` : "";
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
          <button type="button" class="btn btn-primary" data-refresh="${esc(bodyElId)}">Refresh</button>
        </div>
      </div>
      ${filters}
      <div id="${esc(bodyElId)}"></div>
    </section>
  `;
}

/**
 * @param {{ needsGroup?: boolean, needsTopic?: boolean, getFetchOpts?: () => Record<string, string> }} opts
 */
function wirePanelRefresh(root, bodyElId, panelId, getGroup, getTopic, opts) {
  const btn = root.querySelector(`[data-refresh="${bodyElId}"]`);
  const out = root.querySelector(`#${bodyElId}`);
  const needsG = !!opts.needsGroup;
  const needsT = !!opts.needsTopic;
  const getFetchOpts = typeof opts.getFetchOpts === "function" ? opts.getFetchOpts : () => ({});
  const run = () => {
    if (needsG && !getGroup()) {
      out.innerHTML = "<p class='muted'>Enter a <strong>consumer group</strong> above, then click Refresh.</p>";
      return;
    }
    if (needsT && !getTopic()) {
      out.innerHTML = "<p class='muted'>Enter a <strong>topic</strong> above, then click Refresh.</p>";
      return;
    }
    loadPanel(panelId, out, getGroup(), getTopic(), getFetchOpts());
  };
  btn.addEventListener("click", run);
  if (!needsG && !needsT) {
    run();
  } else {
    out.innerHTML = "<p class='muted'>Fill group/topic if needed, then click <strong>Refresh</strong>.</p>";
  }
}

function toggleAwsProfileRow() {
  const cloud = document.getElementById("cloud-select")?.value || "aws";
  const row = document.getElementById("aws-profile-row");
  if (row) row.style.display = cloud === "azure" ? "none" : "";
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
      prSel.innerHTML = "";
      const optNone = document.createElement("option");
      optNone.value = "";
      optNone.textContent = "(none) shell default AWS_PROFILE";
      prSel.appendChild(optNone);
      for (const name of pr.profiles || []) {
        const o = document.createElement("option");
        o.value = name;
        o.textContent = name;
        prSel.appendChild(o);
      }
      const dprof = pr.default_profile || "glassbox_AWS_MT";
      if (def.aws_profile && (pr.profiles || []).includes(def.aws_profile)) prSel.value = def.aws_profile;
      else if ((pr.profiles || []).includes(dprof)) prSel.value = dprof;
      metaEl.textContent = `AWS profiles: built-in list. Defaults file: data/sensitive/last_connected.json`;
    } catch (e) {
      prSel.innerHTML = `<option value="">${esc(e.message || "profiles")}</option>`;
    }
    document.getElementById("namespace-hint").value = def.namespace || "";
    document.getElementById("cluster-typed").value = def.aws_cluster_name || "";
    toggleAwsProfileRow();
  })();

  cloudSel.addEventListener("change", toggleAwsProfileRow);

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
      clustersList = j.clusters || [];
      cSel.innerHTML = '<option value="">— Select a cluster —</option>';
      clustersList.forEach((c, i) => {
        const o = document.createElement("option");
        o.value = String(i);
        const loc = c.location ? ` (${c.location})` : "";
        const rg = c.resourceGroup ? ` [${c.resourceGroup}]` : "";
        o.textContent = `${c.cloud.toUpperCase()}: ${c.name}${rg}${loc}`;
        cSel.appendChild(o);
      });
      if (!clustersList.length) {
        cSel.innerHTML = '<option value="">(no clusters in this region / filter)</option>';
      }
      if (listStatus) {
        listStatus.textContent = clustersList.length ? `Listed ${clustersList.length} cluster(s).` : "No clusters returned.";
        listStatus.className = clustersList.length
          ? "list-clusters-status list-clusters-status--ok"
          : "list-clusters-status muted";
      }
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
  elOut.innerHTML = "<p class='muted'>Loading…</p>";
  try {
    const j = await fetchJson(u.toString());
    if (!j.ok) {
      const blob = [j.error || "failed", j.stderr && `stderr:\n${j.stderr}`, j.stdout && `stdout:\n${j.stdout}`]
        .filter(Boolean)
        .join("\n\n");
      elOut.innerHTML = `<p class='status-bad'>${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(blob)}`;
      bindCopyButtons(elOut);
      return;
    }
    let inner = "";
    if (j.cmd_hint) inner += `<p class="panel-cmd-hint">${esc(j.cmd_hint)}</p>`;
    if (j.sql_executed) inner += `<p class="hint ch-sql-hint">${esc(j.sql_executed)}</p>`;
    const blobOut = [j.stdout || "", j.stderr && `--- stderr ---\n${j.stderr}`].filter(Boolean).join("\n\n");
    inner += kafkaPreWithWrapFooter(blobOut);
    inner += `<p class='hint'>exit ${esc(String(j.returncode))} · pod ${esc(String(j.pod || ""))}</p>`;
    elOut.innerHTML = inner;
    bindCopyButtons(elOut);
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
    </p>
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
      </div>
      <p class="hint">Time filter: use <strong>Hours back</strong> for a rolling window (default last 24h), or set <strong>From</strong> and <strong>To</strong> together for an absolute range on <code>session_ts</code>.</p>
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
  `;
  const grid = app.querySelector("#ch-panels");
  /** @type {Array<[string, string, string, boolean, boolean]>} */
  const chPanels = [
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
    const text = asJson ? JSON.stringify(j, null, 2) : formatChExecPlain(j);
    chExecInner.innerHTML = kafkaPreWithWrapFooter(text);
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
}

function appendElasticsearchToolbarParams(u) {
  u.searchParams.set("es_pod", document.getElementById("es-pod-select")?.value ?? "0");
  u.searchParams.set("idx_health", document.querySelector('input[name="es-idx-health"]:checked')?.value ?? "all");
  u.searchParams.set("idx_state", document.querySelector('input[name="es-idx-state"]:checked')?.value ?? "all");
  const sub = document.getElementById("es-idx-substring")?.value?.trim() ?? "";
  if (sub) u.searchParams.set("idx_substring", sub);
  const alloc = document.getElementById("es-alloc-body")?.value ?? "{}";
  u.searchParams.set("alloc_body", alloc);
  const ip = document.getElementById("es-index-pattern")?.value?.trim() ?? "";
  if (ip) u.searchParams.set("index_pattern", ip);
}

async function loadEsPanel(panelId, elOut) {
  const u = new URL("/api/elasticsearch/panel", location.origin);
  u.searchParams.set("panel", panelId);
  appendElasticsearchToolbarParams(u);
  elOut.innerHTML = "<p class='muted'>Loading…</p>";
  try {
    const j = await fetchJson(u.toString());
    if (!j.ok) {
      const blob = [j.error || "failed", j.stderr && `stderr:\n${j.stderr}`, j.stdout && `stdout:\n${j.stdout}`]
        .filter(Boolean)
        .join("\n\n");
      elOut.innerHTML = `<p class='status-bad'>${esc(j.error || j.stderr || "failed")}</p>${kafkaPreWithWrapFooter(blob)}`;
      bindCopyButtons(elOut);
      return;
    }
    let inner = "";
    if (j.cmd_hint) inner += `<p class="panel-cmd-hint">${esc(j.cmd_hint)}</p>`;
    if (j.path_executed) {
      inner += `<p class="hint ch-sql-hint">${esc(String(j.http_method || "GET"))} ${esc(String(j.path_executed))}</p>`;
    }
    const blobOut = [j.stdout || "", j.stderr && `--- stderr ---\n${j.stderr}`].filter(Boolean).join("\n\n");
    inner += kafkaPreWithWrapFooter(blobOut);
    inner += `<p class='hint'>HTTP via pod ${esc(String(j.pod || ""))}</p>`;
    elOut.innerHTML = inner;
    bindCopyButtons(elOut);
  } catch (e) {
    const d = e.detail || {};
    const blob = JSON.stringify(d, null, 2);
    elOut.innerHTML = `<p class='status-bad'>${esc(e.message)}</p>${kafkaPreWithWrapFooter(blob)}`;
    bindCopyButtons(elOut);
  }
}

/**
 * @param {{ needsIndexPattern?: boolean }} opts
 */
function wireEsPanelRefresh(root, bodyElId, panelId, opts) {
  const needsIndex = !!opts.needsIndexPattern;
  const btn = root.querySelector(`[data-refresh="${bodyElId}"]`);
  const out = root.querySelector(`#${bodyElId}`);
  const run = () => {
    if (needsIndex && !document.getElementById("es-index-pattern")?.value?.trim()) {
      out.innerHTML = "<p class='muted'>Enter an <strong>index pattern</strong> in the toolbar (for the named <code>/_cat/indices/&lt;index&gt;</code> panel), then Refresh.</p>";
      return;
    }
    loadEsPanel(panelId, out);
  };
  btn.addEventListener("click", run);
  if (needsIndex) {
    out.innerHTML = "<p class='muted'>Enter index pattern in the toolbar, then click <strong>Refresh</strong>.</p>";
  } else {
    run();
  }
}

function renderElasticsearch() {
  if (!sessionVerified) {
    app.innerHTML =
      "<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The Elasticsearch page requires a verified session.</div>";
    return;
  }
  app.innerHTML = `
    <p class="muted">
      Uses <code>kubectl exec</code> on <code>glassbox-elasticsearch-master-&lt;n&gt;</code> container <code>elasticsearch</code> (override with env <code>GB_STS_ES_CONTAINER</code> if your chart differs)
      and <code>curl</code> to <code>http://127.0.0.1:9200</code> — no port-forward.
    </p>
    <section class="panel">
      <h2>Elasticsearch parameters</h2>
      <div class="ch-toolbar form-grid-wide">
        <label class="field">
          <span>Pod</span>
          <select class="text" id="es-pod-select"><option value="0">glassbox-elasticsearch-master-0</option></select>
        </label>
        <label class="field" style="min-width:16rem;">
          <span>Index pattern (named cat indices panel)</span>
          <input class="text" id="es-index-pattern" type="text" placeholder="glassbox_singledim_agguser_week_2915" autocomplete="off" />
        </label>
      </div>
      <fieldset class="es-fieldset">
        <legend>Cat indices panel — filters (health / open / name)</legend>
        <p class="hint" style="margin-top:0;">Applied when you refresh <strong>Cat indices (filtered JSON)</strong>. Red indices are unhealthy.</p>
        <div class="es-radio-row">
          <span class="es-radio-label">Health:</span>
          <label><input type="radio" name="es-idx-health" value="all" checked /> all</label>
          <label><input type="radio" name="es-idx-health" value="green" /> green</label>
          <label><input type="radio" name="es-idx-health" value="yellow" /> yellow</label>
          <label><input type="radio" name="es-idx-health" value="red" /> red</label>
        </div>
        <div class="es-radio-row">
          <span class="es-radio-label">State:</span>
          <label><input type="radio" name="es-idx-state" value="all" checked /> all</label>
          <label><input type="radio" name="es-idx-state" value="open" /> open</label>
          <label><input type="radio" name="es-idx-state" value="close" /> closed</label>
        </div>
        <label class="field" style="margin-top:0.5rem;">
          <span>Index name contains</span>
          <input class="text" id="es-idx-substring" type="text" placeholder="substring or * pattern fragment" maxlength="200" autocomplete="off" />
        </label>
      </fieldset>
      <label class="field" style="margin-top:0.75rem;">
        <span>Allocation explain JSON body (<code>POST /_cluster/allocation/explain</code>)</span>
        <textarea id="es-alloc-body" spellcheck="false" rows="3" class="text" style="font-family:var(--mono); min-height:4rem;">{}</textarea>
      </label>
    </section>
    <div class="panels-grid" id="es-panels"></div>
    <section class="panel exec-box">
      <h2>Custom GET path</h2>
      <p class="muted">Read-only GET only. Path must start with <code>/</code>. Some paths require confirmation if not on the conservative allowlist.</p>
      <label class="field">
        <span>Path and query (e.g. <code>/_cluster/health?pretty</code>)</span>
        <input class="text" id="es-exec-path" type="text" placeholder="/_cluster/health?pretty" autocomplete="off" />
      </label>
      <p style="margin-top:0.75rem;">
        <button type="button" class="btn btn-primary" id="es-exec-run">Run GET</button>
        <button type="button" class="btn btn-danger" id="es-exec-run-confirmed" hidden>Run with confirmation</button>
      </p>
      <div id="es-exec-out-wrap" class="exec-out-wrap" hidden>
        <div id="es-exec-out-inner"></div>
        <div class="panel-output-footer">
          <label class="exec-json-chk"><input type="checkbox" id="es-exec-json" /> json</label>
        </div>
      </div>
    </section>
  `;
  const grid = app.querySelector("#es-panels");
  /** @type {Array<[string, string, string, boolean]>} */
  const esPanels = [
    ["Cluster root (version)", "es_root", "es-p-root", false],
    ["Cluster health", "es_cluster_health", "es-p-health", false],
    ["Cat nodes", "es_cat_nodes", "es-p-nodes", false],
    ["Cluster settings", "es_cluster_settings", "es-p-set", false],
    ["Cat allocation", "es_cat_allocation", "es-p-alloc", false],
    ["Allocation explain (POST)", "es_allocation_explain", "es-p-expl", false],
    ["Cat indices (filtered JSON)", "es_cat_indices", "es-p-idx", false],
    ["Cat indices / named index", "es_cat_indices_named", "es-p-idxn", true],
    ["Cat shards", "es_cat_shards", "es-p-shards", false],
    ["Cluster stats", "es_cluster_stats", "es-p-cstats", false],
    ["Nodes stats", "es_nodes_stats", "es-p-nstats", false],
    ["Cat pending tasks", "es_cat_pending_tasks", "es-p-cpt", false],
    ["Cluster pending tasks", "es_cluster_pending_tasks", "es-p-cpnd", false],
    ["Cat thread pool", "es_cat_thread_pool", "es-p-tp", false],
    ["ILM status", "es_ilm_status", "es-p-ilm", false],
    ["Cat templates", "es_cat_templates", "es-p-tpl", false],
    ["Cat aliases", "es_cat_aliases", "es-p-al", false],
    ["Cluster state (metadata slice)", "es_cluster_state_metadata", "es-p-csm", false],
    ["Nodes info", "es_nodes_info", "es-p-ni", false],
  ];
  let html = "";
  for (const row of esPanels) {
    const [title, , bid] = row;
    html += panelCard(title, bid);
  }
  grid.innerHTML = html;
  for (const row of esPanels) {
    const [, pid, bid, needsIdx] = row;
    wireEsPanelRefresh(grid, bid, pid, { needsIndexPattern: needsIdx });
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
    const rep = stack?.components?.elasticsearch?.replicas;
    const sel = document.getElementById("es-pod-select");
    if (!sel) return;
    const n = typeof rep === "number" && rep > 0 ? rep : 1;
    sel.innerHTML = "";
    for (let i = 0; i < n; i++) {
      const o = document.createElement("option");
      o.value = String(i);
      o.textContent = `glassbox-elasticsearch-master-${i}`;
      sel.appendChild(o);
    }
  })();

  const esExecWrap = app.querySelector("#es-exec-out-wrap");
  const esExecInner = app.querySelector("#es-exec-out-inner");
  const esExecJson = app.querySelector("#es-exec-json");
  const esConfBtn = app.querySelector("#es-exec-run-confirmed");
  /** @type {unknown} */
  let lastEsExecPayload = null;

  function renderEsExecOut(j) {
    if (!esExecInner) return;
    const asJson = esExecJson && esExecJson.checked;
    const text = asJson ? JSON.stringify(j, null, 2) : formatExecPlainHelper(j);
    esExecInner.innerHTML = kafkaPreWithWrapFooter(text);
    bindCopyButtons(esExecInner);
  }

  async function runEsExec(confirmed) {
    let path = document.getElementById("es-exec-path")?.value?.trim() ?? "";
    if (!path) {
      if (esExecInner) esExecInner.innerHTML = "<p class='status-bad'>Enter a path (e.g. <code>/_cluster/health?pretty</code>).</p>";
      esExecWrap.hidden = false;
      return;
    }
    if (!path.startsWith("/")) path = `/${path}`;
    const es_pod = Number.parseInt(String(document.getElementById("es-pod-select")?.value ?? "0"), 10) || 0;
    esExecWrap.hidden = false;
    if (esExecInner) esExecInner.innerHTML = "<p class='muted'>Running…</p>";
    if (esConfBtn) esConfBtn.hidden = true;
    try {
      const j = await fetchJson("/api/elasticsearch/exec", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path, method: "GET", es_pod, confirmed }),
      });
      lastEsExecPayload = j;
      if (j.needs_confirmation) {
        renderEsExecOut(j);
        if (esConfBtn) esConfBtn.hidden = false;
        return;
      }
      renderEsExecOut(j);
    } catch (e) {
      const payload = e.detail || { error: e.message };
      lastEsExecPayload = payload;
      if (e.status === 409 && e.detail && e.detail.needs_confirmation) {
        renderEsExecOut(e.detail);
        if (esConfBtn) esConfBtn.hidden = false;
        return;
      }
      renderEsExecOut(payload);
    }
  }

  if (esExecJson) {
    esExecJson.addEventListener("change", () => {
      if (lastEsExecPayload != null) renderEsExecOut(lastEsExecPayload);
    });
  }
  app.querySelector("#es-exec-run").addEventListener("click", () => runEsExec(false));
  app.querySelector("#es-exec-run-confirmed").addEventListener("click", () => {
    const ok = window.confirm(
      "The server flagged this GET path as potentially sensitive.\n\nRun it anyway? Only proceed if you accept the risk.",
    );
    if (ok) runEsExec(true);
  });
}

function renderKafka() {
  if (!sessionVerified) {
    app.innerHTML =
      "<div class='callout callout-warn'>Connect from <a href='#/'>Home</a> first. The Kafka page is hidden until verification succeeds.</div>";
    return;
  }
  app.innerHTML = `
    <p class="muted">Pod <code>glassbox-kafka-0</code> — commands run in the <code>kafka</code> container with a random high <code>JMX_PORT</code> per request to avoid port collisions.</p>

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
          <button type="button" class="btn btn-primary" id="rebalance-analyze-btn">Analyze</button>
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
      <p class="muted" id="rebalance-placeholder">Click <strong>Analyze</strong> to scan data directories and show helper commands.</p>
      <div id="rebalance-scan-out" hidden></div>
      <div id="rebalance-commands" hidden></div>
    </section>

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
    ["Topic describe (head)", "topic_describe", "p-t-desc", false, true],
    ["Topic end offsets (GetOffsetShell)", "topic_offsets_end", "p-t-off", false, true],
  ];
  /** Left ~⅓: consumer list only. Right ~⅔: state → members → verbose. Lag: full-width row below. */
  const groupPanelsLeft = [["Consumer groups (list)", "consumer_groups_list", "p-groups", false, false]];
  const groupPanelsLag = [["Group lag (describe)", "group_lag", "p-lag", true, false, true, false]];
  const groupPanelsRight = [
    ["Group state (--state --verbose)", "group_state", "p-g-state", true, false],
    ["Group members", "group_members", "p-g-mem", true, false, false, false, "panel--kafka-group-members-span"],
    [
      "Group members + partition assignment",
      "group_members_verbose",
      "p-g-memv",
      true,
      false,
      false,
      false,
      "panel--kafka-group-members-span",
    ],
  ];

  /** @param {HTMLElement | null} gridEl */
  function fillKafkaGrid(gridEl, rows) {
    if (!gridEl) return;
    let html = "";
    for (const row of rows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls] = row;
      html += panelCard(title, bid, {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      });
    }
    gridEl.innerHTML = html;
  }

  /** @param {HTMLElement | null} rootEl */
  function fillKafkaGroupsLayout(rootEl, leftRows, rightRows, lagRows) {
    if (!rootEl) return;
    let leftHtml = "";
    for (const row of leftRows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls] = row;
      leftHtml += panelCard(title, bid, {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      });
    }
    let rightHtml = "";
    for (const row of rightRows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls] = row;
      rightHtml += panelCard(title, bid, {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      });
    }
    let lagHtml = "";
    for (const row of lagRows) {
      const [title, , bid, , , spanAll, topicsFilters, extraCls] = row;
      lagHtml += panelCard(title, bid, {
        spanAll: !!spanAll,
        topicsFilters: !!topicsFilters,
        extraPanelClass: typeof extraCls === "string" ? extraCls : "",
      });
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
      const [, pid, bid, needsGroup, needsTopic, , topicsFilters] = row;
      const opts = { needsGroup, needsTopic };
      if (topicsFilters) opts.getFetchOpts = topicsFetchOpts;
      wirePanelRefresh(gridEl, bid, pid, g, t, opts);
    }
  }

  wireKafkaRows(gridBrokers, brokerPanels);
  wireKafkaRows(gridTopics, topicPanels);
  wireKafkaRows(gridGroups, [...groupPanelsLeft, ...groupPanelsRight, ...groupPanelsLag]);

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
  setActiveNav();
  const h = location.hash || "#/";
  if (h.startsWith("#/kafka")) {
    renderKafka();
  } else if (h.startsWith("#/clickhouse")) {
    renderClickhouse();
  } else if (h.startsWith("#/elasticsearch")) {
    renderElasticsearch();
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
