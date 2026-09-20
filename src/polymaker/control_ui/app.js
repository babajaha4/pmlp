"use strict";

const controlToken = document.querySelector('meta[name="control-token"]').content;
const pollSeconds = Number(document.querySelector('meta[name="poll-seconds"]').content);
const state = { snapshot: null, loading: false, action: null, nextRefresh: Date.now() };

const $ = (id) => document.getElementById(id);
const money = (value, digits = 4) => Number(value ?? 0).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const number = (value, digits = 2) => Number(value ?? 0).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const percent = (value, limit) => limit > 0 ? Math.max(0, Number(value) / Number(limit) * 100) : 0;
const dateTime = (value) => value ? new Intl.DateTimeFormat("zh-CN", { dateStyle: "short", timeStyle: "medium" }).format(new Date(value)) : "--";
const shortMarket = (value) => String(value).replace(/^Will /, "").replace(/\?$/, "");

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function setTone(node, value) {
  node.classList.remove("positive", "negative");
  if (Number(value) > 0) node.classList.add("positive");
  if (Number(value) < 0) node.classList.add("negative");
}

function emptyRow(body, columns, text) {
  body.replaceChildren();
  const row = element("tr");
  const cell = element("td", "empty-row", text);
  cell.colSpan = columns;
  row.append(cell);
  body.append(row);
}

function statusBadge(service) {
  const badge = $("serviceBadge");
  badge.className = "status-badge";
  const active = service.active_state === "active" && service.sub_state === "running";
  badge.classList.add(active ? "status-active" : "status-stopped");
  badge.replaceChildren(element("span", "status-dot"), element("span", "", active ? "LIVE 运行中" : "服务已停止"));
  $("processState").textContent = active ? `PID ${service.main_pid} · 重启 ${service.restarts} 次` : "当前不会发送新订单";
  $("environmentLine").textContent = `VPS LIVE · ${active ? "交易中" : "停止"} · ${dateTime(state.snapshot.queried_at)}`;
  $("startButton").disabled = active || Boolean(state.action);
  $("stopButton").disabled = !active || Boolean(state.action);
  $("restartButton").disabled = !active || Boolean(state.action);
}

function metric(id, value, tone) {
  const node = $(id);
  node.textContent = value;
  node.classList.remove("positive", "negative");
  if (tone) node.classList.add(tone);
}

function renderMetrics(data) {
  const summary = data.summary;
  metric("collateralMetric", money(data.wallet.collateral_pusd, 2));
  metric("equityMetric", `${summary.equity_mtm >= 0 ? "+" : ""}${money(summary.equity_mtm)}`, summary.equity_mtm >= 0 ? "positive" : "negative");
  metric("dailyPnlMetric", `${summary.daily_pnl_live >= 0 ? "+" : ""}${money(summary.daily_pnl_live)}`, summary.daily_pnl_live >= 0 ? "positive" : "negative");
  metric("exposureMetric", money(summary.total_exposure, 2));
  $("exposureNote").textContent = `${money(summary.total_exposure_limit, 0)} 上限 · ${number(percent(summary.total_exposure, summary.total_exposure_limit), 1)}%`;
  metric("ordersMetric", String(summary.open_order_count));
  $("ordersNote").textContent = `BUY 预留 ${money(summary.buy_reservation, 2)} pUSD`;
  metric("fillsMetric", String(data.fills.all.maker_count));
  $("fillsNote").textContent = `累计 ${money(data.fills.all.notional, 2)} pUSD`;
}

function badge(text) { return element("span", "", text); }

function renderStrategyBand(data) {
  const strategy = data.strategy;
  const profiles = [...new Set(strategy.profiles.map((item) => item.profile))];
  $("strategyHeadline").textContent = `${profiles.join(" / ")} · 双边 maker-only 库存做市`;
  const badges = $("strategyBadges");
  badges.replaceChildren(
    badge(strategy.maker_only ? "Post-only 已强制" : "Post-only 未启用"),
    badge(strategy.automatic_merge ? "Auto merge 开启" : "Auto merge 关闭"),
    badge(`${strategy.reconcile_interval_seconds}s 权威对账`),
    badge(`${strategy.heartbeat_interval_seconds}s Heartbeat`),
  );
}

function regimeNode(value) {
  return element("span", `regime regime-${String(value).toLowerCase()}`, value);
}

function exposureBar(value, limit) {
  const cell = element("div", "bar-cell");
  const ratio = percent(value, limit);
  const text = element("span", "", `${money(value, 2)} / ${money(limit, 0)}`);
  const bar = element("progress", `mini-bar ${ratio >= 95 ? "danger" : ratio >= 80 ? "warn" : ""}`);
  bar.max = 100;
  bar.value = Math.min(100, ratio);
  cell.append(text, bar);
  return cell;
}

function renderMarkets(data) {
  const body = $("marketRows");
  body.replaceChildren();
  data.markets.forEach((market) => {
    const row = element("tr");
    const name = element("td");
    name.append(element("span", "market-name", shortMarket(market.market)), element("span", "subtext", market.profile));
    row.append(
      name,
      element("td"),
      element("td", "", money(market.position_value)),
      element("td", "", money(market.buy_reservation)),
      element("td", "", money(market.exposure)),
      element("td"),
      element("td", "", String(market.order_count)),
    );
    row.children[1].append(regimeNode(market.regime));
    row.children[5].append(exposureBar(market.exposure, market.exposure_limit));
    body.append(row);
  });
  if (!data.markets.length) emptyRow(body, 7, "没有配置市场");
  const ledger = $("ledgerBadge");
  ledger.textContent = data.summary.ledger_matches_positions ? "账本与权威仓位一致" : "账本与权威仓位不一致";
  ledger.className = `inline-state ${data.summary.ledger_matches_positions ? "ok" : "bad"}`;
}

function renderPositions(data) {
  const body = $("positionRows");
  body.replaceChildren();
  data.positions.forEach((position) => {
    const row = element("tr");
    const name = element("td");
    name.append(element("span", "market-name", shortMarket(position.market)), element("span", "subtext", position.outcome));
    const pnl = element("td", "", `${position.unrealized_pnl >= 0 ? "+" : ""}${money(position.unrealized_pnl)}`);
    setTone(pnl, position.unrealized_pnl);
    row.append(
      name,
      element("td", "", number(position.size, 4)),
      element("td", "", number(position.average_price, 4)),
      element("td", "", position.best_bid == null ? "--" : `${number(position.best_bid, 3)} / ${number(position.best_ask, 3)}`),
      element("td", "", money(position.value)),
      pnl,
    );
    body.append(row);
  });
  if (!data.positions.length) emptyRow(body, 6, "当前没有配置市场持仓");
}

function riskLine(label, value, limit, suffix = "pUSD") {
  const line = element("div", "risk-line");
  const top = element("div", "risk-line-top");
  top.append(element("span", "", label), element("strong", "", `${money(value, 2)} / ${money(limit, 2)} ${suffix}`));
  const ratio = percent(value, limit);
  const track = element("progress", `risk-track ${ratio >= 95 ? "danger" : ratio >= 80 ? "warn" : ""}`);
  track.max = 100;
  track.value = Math.min(100, ratio);
  line.append(top, track);
  return line;
}

function renderRisk(data) {
  const bars = $("riskBars");
  const limits = data.risk.limits;
  const killed = data.risk.state && Boolean(data.risk.state.killed);
  bars.replaceChildren(
    riskLine("全局最坏情况敞口", data.summary.total_exposure, limits.total),
    riskLine("BUY 挂单资本预留", data.summary.buy_reservation, limits.total),
    riskLine("当日亏损使用", Math.max(0, -data.summary.daily_pnl_live), limits.daily_loss),
    riskLine("Kill 状态", killed ? 1 : 0, 1, killed ? "已触发" : "正常"),
  );
}

function renderOrders(data) {
  const body = $("orderRows");
  body.replaceChildren();
  data.orders.forEach((order) => {
    const row = element("tr");
    row.append(
      element("td", "market-name", shortMarket(order.market)),
      element("td", "", order.outcome),
      element("td", order.side === "BUY" ? "side-buy" : "side-sell", order.side),
      element("td", "", number(order.price, 4)),
      element("td", "", number(order.size, 4)),
      element("td", "", money(order.notional)),
      element("td", "", order.managed ? "机器人" : "未配置"),
    );
    body.append(row);
  });
  if (!data.orders.length) emptyRow(body, 7, "当前没有权威挂单");
  const badge = $("managedOrdersBadge");
  badge.textContent = data.summary.unconfigured_order_count === 0 ? "全部属于配置 token" : `${data.summary.unconfigured_order_count} 个未配置订单`;
  badge.className = `inline-state ${data.summary.unconfigured_order_count === 0 ? "ok" : "bad"}`;
}

function renderFills(data) {
  const body = $("fillRows");
  body.replaceChildren();
  data.fills.latest.forEach((fill) => {
    const row = element("tr");
    row.append(
      element("td", "", dateTime(fill.timestamp)),
      element("td", "market-name", shortMarket(fill.market)),
      element("td", "", fill.outcome),
      element("td", fill.side === "BUY" ? "side-buy" : "side-sell", fill.side),
      element("td", "", number(fill.price, 4)),
      element("td", "", number(fill.size, 4)),
      element("td", "", money(fill.notional)),
      element("td", "", fill.maker ? "Maker" : "Taker"),
    );
    body.append(row);
  });
  if (!data.fills.latest.length) emptyRow(body, 8, "尚无成交记录");
  $("fillSummary").textContent = `UTC 今日 ${data.fills.today_utc.count} 笔 · 累计 ${data.fills.all.count} 笔`;
}

const parameterLabels = {
  base_size_usdc: "基础订单额", inventory_cap_usdc: "策略库存上限", inventory_soft_fraction: "Soft cap 比例",
  layers: "报价层数", minimum_edge_ticks: "最小边际", minimum_half_spread_ticks: "最小半价差",
  inventory_skew_gamma: "库存偏斜 Gamma", volatility_spread_weight: "波动扩点权重", toxicity_spread_weight: "毒性扩点权重",
  reward_aware_placement: "奖励带报价", reward_target_ratio: "奖励带目标比例",
  anti_sniping_enabled: "反狙击保护", anti_sniping_pause_seconds: "跳变暂停",
  anti_sniping_stable_confirm_seconds: "稳定确认", fill_cooldown_seconds: "成交冷却",
  max_reprice_ticks_per_update: "单次最大追价",
  trend_size_multiplier: "趋势状态下单倍率", event_cooloff_seconds: "事件冷却", exit_urgency_seconds: "退出紧迫周期",
};

function parameterValue(key, value) {
  if (["base_size_usdc", "inventory_cap_usdc"].includes(key)) return `${money(value, 2)} pUSD`;
  if (["event_cooloff_seconds", "exit_urgency_seconds", "anti_sniping_pause_seconds", "anti_sniping_stable_confirm_seconds", "fill_cooldown_seconds"].includes(key)) return `${number(value, 1)} 秒`;
  if (["minimum_edge_ticks", "minimum_half_spread_ticks"].includes(key)) return `${number(value, 0)} ticks`;
  if (key === "inventory_soft_fraction" || key === "trend_size_multiplier" || key === "reward_target_ratio") return `${number(value * 100, 0)}%`;
  if (["reward_aware_placement", "anti_sniping_enabled"].includes(key)) return value ? "启用" : "停用";
  if (key === "max_reprice_ticks_per_update") return `${number(value, 0)} ticks`;
  return number(value, 2);
}

function renderProfiles(data) {
  const list = $("profileRows");
  list.replaceChildren();
  data.strategy.profiles.forEach((profile) => {
    const wrapper = element("article", "profile-row");
    const title = element("div", "profile-title");
    title.append(element("h3", "", shortMarket(profile.market)), element("span", "", profile.profile));
    const params = element("div", "parameter-grid");
    Object.entries(profile.parameters).forEach(([key, value]) => {
      const item = element("div", "parameter");
      item.append(element("span", "", parameterLabels[key] || key), element("strong", "", parameterValue(key, value)));
      params.append(item);
    });
    wrapper.append(title, params);
    list.append(wrapper);
  });
}

function healthItem(label, value, tone) {
  const wrapper = element("div", "health-item");
  wrapper.append(element("dt", "", label), element("dd", tone || "", String(value)));
  return wrapper;
}

function renderHealth(data) {
  const health = data.health;
  const service = data.service;
  const risk = data.risk.state || {};
  $("healthGrid").replaceChildren(
    healthItem("Active state", `${service.active_state} / ${service.sub_state}`, service.active_state === "active" ? "positive" : "negative"),
    healthItem("Main PID", service.main_pid || "--"),
    healthItem("服务重启", service.restarts),
    healthItem("启动时间", service.active_since || "--"),
    healthItem("Heartbeat 200", health.heartbeat_ok),
    healthItem("Positions 200", health.positions_ok),
    healthItem("Trades 200", health.trades_ok),
    healthItem("Orders 200", health.orders_ok),
    healthItem("STATE_UNKNOWN", health.state_unknown, health.state_unknown ? "negative" : "positive"),
    healthItem("Traceback", health.tracebacks, health.tracebacks ? "negative" : "positive"),
    healthItem("订单错误", health.order_errors, health.order_errors ? "negative" : "positive"),
    healthItem("对账错误", health.reconcile_errors, health.reconcile_errors ? "negative" : "positive"),
    healthItem("订单批次", risk.order_attempts ?? "--"),
    healthItem("失败批次", risk.order_errors ?? "--", risk.order_errors ? "negative" : "positive"),
    healthItem("人工 Kill", risk.manual_killed ? "已触发" : "正常", risk.manual_killed ? "negative" : "positive"),
    healthItem("查询时间", dateTime(data.queried_at)),
  );
}

function renderAlert(data) {
  const alerts = [...(data.warnings || [])];
  if (!data.summary.ledger_matches_positions) alerts.unshift("持久化成交账本与权威仓位不一致");
  if (data.summary.unconfigured_order_count) alerts.unshift("发现配置范围之外的订单；控制面板不会操作这些订单");
  if (data.health.state_unknown || data.health.tracebacks || data.health.order_errors || data.health.reconcile_errors) alerts.unshift("本次服务运行日志包含异常事件");
  const band = $("alertBand");
  band.classList.toggle("hidden", alerts.length === 0);
  band.textContent = alerts.join(" · ");
}

function render(data) {
  state.snapshot = data;
  statusBadge(data.service);
  renderMetrics(data);
  renderStrategyBand(data);
  renderMarkets(data);
  renderPositions(data);
  renderRisk(data);
  renderOrders(data);
  renderFills(data);
  renderProfiles(data);
  renderHealth(data);
  renderAlert(data);
}

function showError(message) {
  const badge = $("serviceBadge");
  badge.className = "status-badge status-error";
  badge.replaceChildren(element("span", "status-dot"), element("span", "", "读取失败"));
  const band = $("alertBand");
  band.classList.remove("hidden");
  band.textContent = message;
}

async function refresh() {
  if (state.loading || state.action) return;
  state.loading = true;
  $("refreshButton").disabled = true;
  try {
    const response = await fetch("/api/snapshot", { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok || !payload.ok) throw new Error(payload.error || "快照读取失败");
    render(payload.data);
    state.nextRefresh = Date.now() + pollSeconds * 1000;
  } catch (error) {
    showError(`无法读取 VPS：${error.message}`);
    state.nextRefresh = Date.now() + pollSeconds * 1000;
  } finally {
    state.loading = false;
    $("refreshButton").disabled = false;
  }
}

const actionText = {
  start: { icon: "▶", title: "启动 LIVE 做市", text: "服务将通过双重 LIVE 确认启动，并在安全启动对账通过后恢复真实 maker-only 挂单。", button: "确认启动", cls: "command-start" },
  stop: { icon: "■", title: "停止做市服务", text: "服务会收到 SIGINT，优雅撤销本项目配置 token 的挂单后退出；持久化账本与 kill 状态保留。", button: "确认停止", cls: "command-stop" },
  restart: { icon: "↻", title: "重启做市服务", text: "当前挂单会先按配置 token 撤销；新进程只有在权威订单、仓位和账本检查通过后才恢复报价。", button: "确认重启", cls: "command-restart" },
};

function confirmAction(action) {
  const copy = actionText[action];
  if (!copy) return;
  state.action = action;
  $("dialogIcon").textContent = copy.icon;
  $("dialogTitle").textContent = copy.title;
  $("dialogText").textContent = copy.text;
  const confirm = $("confirmActionButton");
  confirm.textContent = copy.button;
  confirm.className = `command ${copy.cls}`;
  $("confirmDialog").showModal();
}

function toast(message) {
  const node = $("toast");
  node.textContent = message;
  node.classList.remove("hidden");
  window.setTimeout(() => node.classList.add("hidden"), 4500);
}

async function performAction(action) {
  document.querySelectorAll("button[data-action]").forEach((button) => { button.disabled = true; });
  try {
    const response = await fetch("/api/action", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Control-Token": controlToken },
      body: JSON.stringify({ action }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) throw new Error(payload.error || "服务操作失败");
    toast(`${actionText[action].title}成功`);
    state.action = null;
    if (payload.result.snapshot) {
      render(payload.result.snapshot);
      state.nextRefresh = Date.now() + pollSeconds * 1000;
    } else {
      await refresh();
    }
  } catch (error) {
    state.action = null;
    showError(`${actionText[action].title}失败：${error.message}`);
    toast(`${actionText[action].title}失败`);
    if (state.snapshot) statusBadge(state.snapshot.service);
  }
}

document.querySelectorAll("button[data-action]").forEach((button) => button.addEventListener("click", () => confirmAction(button.dataset.action)));
$("confirmDialog").addEventListener("close", () => {
  const action = state.action;
  const confirmed = $("confirmDialog").returnValue === "confirm";
  if (!confirmed) state.action = null;
  if (confirmed && action) performAction(action);
});
$("refreshButton").addEventListener("click", refresh);
document.querySelectorAll(".tab").forEach((button) => button.addEventListener("click", () => {
  document.querySelectorAll(".tab").forEach((tab) => { tab.classList.toggle("active", tab === button); tab.setAttribute("aria-selected", tab === button ? "true" : "false"); });
  document.querySelectorAll(".tab-panel").forEach((panel) => { const active = panel.id === `${button.dataset.tab}Panel`; panel.classList.toggle("active", active); panel.hidden = !active; });
}));

window.setInterval(() => {
  const remaining = Math.max(0, Math.ceil((state.nextRefresh - Date.now()) / 1000));
  $("refreshClock").textContent = state.loading ? "更新中" : `${remaining}s`;
  if (!state.loading && !state.action && remaining === 0) refresh();
}, 1000);

refresh();
