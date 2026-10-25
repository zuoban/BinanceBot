const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../src/server/dashboard.html'), 'utf8')
  .match(/<script>([\s\S]*?)<\/script>/)[1];

test('page links restore the requested tab and browser back restores the root overview', () => {
  const ui = dashboard();
  ui.run("switchTab('tab-orders', document.getElementById('tab-btn-orders'))");
  assert.equal(ui.element('page-title').textContent, '当前挂单');
  assert.deepEqual(ui.history, ['#orders']);
  ui.run("window.location.hash = '#logs'; restoreNavigation()");
  assert.equal(ui.element('page-title').textContent, '运行日志');
  assert.equal(ui.element('tab-btn-logs').getAttribute('aria-selected'), 'true');
  assert.deepEqual(ui.history, ['#orders'], 'restoring browser history must not add another entry');
  ui.run("window.location.hash = ''; restoreNavigation()");
  assert.equal(ui.element('page-title').textContent, '策略总览');
  ui.run("window.location.hash = '#unknown'; restoreNavigation()");
  assert.equal(ui.element('page-title').textContent, '策略总览');
});

test('compact navigation closes after selection and returns focus to its visible trigger', () => {
  const ui = dashboard();
  ui.run('toggleNavigation()');
  assert.equal(ui.element('nav-toggle').getAttribute('aria-expanded'), 'true');
  ui.run("switchTab('tab-orders', document.getElementById('tab-btn-orders'))");
  assert.equal(ui.element('nav-toggle').getAttribute('aria-expanded'), 'false');
  assert.equal(ui.element('mobile-page-label').textContent, '当前挂单');
  assert.equal(ui.run('document.activeElement.id'), 'nav-toggle');
});

test('strategy edits survive navigation and returning to the saved value clears the draft indicator', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  const reads = ui.requests.filter(request => request.url === '/api/pairs/SOLUSDC/config').length;
  ui.element('cfg-amount').value = '250';
  ui.run("syncFormState('config-form'); switchTab('tab-config', document.getElementById('tab-btn-config'))");
  assert.equal(ui.element('config-draft-status').textContent, '有未保存的修改');
  ui.run("switchTab('tab-orders', document.getElementById('tab-btn-orders')); switchTab('tab-config', document.getElementById('tab-btn-config'))");
  await new Promise(setImmediate);
  assert.equal(ui.element('cfg-amount').value, '250');
  assert.equal(ui.requests.filter(request => request.url === '/api/pairs/SOLUSDC/config').length, reads);
  ui.element('cfg-amount').value = '100';
  ui.run("syncFormState('config-form')");
  assert.equal(ui.run("isFormDirty('config-form')"), false);
  assert.equal(ui.element('config-draft-status').classList.contains('is-dirty'), false);
});

test('a new strategy draft remains open after leaving and returning, even before it is edited', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run("openNewPair(); switchTab('tab-overview', document.getElementById('tab-btn-overview')); switchTab('tab-config', document.getElementById('tab-btn-config'))");
  await new Promise(setImmediate);
  assert.equal(ui.run('editingNewPair'), true);
  assert.equal(ui.element('cfg-symbol').value, '');
  assert.equal(ui.element('config-title').textContent, '新增交易对');
});

test('a card parameter action opens its strategy after leaving a new-pair draft', async () => {
  for (const draftSymbol of ['ETHUSDT', '']) {
    const ui = dashboard();
    await ui.run('initConsole()');
    ui.run('openNewPair()');
    ui.element('cfg-symbol').value = draftSymbol;
    ui.run("switchTab('tab-overview', document.getElementById('tab-btn-overview'))");
    await ui.run("openPairPage('SOLUSDC', 'tab-config')");
    await new Promise(setImmediate);
    assert.equal(ui.confirmations.length, draftSymbol ? 1 : 0);
    assert.equal(ui.run('editingNewPair'), false);
    assert.equal(ui.run('selectedPair'), 'SOLUSDC');
    assert.equal(ui.element('cfg-symbol').value, 'SOLUSDC');
    assert.equal(ui.element('config-title').textContent, 'SOLUSDC · 策略参数');
    assert.equal(ui.element('cfg-symbol').readOnly, true);
    assert.equal(ui.element('btn-save-cfg').textContent, '保存并立即生效');
    assert.equal(ui.run('activeTab'), 'tab-config');
  }
});

test('cancelling a card parameter action preserves the new-pair draft and current page', async () => {
  const ui = dashboard({ confirmResult: false });
  await ui.run('initConsole()');
  ui.run('openNewPair()');
  ui.element('cfg-symbol').value = 'ETHUSDT';
  ui.element('cfg-amount').value = '250';
  ui.run("switchTab('tab-overview', document.getElementById('tab-btn-overview'))");
  await new Promise(setImmediate);
  const requests = ui.requests.length, socket = ui.sockets.at(-1);
  await ui.run("openPairPage('SOLUSDC', 'tab-config')");
  assert.equal(ui.confirmations.length, 1);
  assert.equal(ui.run('editingNewPair'), true);
  assert.equal(ui.element('cfg-symbol').value, 'ETHUSDT');
  assert.equal(ui.element('cfg-amount').value, '250');
  assert.equal(ui.run('activeTab'), 'tab-overview');
  assert.equal(ui.element('pair-select').value, 'SOLUSDC');
  assert.equal(ui.requests.length, requests);
  assert.equal(ui.sockets.at(-1), socket);
});

test('other current-pair card actions keep the new-pair draft available', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run('openNewPair()');
  ui.element('cfg-symbol').value = 'ETHUSDT';
  for (const tabId of ['tab-detail', 'tab-orders']) {
    await ui.run(`openPairPage('SOLUSDC', '${tabId}')`);
    assert.equal(ui.run('activeTab'), tabId);
    assert.equal(ui.run('editingNewPair'), true);
    assert.equal(ui.element('cfg-symbol').value, 'ETHUSDT');
  }
  assert.equal(ui.confirmations.length, 0);
});

test('cancelling a pair switch or reset preserves the draft, selection, and connection', async () => {
  const ui = dashboard({ confirmResult: false });
  await ui.run('initConsole()');
  ui.element('cfg-interval').value = '2';
  const requests = ui.requests.length, socket = ui.sockets.at(-1);
  await ui.run("selectPair('BTCUSDT')");
  await ui.run('resetConfigForm()');
  assert.equal(ui.confirmations.length, 2);
  assert.equal(ui.run('selectedPair'), 'SOLUSDC');
  assert.equal(ui.element('pair-select').value, 'SOLUSDC');
  assert.equal(ui.element('cfg-interval').value, '2');
  assert.equal(ui.sockets.at(-1), socket);
  assert.equal(ui.requests.length, requests);
});

test('accepting a reset restores the server configuration and clears unsaved changes', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.element('cfg-interval').value = '2';
  await ui.run('resetConfigForm()');
  assert.equal(ui.confirmations.length, 1);
  assert.equal(ui.element('cfg-interval').value, '1');
  assert.equal(ui.run("isFormDirty('config-form')"), false);
  ui.run("fillConfigForm({ symbol: 'SOLUSDC', min_price: '80', dry_run: true })");
  assert.equal(ui.element('cfg-advanced').open, true, 'active price boundaries must remain visible');
});

test('failed global saves retain the draft and successful saves synchronize its indicator', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.element('cfg-sync-interval').value = '12';
  ui.run(`originalAuthFetch = authFetch;
    authFetch = async (url, options) => url === '/api/global/config' && options?.method === 'POST'
      ? { ok: false, json: async () => ({ success: false, message: 'save failed' }) } : originalAuthFetch(url, options);`);
  await ui.run('submitGlobalConfig()');
  assert.equal(ui.run("isFormDirty('global-config-form')"), true);
  assert.equal(ui.element('cfg-sync-interval').disabled, false);
  ui.run('authFetch = originalAuthFetch');
  await ui.run('submitGlobalConfig()');
  assert.equal(ui.run("isFormDirty('global-config-form')"), false);
  assert.equal(ui.element('global-draft-status').classList.contains('is-dirty'), false);
});

test('clearing filters also restores default sorting and the clear action reflects active filters', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  assert.equal(ui.element('overview-reset-filters').hidden, true);
  ui.element('overview-sort').value = 'activity';
  ui.run('renderOverview()');
  assert.equal(ui.element('overview-reset-filters').hidden, false);
  ui.run('resetOverviewFilters()');
  assert.equal(ui.element('overview-sort').value, 'default');
  assert.equal(ui.element('overview-reset-filters').hidden, true);
  assert.equal(ui.run('document.activeElement.id'), 'overview-search');
});

test('hourly chart and its table share totals, retain zero hours, and clear stale data together', () => {
  const ui = dashboard({ charts: true });
  ui.run(`renderHourlyTradeChart({
    window_start: '2026-10-15T00:00:00+08:00', window_end: '2026-10-15T02:00:00+08:00',
    buckets: [
      { hour_start: '2026-10-15T00:00:00+08:00', buy_count: 0, sell_count: 0 },
      { hour_start: '2026-10-15T01:00:00+08:00', buy_count: 3, sell_count: 2 }
    ]
  })`);
  assert.equal(ui.element('hourly-trade-total').textContent, 5);
  assert.equal(ui.element('hourly-buy-total').textContent, 3);
  assert.equal(ui.element('hourly-sell-total').textContent, 2);
  const table = ui.element('hourly-data-tbody').innerHTML;
  assert.match(table, /01:00<\/th><td>3<\/td><td>2<\/td><td>5<\/td>/);
  assert.match(table, /00:00<\/th><td>0<\/td><td>0<\/td><td>0<\/td>/);
  assert.ok(table.indexOf('01:00') < table.indexOf('00:00'), 'newest hour is first in both views');
  ui.run('renderHourlyTradeChart(null)');
  assert.equal(ui.element('hourly-trade-total').textContent, '--');
  assert.equal(ui.element('hourly-trade-chart').innerHTML, '');
  assert.match(ui.element('hourly-data-tbody').innerHTML, /成交统计暂不可用/);
});

function snapshot(symbol, status = 'RUNNING') {
  return {
    symbol, status, dry_run: true,
    ticker: { symbol }, stats: {}, position: {}, account: {},
    grid_config: { symbol, grid_interval: '1', order_amount_usdc: '100', buy_window: 5, sell_window: 5 },
    active_orders: [], recent_trades: [], recent_logs: [],
  };
}

function overviewData(states, scoped = true) {
  return {
    generated_at: new Date().toISOString(),
    pairs: [...states.values()].map(state => ({
      scoped,
      mode: state.dry_run ? 'PAPER' : state.grid_config.is_testnet ? 'TESTNET' : 'LIVE',
      snapshot: state,
    })),
  };
}

function setAccount(ui, symbol, { mode = 'PAPER', asset = 'USDC', equity = '10000', available = '10000', unrealized = '0', age = 0 } = {}) {
  const data = ui.states.get(symbol);
  data.dry_run = mode === 'PAPER';
  data.grid_config.is_testnet = mode === 'TESTNET';
  data.account = {
    asset, total_wallet_balance: equity, margin_balance: equity,
    available_balance: available, update_time: new Date(ui.clock.now - age).toISOString(),
  };
  data.position.unrealized_pnl = unrealized;
}

function response(data, success = true) {
  return { ok: true, status: 200, json: async () => ({ success, data }) };
}

function deferred() {
  let resolve, reject;
  const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}

function dashboard({ scoped = true, fetchStatus, fetchOverview, fetchControl, confirmResult = true, charts = false } = {}) {
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) {
      const classes = new Set(), attributes = new Map();
      elements.set(id, {
        id, value: '', disabled: false, style: { setProperty() {} }, dataset: {},
        textContent: '', innerHTML: '', children: [],
        classList: {
          add: name => classes.add(name), remove: name => classes.delete(name),
          contains: name => classes.has(name),
          toggle(name, force = !classes.has(name)) { force ? classes.add(name) : classes.delete(name); },
        },
        setAttribute: (name, value) => attributes.set(name, value),
        getAttribute: name => attributes.get(name),
        focus() { context.document.activeElement = this; },
        getClientRects: () => [{}], scrollIntoView() {},
        contains(target) { return this.children.includes(target); },
        replaceChildren(...children) { this.children = children; this.innerHTML = ''; },
      });
    }
    return elements.get(id);
  }
  const tabNames = ['overview', 'detail', 'orders', 'trades', 'config', 'global-config', 'logs'];
  const tabs = tabNames.map(name => {
    const tab = element(`tab-btn-${name}`);
    tab.setAttribute('aria-controls', `tab-${name}`);
    if (name === 'overview') tab.classList.add('active');
    return tab;
  });
  element('console-nav').children = tabs;
  element('overview-status-filter').value = 'all';
  element('overview-mode-filter').value = 'all';
  element('overview-sort').value = 'default';
  const pairs = scoped ? ['SOLUSDC', 'BTCUSDT'] : ['SOLUSDC'];
  const states = new Map(pairs.map(symbol => [symbol, snapshot(symbol)]));
  const requests = [], sockets = [], accountSockets = [], intervals = [], confirmations = [], history = [];
  const clock = { now: Date.now() };
  const storage = new Map([['binance_bot_auth_token', 'test-token']]);
  let singleSymbol = 'SOLUSDC';
  class Clock extends Date { static now() { return clock.now; } }
  class Socket {
    static OPEN = 1;
    constructor(url, protocols) {
      this.readyState = 0;
      this.url = url;
      this.protocols = protocols;
      (url.endsWith('/api/accounts/ws') ? accountSockets : sockets).push(this);
    }
    close() { this.readyState = 3; this.onclose?.(); }
    sendSnapshot(data) {
      this.readyState = Socket.OPEN;
      this.onmessage({ data: JSON.stringify({ type: 'snapshot', data }) });
    }
    sendAccounts(pairs) {
      this.readyState = 1;
      this.onmessage({ data: JSON.stringify({ type: 'accounts', data: { pairs } }) });
    }
  }
  const context = {
    document: {
      activeElement: null, hidden: false, getElementById: element,
      querySelectorAll: selector => selector === '.tab-button' ? tabs
        : selector === '.tab-content' ? tabNames.map(name => element(`tab-${name}`)) : [],
      querySelector: selector => selector === '.tab-nav' ? element('console-nav')
        : selector === '.app-header' ? element('app-header')
        : selector === '.mobile-actions-toggle' ? element('actions-toggle')
        : selector === '.tab-button.active' ? tabs.find(tab => tab.classList.contains('active'))
        : tabs.find(tab => selector === `[aria-controls="${tab.getAttribute('aria-controls')}"]`),
      createElement: () => ({}),
    },
    sessionStorage: {
      getItem: key => storage.get(key) || '', setItem: (key, value) => storage.set(key, value),
      removeItem: key => storage.delete(key),
    },
    window: {
      location: { protocol: 'http:', host: 'localhost', hash: '' }, addEventListener() {}, scrollTo() {},
      history: { pushState(_state, _title, hash) { history.push(hash); context.window.location.hash = hash; } },
    },
    Date: Clock, WebSocket: Socket, console: { log() {}, warn() {}, error() {} },
    setTimeout() {}, setInterval: (fn, ms) => { intervals.push({ fn, ms }); }, requestAnimationFrame: fn => fn(),
    confirm: text => { confirmations.push(text); return confirmResult; }, alert() {},
    fetch: async (url, options = {}) => {
      requests.push({ url, options });
      if (url === '/api/overview') return fetchOverview ? fetchOverview(states) : response(overviewData(
        scoped ? states : new Map([[singleSymbol, states.get(singleSymbol)]]), scoped,
      ));
      if (url === '/api/global/config') return response({ recv_window: 8000, sync_interval_secs: 7, has_api_key: true, api_key_preview: 'glob...key1', has_api_secret: true, telegram_enabled: true, has_telegram_bot_token: true, telegram_chat_id: '123456' });
      if (url === '/api/pairs') {
        if (options.method === 'POST') {
          const config = JSON.parse(options.body);
          if (pairs.includes(config.symbol)) return response(null, false);
          pairs.push(config.symbol);
          states.set(config.symbol, snapshot(config.symbol, 'PAUSED'));
          return response(config.symbol);
        }
        return response(pairs.map(symbol => ({ symbol, scoped })));
      }
      const symbol = scoped ? url.split('/')[3] : singleSymbol;
      if (options.method === 'DELETE') {
        const index = pairs.indexOf(symbol);
        pairs.splice(index, 1);
        states.delete(symbol);
        return response(symbol);
      }
      if (url.endsWith('/config')) return response({
        symbol, grid_interval: '1', order_amount_usdc: '100', buy_window: 5, sell_window: 5, dry_run: true,
      });
      if (url.endsWith('/control')) {
        const action = JSON.parse(options.body).action;
        if (fetchControl) return fetchControl(symbol, action, states);
        states.get(symbol).status = action === 'resume' ? 'RUNNING' : 'PAUSED';
        return response('accepted');
      }
      if (url.endsWith('/status')) {
        return fetchStatus ? fetchStatus(symbol, states) : response(states.get(symbol));
      }
      throw new Error(`Unexpected request: ${url}`);
    },
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  // Exercise the real selection/control code; chart geometry is tested separately by browser QA.
  vm.runInContext(`
    renderLadder = () => {}; renderOrdersTable = () => {};
    renderTradesTable = () => {}; renderLogs = () => {};
  `, context);
  if (!charts) vm.runInContext('renderHourlyTradeChart = () => {};', context);
  return {
    element, requests, sockets, accountSockets, states, intervals, confirmations, clock, history,
    run: code => vm.runInContext(code, context),
    setSingleSymbol: symbol => { singleSymbol = symbol; states.set(symbol, snapshot(symbol)); },
    addExternalPair(symbol, status = 'PAUSED') {
      pairs.push(symbol);
      states.set(symbol, snapshot(symbol, status));
    },
    removeExternalPair(symbol) {
      pairs.splice(pairs.indexOf(symbol), 1);
      states.delete(symbol);
    },
    async poll() {
      intervals.filter(timer => timer.ms === 3000).forEach(timer => timer.fn());
      await new Promise(setImmediate);
    },
  };
}

test('HTTP snapshots enable controls without any WebSocket frames, including after selection', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  assert.equal(ui.element('btn-pause-resume').disabled, false);
  assert.equal(ui.element('btn-cancel-orders').disabled, false);
  assert.ok(ui.requests.some(request => request.url === '/api/pairs/SOLUSDC/status'));
  await ui.run("selectPair('BTCUSDT')");
  assert.equal(ui.element('symbol-badge').textContent, 'BTCUSDT PERP');
  await ui.run("sendControlAction('pause')");
  assert.equal(ui.element('pause-label').textContent, '恢复策略');
  assert.equal(ui.states.get('SOLUSDC').status, 'RUNNING');
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
});

test('cancellation stays available even when both snapshot transports fail', async () => {
  const ui = dashboard({ fetchStatus: async () => { throw new Error('offline'); } });
  await ui.run('initConsole()');
  assert.equal(ui.element('btn-pause-resume').disabled, true);
  assert.equal(ui.element('btn-cancel-orders').disabled, false);
  ui.run('cancelAllOrders()');
  await new Promise(setImmediate);
  assert.equal(ui.states.get('SOLUSDC').status, 'PAUSED');
  assert.ok(ui.requests.some(request => request.url.endsWith('/control')));
});

test('pair-page connection warnings remain visible with the actions menu closed until fresh data arrives', async () => {
  let offline = false;
  const ui = dashboard({ fetchStatus: (symbol, states) => {
    if (offline) throw new Error('offline');
    return response(states.get(symbol));
  } });
  ui.element('header-actions').hidden = true;
  await ui.run('initConsole()');
  await ui.run("openPairPage('SOLUSDC')");
  assert.equal(ui.element('connection-alert').hidden, true);
  assert.equal(ui.element('log-update-status').textContent, '定时刷新');
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC'));
  assert.equal(ui.element('log-update-status').textContent, '自动更新');

  offline = true;
  ui.sockets.at(-1).close();
  assert.equal(ui.element('connection-alert').hidden, false);
  assert.match(ui.element('connection-alert').textContent, /连接中断.*数据可能已过期/);
  assert.equal(ui.element('log-update-status').textContent, '更新中断');
  assert.equal(ui.element('header-actions').hidden, true);

  ui.run('initWebSocket()');
  assert.equal(ui.element('connection-alert').hidden, false, 'a reconnect attempt must not hide the warning');
  const retry = ui.sockets.at(-1);
  retry.readyState = 1;
  retry.onopen();
  assert.equal(ui.element('connection-alert').hidden, false, 'opening a socket alone does not refresh stale data');
  await ui.poll();
  assert.equal(ui.element('connection-alert').hidden, false);
  assert.match(ui.element('connection-alert').textContent, /数据刷新失败/);

  offline = false;
  await ui.poll();
  assert.equal(ui.element('connection-alert').hidden, true);
  assert.equal(ui.element('log-update-status').textContent, '定时刷新');
  retry.sendSnapshot(snapshot('SOLUSDC'));
  assert.equal(ui.element('connection-alert').hidden, true);
  assert.equal(ui.element('log-update-status').textContent, '自动更新');
  assert.equal(ui.element('log-update-indicator').classList.contains('is-disconnected'), false);
});

test('a late HTTP failure cannot replace the healthy state of a fresh WebSocket snapshot', async () => {
  const pending = deferred();
  const ui = dashboard({ fetchStatus: () => pending.promise });
  const initializing = ui.run('initConsole()');
  await new Promise(setImmediate);
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC'));
  pending.reject(new Error('offline'));
  await initializing;
  assert.equal(ui.element('connection-alert').hidden, true);
  assert.equal(ui.element('connection-status').classList.contains('is-disconnected'), false);
  assert.equal(ui.element('log-update-status').textContent, '自动更新');
});

test('polling resumes after WebSocket silence and reflects completed controls', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC'));
  const count = ui.requests.filter(request => request.url.endsWith('/status')).length;
  await ui.poll();
  assert.equal(ui.requests.filter(request => request.url.endsWith('/status')).length, count,
    'fresh WebSocket snapshots should suppress selected-pair HTTP polling');
  ui.clock.now += 6000;
  ui.states.get('SOLUSDC').status = 'PAUSED';
  await ui.poll();
  assert.equal(ui.element('pause-label').textContent, '恢复策略');
  assert.equal(ui.element('btn-pause-resume').disabled, false);
});

test('late HTTP snapshots cannot replace fresher WebSocket state', async () => {
  const pending = deferred();
  const ui = dashboard({ fetchStatus: () => pending.promise });
  const initializing = ui.run('initConsole()');
  await new Promise(setImmediate);
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC', 'PAUSED'));
  pending.resolve(response(snapshot('SOLUSDC', 'RUNNING')));
  await initializing;
  assert.equal(ui.element('pause-label').textContent, '恢复策略');
});

test('late snapshots from the previous selection cannot change the active pair', async () => {
  const pending = deferred();
  const ui = dashboard({ fetchStatus: symbol => symbol === 'SOLUSDC'
    ? pending.promise : response(snapshot('BTCUSDT', 'PAUSED')) });
  const initializing = ui.run('initConsole()');
  await new Promise(setImmediate);
  await ui.run("selectPair('BTCUSDT')");
  pending.resolve(response(snapshot('SOLUSDC', 'RUNNING')));
  await initializing;
  assert.equal(ui.element('symbol-badge').textContent, 'BTCUSDT PERP');
  assert.equal(ui.element('pause-label').textContent, '恢复策略');
});

test('legacy symbol changes update the picker, persisted selection, and cancellation target', async () => {
  const ui = dashboard({ scoped: false });
  await ui.run('initConsole()');
  ui.setSingleSymbol('BTCUSDT');
  await ui.run('loadPairSnapshot()');
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('pair-select').value, 'BTCUSDT');
  assert.equal(ui.element('pair-select').children[0].textContent, 'BTCUSDT');
  assert.equal(ui.run("sessionStorage.getItem('binance_bot_selected_pair')"), 'BTCUSDT');
  ui.run('cancelAllOrders()');
  await new Promise(setImmediate);
  assert.match(ui.confirmations.at(-1), /BTCUSDT/);
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
  assert.equal(ui.states.get('SOLUSDC').status, 'RUNNING');
});

test('late HTTP state from a legacy symbol cannot undo a confirmed configuration change', async () => {
  const pending = deferred();
  let reads = 0;
  const ui = dashboard({ scoped: false, fetchStatus: (symbol, states) => ++reads === 2
    ? pending.promise : response(states.get(symbol)) });
  await ui.run('initConsole()');
  const oldRead = ui.run('loadPairSnapshot()');
  await new Promise(setImmediate);
  ui.setSingleSymbol('BTCUSDT');
  await ui.run('loadConfigForm()');
  pending.resolve(response(snapshot('SOLUSDC')));
  await oldRead;
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('pair-select').value, 'BTCUSDT');
  await ui.run('loadPairSnapshot()');
  assert.equal(ui.element('symbol-badge').textContent, 'BTCUSDT PERP');
});

test('new pair forms ignore a late configuration response from the active strategy', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run('authFetch = () => pendingConfigResponse');
  // Use a deferred promise inside the VM, so this also covers the asynchronous form reader.
  ui.run('pendingConfigResponse = new Promise(resolve => resolveConfigResponse = resolve)');
  const reading = ui.run('loadConfigForm()');
  ui.run('startNewPair()');
  ui.element('cfg-symbol').value = 'ETHUSDT';
  ui.run("resolveConfigResponse({ ok: true, json: async () => ({ success: true, data: { symbol: 'SOLUSDC' } }) })");
  await reading;
  assert.equal(ui.element('cfg-symbol').value, 'ETHUSDT');
  assert.equal(ui.element('cfg-symbol').readOnly, false);
  assert.equal(ui.element('btn-save-cfg').textContent, '保存并添加交易对');
});

test('adding a pair saves through the global API and selects the paused new strategy', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run('startNewPair()');
  ui.element('cfg-symbol').value = 'ethusdt';
  await ui.run('submitConfig()');
  const request = ui.requests.find(request => request.url === '/api/pairs' && request.options.method === 'POST');
  const payload = JSON.parse(request.options.body);
  assert.equal(payload.symbol, 'ETHUSDT');
  for (const field of ['recv_window', 'sync_interval_secs', 'api_key', 'api_secret', 'telegram_enabled', 'telegram_bot_token', 'telegram_chat_id']) {
    assert.equal(field in payload, false, `${field} must be saved only in global settings`);
  }
  assert.equal('save_to_file' in payload, false);
  assert.equal(ui.run('selectedPair'), 'ETHUSDT');
  assert.equal(ui.states.get('ETHUSDT').status, 'PAUSED');
  assert.equal(ui.element('cfg-symbol').readOnly, true);
  assert.equal(ui.element('btn-save-cfg').disabled, false);
});

test('removing the selected pair reloads selection and cancellation targets', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  await ui.run('removeSelectedPair()');
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('pair-select').disabled, true);
  assert.equal(ui.element('btn-remove-pair').disabled, true);
  assert.ok(ui.requests.some(request => request.options.method === 'DELETE' && request.url === '/api/pairs/SOLUSDC'));
  ui.run('cancelAllOrders()');
  await new Promise(setImmediate);
  assert.match(ui.confirmations.at(-1), /BTCUSDT/);
});

test('failed configuration reads leave saving disabled instead of submitting stale values', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run("authFetch = async () => { throw new Error('offline'); }");
  await ui.run('loadConfigForm()');
  const count = ui.requests.length;
  await ui.run('submitConfig()');
  assert.equal(ui.element('btn-save-cfg').disabled, true);
  assert.equal(ui.requests.length, count);
  assert.match(ui.element('cfg-alert').textContent, /配置读取失败/);
});

test('switching and adding pairs preserves unsaved global credentials and notification settings', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  assert.equal(ui.element('cfg-recv-window').value, 8000);
  assert.equal(ui.element('cfg-telegram-chat-id').value, '123456');
  ui.element('cfg-api-key').value = 'unsaved-global-key';
  ui.element('cfg-telegram-chat-id').value = '987654';
  await ui.run("selectPair('BTCUSDT')");
  ui.run('startNewPair()');
  assert.equal(ui.element('cfg-api-key').value, 'unsaved-global-key');
  assert.equal(ui.element('cfg-telegram-chat-id').value, '987654');
  assert.equal(ui.element('cfg-recv-window').value, 8000);
  ui.element('cfg-symbol').value = 'ETHUSDT';
  await ui.run('submitConfig()');
  assert.equal(ui.element('cfg-api-key').value, 'unsaved-global-key');
  assert.equal(ui.element('cfg-telegram-chat-id').value, '987654');
});

test('global settings save independently of the selected pair and never send strategy fields', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  await ui.run("selectPair('BTCUSDT')");
  ui.element('cfg-api-key').value = ' new-global-key ';
  ui.element('cfg-api-secret').value = ' new-global-secret ';
  await ui.run('submitGlobalConfig()');
  const request = ui.requests.find(request => request.url === '/api/global/config' && request.options.method === 'POST');
  const payload = JSON.parse(request.options.body);
  assert.equal(payload.api_key, 'new-global-key');
  assert.equal(payload.api_secret, 'new-global-secret');
  assert.equal(payload.recv_window, 8000);
  assert.equal(payload.telegram_enabled, true);
  for (const field of ['symbol', 'direction', 'grid_interval', 'dry_run', 'is_testnet']) {
    assert.equal(field in payload, false);
  }
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('cfg-api-key').value, '');
  assert.equal(ui.element('btn-save-global').disabled, false);
});

test('failed global reads disable saving to prevent submitting stale global settings', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run('pendingGlobalResponse = new Promise(resolve => resolveGlobalResponse = resolve)');
  ui.run('authFetch = () => pendingGlobalResponse');
  const reading = ui.run('loadGlobalConfigForm()');
  assert.equal(ui.element('btn-save-global').disabled, true);
  const count = ui.requests.length;
  await ui.run('submitGlobalConfig()');
  assert.equal(ui.requests.length, count);
  ui.run("resolveGlobalResponse({ ok: false, json: async () => ({ success: false, message: 'offline' }) })");
  await reading;
  assert.equal(ui.element('btn-save-global').disabled, true);
  assert.match(ui.element('global-cfg-alert').textContent, /全局设置读取失败/);
});

test('direction form loads short, preserves the submitted value, and defaults new pairs to long', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run("fillConfigForm({ symbol: 'SOLUSDC', direction: 'SHORT', grid_interval: '1', order_amount_usdc: '100', buy_window: 3, sell_window: 3, dry_run: true })");
  assert.equal(ui.element('cfg-direction').value, 'SHORT');
  await ui.run('loadConfigForm()');
  assert.equal(ui.element('cfg-direction').value, 'LONG');
  ui.element('cfg-direction').value = 'SHORT';
  await ui.run('submitConfig()');
  const request = ui.requests.find(request => request.url === '/api/pairs/SOLUSDC/config' && request.options.method === 'POST');
  assert.equal(JSON.parse(request.options.body).direction, 'SHORT');
  ui.run('startNewPair()');
  assert.equal(ui.element('cfg-direction').value, 'LONG');
});

test('short position shows positive notional and reserves only closing buys', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  const data = snapshot('SOLUSDC');
  data.grid_config.direction = 'SHORT';
  data.position.size = '-2';
  data.ticker.mark_price = '100';
  data.active_orders = [
    { side: 'BUY', quantity: '1.5' },
    { side: 'SELL', quantity: '3' },
  ];
  ui.states.set('SOLUSDC', data);
  await ui.poll();
  assert.equal(ui.element('card-position-value').textContent, '200.00 USDC');
  assert.equal(ui.element('card-close-reserved-label').textContent, '已挂买平空');
  assert.equal(ui.element('card-sell-reserved').textContent, '1.50 SOL');
  assert.equal(ui.element('card-sell-reserved').className, '');
  assert.match(ui.element('card-window-info').textContent, /做空/);
  data.active_orders[0].quantity = '2.5';
  await ui.poll();
  assert.equal(ui.element('card-sell-reserved').className, 'text-red');
});

test('account balances move above the cards and independent paper wallets are summed by asset', async () => {
  const ui = dashboard();
  setAccount(ui, 'SOLUSDC', { equity: '10000', available: '9000', unrealized: '12.5' });
  setAccount(ui, 'BTCUSDT', { equity: '10000', available: '8000', unrealized: '-2.5' });
  await ui.run('initConsole()');
  const accounts = ui.element('overview-accounts').innerHTML;
  assert.match(accounts, /2 个独立模拟账户合计/);
  assert.match(accounts, /data-account-equity>20,010\.00/);
  assert.match(accounts, /data-account-available>17,000\.00/);
  assert.equal(ui.run('document.title'), '20,010.00|17,000.00');
  assert.doesNotMatch(ui.element('overview-pairs').innerHTML, /pair-account|账户权益|可用余额/);
  assert.equal(ui.element('overview-accounts').getAttribute('aria-busy'), 'false');
  ui.element('overview-search').value = 'SOL';
  ui.run('renderOverview()');
  assert.equal(ui.element('overview-accounts').innerHTML, accounts, 'pair filters must not hide account funds');
  assert.equal(ui.run('document.title'), '20,010.00|17,000.00');
});

test('shared account balances use the newest account observation once regardless of pair order', async () => {
  const ui = dashboard();
  setAccount(ui, 'SOLUSDC', { mode: 'LIVE', equity: '12345', available: '8000', age: 1000 });
  setAccount(ui, 'BTCUSDT', { mode: 'LIVE', equity: '10000', available: '7000', age: 5000 });
  await ui.run('initConsole()');
  const accounts = ui.element('overview-accounts').innerHTML;
  assert.equal((accounts.match(/class="account-summary"/g) || []).length, 1);
  assert.match(accounts, /2 个交易对共用/);
  assert.match(accounts, /data-account-equity>12,345\.00/);
  assert.match(accounts, /data-account-available>8,000\.00/);
  setAccount(ui, 'BTCUSDT', { mode: 'LIVE', equity: '12400', available: '8100' });
  await ui.run('loadOverview()');
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-equity>12,400\.00/);
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-available>8,100\.00/);
  assert.equal(ui.run('document.title'), '12,400.00|8,100.00');
});

test('account overview keeps execution modes and quote assets separate even for identical balances', async () => {
  const ui = dashboard();
  ui.addExternalPair('ETHUSDC');
  ui.addExternalPair('XRPUSDT');
  setAccount(ui, 'SOLUSDC', { mode: 'LIVE' });
  setAccount(ui, 'BTCUSDT', { mode: 'TESTNET' });
  setAccount(ui, 'ETHUSDC');
  setAccount(ui, 'XRPUSDT', { asset: 'USDT' });
  await ui.run('initConsole()');
  const accounts = ui.element('overview-accounts').innerHTML;
  assert.equal((accounts.match(/class="account-summary"/g) || []).length, 4);
  for (const [mode, asset] of [['LIVE', 'USDC'], ['TESTNET', 'USDC'], ['PAPER', 'USDC'], ['PAPER', 'USDT']]) {
    assert.match(accounts, new RegExp(`data-mode="${mode}" data-asset="${asset}"`));
  }
  assert.equal(ui.run('document.title'), Array(4).fill('10,000.00|10,000.00').join(' / '));
  assert.doesNotMatch(accounts, /20,000|40,000/);
});

test('stale shared funds and incomplete paper totals are unavailable while valid zero balances remain visible', async () => {
  const ui = dashboard();
  setAccount(ui, 'SOLUSDC', { mode: 'LIVE', age: 31000 });
  setAccount(ui, 'BTCUSDT', { mode: 'LIVE', age: 60000 });
  await ui.run('initConsole()');
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-equity>--/);
  assert.match(ui.element('overview-accounts').innerHTML, /账户数据待更新/);
  assert.equal(ui.run('document.title'), '--|--');
  setAccount(ui, 'SOLUSDC', { equity: '0', available: '0' });
  setAccount(ui, 'BTCUSDT', { equity: '100', available: undefined });
  delete ui.states.get('BTCUSDT').account.available_balance;
  await ui.run('loadOverview()');
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-equity>100\.00/);
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-available>--/);
  ui.states.get('BTCUSDT').account.available_balance = '0';
  await ui.run('loadOverview()');
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-available>0\.00/);
  assert.doesNotMatch(ui.element('overview-accounts').innerHTML, /账户数据待更新/);
  delete ui.states.get('BTCUSDT').account.total_wallet_balance;
  await ui.run('loadOverview()');
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-equity>--/);
});

test('account overview survives a failed refresh and expires shared balances using account timestamps', async () => {
  let offline = false;
  const ui = dashboard({ fetchOverview: states => {
    if (offline) throw new Error('offline');
    return response(overviewData(states));
  } });
  setAccount(ui, 'SOLUSDC', { mode: 'LIVE' });
  setAccount(ui, 'BTCUSDT', { mode: 'LIVE' });
  await ui.run('initConsole()');
  const accounts = ui.element('overview-accounts').innerHTML;
  offline = true;
  await ui.run('loadOverview()');
  assert.equal(ui.element('overview-accounts').innerHTML, accounts);
  ui.clock.now += 31000;
  ui.run('renderMarkPrice()');
  assert.equal(ui.run('document.title'), '--|--');
  await ui.run('loadOverview()');
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-equity>--/);
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-available>--/);
  assert.equal(ui.element('overview-alert').hidden, false);
});

test('account pushes update the compact title in a background tab without waiting for timers', async () => {
  const ui = dashboard();
  setAccount(ui, 'SOLUSDC', { equity: '100', available: '90' });
  setAccount(ui, 'BTCUSDT', { equity: '200', available: '180' });
  await ui.run('initConsole()');
  assert.equal(ui.run('document.title'), '300.00|270.00');
  ui.run("document.hidden = true; switchTab('tab-logs', document.getElementById('tab-btn-logs'))");
  const requests = ui.requests.length;
  setAccount(ui, 'SOLUSDC', { equity: '150', available: '120', unrealized: '2.5' });
  ui.accountSockets[0].sendAccounts(overviewData(ui.states).pairs);
  assert.equal(ui.run('document.hidden'), true);
  assert.equal(ui.run('document.title'), '352.50|300.00');
  assert.equal(ui.requests.length, requests, 'a pushed update must not depend on HTTP polling');
  assert.match(ui.element('overview-accounts').innerHTML, /data-account-equity>352\.50/);
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC'));
  assert.equal(ui.run('document.title'), '352.50|300.00', 'detail snapshots must not overwrite the balance title');
});

test('account streams recover through HTTP and reconnect without depending on the active page', async () => {
  const ui = dashboard({ scoped: false });
  setAccount(ui, 'SOLUSDC', { equity: '100', available: '90' });
  await ui.run('initConsole()');
  ui.accountSockets[0].sendAccounts(overviewData(ui.states).pairs);
  ui.run("switchTab('tab-global-config', document.getElementById('tab-btn-global-config'))");
  const reads = ui.requests.filter(request => request.url === '/api/overview').length;
  ui.accountSockets[0].close();
  setAccount(ui, 'SOLUSDC', { equity: '250', available: '210' });
  await ui.poll();
  assert.equal(ui.run('document.title'), '250.00|210.00');
  assert.equal(ui.accountSockets.length, 2);
  assert.ok(ui.requests.filter(request => request.url === '/api/overview').length > reads);
});

test('logging out closes the account stream and old session messages cannot restore balances', async () => {
  const ui = dashboard({ scoped: false });
  setAccount(ui, 'SOLUSDC', { equity: '100', available: '90' });
  await ui.run('initConsole()');
  const stream = ui.accountSockets[0];
  stream.sendAccounts(overviewData(ui.states).pairs);
  assert.equal(ui.run('document.title'), '100.00|90.00');
  ui.run("setAuthToken('')");
  assert.equal(stream.readyState, 3);
  stream.sendAccounts(overviewData(ui.states).pairs);
  assert.equal(ui.run('document.title'), '--|--');
  assert.equal(ui.run('accountEntries'), null);
});

test('the overview displays every configured pair and totals all strategies independently of selection', async () => {
  const ui = dashboard();
  const sol = ui.states.get('SOLUSDC');
  const btc = ui.states.get('BTCUSDT');
  sol.active_orders = [{ side: 'BUY', quantity: '1' }];
  btc.active_orders = [{ side: 'BUY', quantity: '1' }, { side: 'SELL', quantity: '1' }];
  btc.status = 'PAUSED';
  const window = { window_start: new Date(ui.clock.now - 3600000).toISOString(), window_end: new Date(ui.clock.now).toISOString() };
  sol.hourly_trade_stats = { ...window, buckets: [{ buy_count: 2, sell_count: 1 }] };
  btc.hourly_trade_stats = { ...window, buckets: [{ buy_count: 1, sell_count: 3 }] };
  await ui.run('initConsole()');
  const cards = ui.element('overview-pairs').innerHTML;
  assert.match(cards, /SOLUSDC/);
  assert.match(cards, /BTCUSDT/);
  assert.equal(String(ui.element('overview-pair-count').textContent), '2');
  assert.equal(String(ui.element('overview-running-count').textContent), '1');
  assert.equal(String(ui.element('overview-order-count').textContent), '3');
  assert.equal(String(ui.element('overview-trade-count').textContent), '7');
  assert.equal(String(ui.element('overview-buy-count').textContent), '3');
  assert.equal(String(ui.element('overview-sell-count').textContent), '4');
  assert.equal(ui.run('selectedPair'), 'SOLUSDC');
  await ui.run("selectPair('BTCUSDT')");
  assert.match(ui.element('overview-pairs').innerHTML, /SOLUSDC/);
  assert.equal(String(ui.element('overview-order-count').textContent), '3');
  ui.element('overview-search').value = 'BTC';
  ui.run('renderOverview()');
  assert.equal(String(ui.element('overview-buy-count').textContent), '3');
  assert.equal(String(ui.element('overview-sell-count').textContent), '4');
});

test('overview search, status, and mode filters combine without changing global totals', async () => {
  const ui = dashboard();
  const btc = ui.states.get('BTCUSDT');
  btc.status = 'PAUSED';
  btc.dry_run = false;
  btc.grid_config.is_testnet = true;
  await ui.run('initConsole()');
  ui.element('overview-search').value = ' btc ';
  ui.element('overview-status-filter').value = 'PAUSED';
  ui.element('overview-mode-filter').value = 'TESTNET';
  ui.run('renderOverview()');
  assert.match(ui.element('overview-pairs').innerHTML, /BTCUSDT/);
  assert.doesNotMatch(ui.element('overview-pairs').innerHTML, /SOLUSDC/);
  ui.element('overview-mode-filter').value = 'PAPER';
  ui.run('renderOverview()');
  assert.doesNotMatch(ui.element('overview-pairs').innerHTML, /BTCUSDT|SOLUSDC/);
  assert.equal(String(ui.element('overview-pair-count').textContent), '2');
  assert.equal(String(ui.element('overview-running-count').textContent), '1');
  ui.element('overview-search').value = '';
  ui.element('overview-status-filter').value = 'all';
  ui.element('overview-mode-filter').value = 'all';
  ui.run('renderOverview()');
  assert.match(ui.element('overview-pairs').innerHTML, /SOLUSDC/);
  assert.match(ui.element('overview-pairs').innerHTML, /BTCUSDT/);
});

test('overview refresh continues while the selected pair has a fresh WebSocket snapshot', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC'));
  ui.states.get('BTCUSDT').status = 'PAUSED';
  const overviewReads = ui.requests.filter(request => request.url === '/api/overview').length;
  const selectedReads = ui.requests.filter(request => request.url === '/api/pairs/SOLUSDC/status').length;
  await ui.poll();
  assert.equal(ui.requests.filter(request => request.url === '/api/overview').length, overviewReads + 1);
  assert.equal(ui.requests.filter(request => request.url === '/api/pairs/SOLUSDC/status').length, selectedReads);
  assert.equal(String(ui.element('overview-running-count').textContent), '1');
  assert.equal(ui.run('selectedPair'), 'SOLUSDC');
});

test('a failed overview refresh retains cards and leaves healthy selected-pair data usable', async () => {
  let offline = false;
  const ui = dashboard({ fetchOverview: states => {
    if (offline) throw new Error('overview unavailable');
    return response(overviewData(states));
  } });
  await ui.run('initConsole()');
  const cards = ui.element('overview-pairs').innerHTML;
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC'));
  offline = true;
  await ui.poll();
  assert.equal(ui.element('overview-pairs').innerHTML, cards);
  assert.equal(ui.element('overview-alert').hidden, false);
  assert.equal(ui.element('connection-alert').hidden, true);
  assert.equal(ui.element('btn-pause-resume').disabled, false);
  offline = false;
  await ui.poll();
  assert.equal(ui.element('overview-alert').hidden, true);
});

test('opening a pair page updates navigation, the picker, and the control target together', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  await ui.run("openPairPage('BTCUSDT', 'tab-orders')");
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('pair-select').value, 'BTCUSDT');
  assert.equal(ui.element('tab-btn-orders').getAttribute('aria-selected'), 'true');
  await ui.run("sendControlAction('pause')");
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
  assert.equal(ui.states.get('SOLUSDC').status, 'RUNNING');
  await ui.run("openPairPage('SOLUSDC', 'tab-detail')");
  assert.equal(ui.element('tab-btn-detail').getAttribute('aria-selected'), 'true');
  assert.equal(ui.element('pair-select').value, 'SOLUSDC');
  await ui.run("openPairPage('BTCUSDT', 'tab-config')");
  assert.equal(ui.element('tab-btn-config').getAttribute('aria-selected'), 'true');
  assert.equal(ui.element('cfg-symbol').value, 'BTCUSDT');
});

test('a late overview response cannot restore a pair removed during its request', async () => {
  const pending = deferred();
  let reads = 0;
  const ui = dashboard({ fetchOverview: states => ++reads === 2
    ? pending.promise : response(overviewData(states)) });
  await ui.run('initConsole()');
  const oldData = overviewData(ui.states);
  const oldRead = ui.run('loadOverview()');
  await new Promise(setImmediate);
  await ui.run('removeSelectedPair()');
  pending.resolve(response(oldData));
  await oldRead;
  assert.equal(String(ui.element('overview-pair-count').textContent), '1');
  assert.match(ui.element('overview-pairs').innerHTML, /BTCUSDT/);
  assert.doesNotMatch(ui.element('overview-pairs').innerHTML, /SOLUSDC/);
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('overview-alert').hidden, true);
});

test('a late overview failure cannot replace a successful refresh after pair-list reload', async () => {
  const pending = deferred();
  let reads = 0;
  const ui = dashboard({ fetchOverview: states => ++reads === 2
    ? pending.promise : response(overviewData(states)) });
  await ui.run('initConsole()');
  const oldRead = ui.run('loadOverview()');
  await new Promise(setImmediate);
  await ui.run('initConsole()');
  pending.reject(new Error('old request failed'));
  await oldRead;
  assert.equal(ui.element('overview-alert').hidden, true);
  assert.equal(ui.element('btn-refresh-overview').disabled, false);
  assert.equal(String(ui.element('overview-pair-count').textContent), '2');
});

test('the overview retains configured pairs as pending cards when its first read fails', async () => {
  const ui = dashboard({ fetchOverview: async () => { throw new Error('offline'); } });
  await ui.run('initConsole()');
  assert.match(ui.element('overview-pairs').innerHTML, /SOLUSDC/);
  assert.match(ui.element('overview-pairs').innerHTML, /BTCUSDT/);
  assert.match(ui.element('overview-pairs').innerHTML, /等待数据/);
  assert.equal(String(ui.element('overview-pair-count').textContent), '2');
  assert.equal(String(ui.element('overview-order-count').textContent), '--');
  assert.equal(ui.element('overview-alert').hidden, false);
  assert.equal(ui.element('overview-pairs').getAttribute('aria-busy'), 'false');
  assert.equal(ui.element('btn-pause-resume').disabled, false);
});

test('overview order and activity sorting retains each pair and reset restores all filters', async () => {
  const ui = dashboard();
  const sol = ui.states.get('SOLUSDC');
  const btc = ui.states.get('BTCUSDT');
  sol.active_orders = [{ side: 'BUY', quantity: '1' }];
  btc.active_orders = [{ side: 'BUY', quantity: '1' }, { side: 'SELL', quantity: '1' }];
  sol.hourly_trade_stats = { window_end: new Date(ui.clock.now).toISOString(), buckets: [{ buy_count: 5, sell_count: 0 }] };
  btc.hourly_trade_stats = { window_end: new Date(ui.clock.now).toISOString(), buckets: [{ buy_count: 0, sell_count: 2 }] };
  await ui.run('initConsole()');
  ui.element('overview-sort').value = 'orders';
  ui.run('renderOverview()');
  let cards = ui.element('overview-pairs').innerHTML;
  assert.ok(cards.indexOf('data-symbol="BTCUSDT"') < cards.indexOf('data-symbol="SOLUSDC"'));
  ui.element('overview-sort').value = 'activity';
  ui.run('renderOverview()');
  cards = ui.element('overview-pairs').innerHTML;
  assert.ok(cards.indexOf('data-symbol="SOLUSDC"') < cards.indexOf('data-symbol="BTCUSDT"'));
  ui.element('overview-search').value = 'missing';
  ui.element('overview-status-filter').value = 'PAUSED';
  ui.element('overview-mode-filter').value = 'LIVE';
  ui.run('resetOverviewFilters()');
  assert.equal(ui.element('overview-search').value, '');
  assert.equal(ui.element('overview-status-filter').value, 'all');
  assert.equal(ui.element('overview-mode-filter').value, 'all');
  assert.equal(ui.element('overview-visible-count').textContent, '2 / 2');
  assert.equal(ui.run('document.activeElement.id'), 'overview-search');
});

test('missing or previous-day trade statistics remain unavailable until every pair is current', async () => {
  const ui = dashboard();
  ui.clock.now = Date.parse('2026-10-16T04:00:00Z');
  const current = { window_end: '2026-10-16T12:00:00+08:00', buckets: [{ buy_count: 2, sell_count: 1 }] };
  ui.states.get('SOLUSDC').hourly_trade_stats = current;
  ui.states.get('BTCUSDT').hourly_trade_stats = { ...current, window_end: '2026-10-15T23:00:00+08:00' };
  await ui.run('initConsole()');
  assert.equal(String(ui.element('overview-trade-count').textContent), '--');
  assert.equal(String(ui.element('overview-buy-count').textContent), '--');
  assert.equal(String(ui.element('overview-sell-count').textContent), '--');
  assert.match(ui.element('overview-trade-note').textContent, /待更新/);
  ui.states.get('BTCUSDT').hourly_trade_stats = current;
  await ui.run('loadOverview()');
  assert.equal(String(ui.element('overview-trade-count').textContent), '6');
  assert.equal(String(ui.element('overview-buy-count').textContent), '4');
  assert.equal(String(ui.element('overview-sell-count').textContent), '2');
  ui.clock.now = Date.parse('2026-10-16T16:00:01Z');
  ui.run('renderOverview()');
  assert.equal(String(ui.element('overview-trade-count').textContent), '--');
  assert.equal(String(ui.element('overview-buy-count').textContent), '--');
  assert.equal(String(ui.element('overview-sell-count').textContent), '--');
});

test('shared-page navigation hides pair actions and returning to detail restores their context', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  for (const id of ['pair-picker', 'mode-badge', 'btn-pause-resume', 'pair-actions']) {
    assert.equal(ui.element(id).hidden, true);
  }
  assert.equal(ui.element('overview-scope').hidden, false);
  await ui.run("openPairPage('BTCUSDT')");
  for (const id of ['pair-picker', 'mode-badge', 'btn-pause-resume', 'pair-actions']) {
    assert.equal(ui.element(id).hidden, false);
  }
  assert.match(ui.element('pair-actions-label').textContent, /BTCUSDT/);
  ui.run("switchTab('tab-global-config', document.getElementById('tab-btn-global-config'))");
  assert.equal(ui.element('pair-actions').hidden, true);
  assert.equal(ui.element('overview-scope').hidden, true);
  ui.run("switchTab('tab-detail', document.getElementById('tab-btn-detail'))");
  assert.equal(ui.element('pair-actions').hidden, false);
  assert.match(ui.element('detail-pair-title').textContent, /BTCUSDT/);
});

test('a slow card selection cannot move the user away from a page chosen afterward', async () => {
  const pending = deferred();
  const ui = dashboard({ fetchStatus: (symbol, states) => symbol === 'BTCUSDT'
    ? pending.promise : response(states.get(symbol)) });
  await ui.run('initConsole()');
  const opening = ui.run("openPairPage('BTCUSDT')");
  await new Promise(setImmediate);
  ui.run("switchTab('tab-global-config', document.getElementById('tab-btn-global-config'))");
  pending.resolve(response(snapshot('BTCUSDT')));
  await opening;
  assert.equal(ui.element('tab-btn-global-config').getAttribute('aria-selected'), 'true');
  assert.equal(ui.element('pair-actions').hidden, true);
});

test('pairs added in another window become selectable without replacing an unsaved strategy form', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  await ui.run("openPairPage('SOLUSDC', 'tab-config')");
  await new Promise(setImmediate);
  ui.element('cfg-interval').value = '7.25';
  ui.addExternalPair('ETHUSDT');
  await ui.run('loadOverview()');
  assert.equal(ui.element('pair-select').children.length, 3);
  assert.ok(ui.element('pair-select').children.some(option => option.value === 'ETHUSDT'));
  assert.equal(ui.element('pair-select').value, 'SOLUSDC');
  assert.equal(ui.element('cfg-interval').value, '7.25');
  assert.equal(ui.element('config-pair-count').textContent, '已配置 3 个交易对');
  assert.match(ui.element('overview-pairs').innerHTML, /ETHUSDT/);
  await ui.run("openPairPage('ETHUSDT')");
  assert.equal(ui.run('selectedPair'), 'ETHUSDT');
  assert.equal(ui.element('pair-select').value, 'ETHUSDT');
  assert.equal(ui.element('tab-btn-detail').getAttribute('aria-selected'), 'true');
  await ui.run("sendControlAction('resume')");
  assert.equal(ui.states.get('ETHUSDT').status, 'RUNNING');
  assert.equal(ui.states.get('SOLUSDC').status, 'RUNNING');
});

test('a selected pair removed in another window falls back to an existing strategy and retires its socket', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  await ui.run("openPairPage('SOLUSDC')");
  const oldSocket = ui.sockets.at(-1);
  ui.removeExternalPair('SOLUSDC');
  await ui.run('loadOverview()');
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('pair-select').value, 'BTCUSDT');
  assert.equal(ui.element('pair-select').children.length, 1);
  assert.equal(ui.element('pair-select').disabled, true);
  assert.equal(ui.element('cfg-symbol').value, 'BTCUSDT');
  assert.match(ui.element('detail-pair-title').textContent, /BTCUSDT/);
  assert.doesNotMatch(ui.element('overview-pairs').innerHTML, /SOLUSDC/);
  oldSocket.sendSnapshot(snapshot('SOLUSDC', 'PAUSED'));
  assert.equal(ui.element('symbol-badge').textContent, 'BTCUSDT PERP');
  await ui.run("sendControlAction('pause')");
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
});

test('external pair changes preserve a new-pair draft and cancelling falls back to an existing strategy', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run('openNewPair()');
  ui.element('cfg-symbol').value = 'XRPUSDT';
  ui.element('cfg-interval').value = '0.0075';
  ui.element('cfg-direction').value = 'SHORT';
  ui.removeExternalPair('SOLUSDC');
  ui.addExternalPair('ETHUSDT');
  await ui.run('loadOverview()');
  assert.equal(ui.run('editingNewPair'), true);
  assert.equal(ui.element('cfg-symbol').value, 'XRPUSDT');
  assert.equal(ui.element('cfg-interval').value, '0.0075');
  assert.equal(ui.element('cfg-direction').value, 'SHORT');
  assert.equal(ui.element('btn-save-cfg').textContent, '保存并添加交易对');
  assert.equal(ui.element('pair-actions').hidden, true);
  assert.equal(ui.element('btn-pause-resume').hidden, true);
  await ui.run('loadConfigForm()');
  assert.equal(ui.run('editingNewPair'), false);
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('cfg-symbol').value, 'BTCUSDT');
  assert.equal(ui.element('pair-select').value, 'BTCUSDT');
  assert.equal(ui.element('pair-actions').hidden, false);
});

test('overview cards preserve nonzero prices and position sizes smaller than four decimals', async () => {
  const ui = dashboard();
  const sol = ui.states.get('SOLUSDC');
  sol.ticker.mark_price = '0.000012345';
  sol.ticker.mark_update_time = new Date(ui.clock.now).toISOString();
  sol.position.size = '-0.00000042';
  await ui.run('initConsole()');
  const card = ui.element('overview-pairs').innerHTML.match(/<article[^>]*data-symbol="SOLUSDC"[\s\S]*?<\/article>/)[0];
  const price = card.match(/class="pair-price">([^<]+)</)[1];
  const position = card.match(/<dt>持仓 <small>SOL<\/small><\/dt><dd>([^<]+)</)[1];
  assert.ok(Number(price) > 0, `the price must remain nonzero: ${price}`);
  assert.ok(Number(position) < 0, `the position must retain its negative amount: ${position}`);
  assert.ok(Math.abs(Number(price) - Number(sol.ticker.mark_price)) / Number(sol.ticker.mark_price) < 0.01);
  assert.equal(Number(position), Number(sol.position.size));
});

function cardMetric(ui, symbol, label) {
  const card = ui.element('overview-pairs').innerHTML.match(new RegExp(`<article[^>]*data-symbol="${symbol}"[\\s\\S]*?</article>`))[0];
  const metric = card.match(new RegExp(`<dt>${label} <small>([^<]+)</small></dt><dd(?: class="([^"]*)")?>([^<]+)([\\s\\S]*?)</dd>`));
  assert.ok(metric, `${symbol} must display ${label}`);
  return { unit: metric[1], color: metric[2] || '', value: metric[3], pending: metric[4].includes('待同步') };
}

test('pair cards value long and short positions at mark price and add raw realized PnL to unrealized PnL', async () => {
  const ui = dashboard();
  const sol = ui.states.get('SOLUSDC');
  const btc = ui.states.get('BTCUSDT');
  sol.ticker = { symbol: 'SOLUSDC', mark_price: '105.25', last_price: '999', mark_update_time: new Date(ui.clock.now).toISOString() };
  sol.position = { size: '2.5', unrealized_pnl: '-4.25' };
  sol.stats = { total_realized_pnl: '10.5', total_commission: '3' };
  btc.ticker = { symbol: 'BTCUSDT', mark_price: '60000', mark_update_time: new Date(ui.clock.now).toISOString() };
  btc.position = { size: '-0.125', unrealized_pnl: '12.5' };
  btc.stats = { total_realized_pnl: '-20' }; // Missing commission must not hide realized PnL.
  await ui.run('initConsole()');
  assert.equal(cardMetric(ui, 'SOLUSDC', '持仓金额').value, '263.13');
  assert.equal(cardMetric(ui, 'SOLUSDC', '持仓金额').unit, 'USDC');
  assert.equal(cardMetric(ui, 'BTCUSDT', '持仓金额').value, '7,500.00');
  assert.equal(cardMetric(ui, 'BTCUSDT', '持仓金额').unit, 'USDT');
  assert.equal(cardMetric(ui, 'SOLUSDC', '已实现盈亏').value, '+10.5000');
  assert.equal(cardMetric(ui, 'SOLUSDC', '累计盈亏').value, '+6.2500');
  assert.equal(cardMetric(ui, 'SOLUSDC', '累计盈亏').color, 'text-green');
  assert.equal(cardMetric(ui, 'BTCUSDT', '已实现盈亏').value, '-20.0000');
  assert.equal(cardMetric(ui, 'BTCUSDT', '累计盈亏').value, '-7.5000');
  assert.equal(cardMetric(ui, 'BTCUSDT', '累计盈亏').color, 'text-red');
  assert.doesNotMatch(ui.element('overview-pairs').innerHTML, /累计扣费后/);
});

test('card totals preserve real zeroes and keep missing or invalid operands unavailable', async () => {
  const ui = dashboard({ scoped: false });
  const sol = ui.states.get('SOLUSDC');
  sol.ticker = { symbol: 'SOLUSDC', mark_price: '105', mark_update_time: new Date(ui.clock.now).toISOString() };
  sol.position = { size: '0', unrealized_pnl: '0' };
  sol.stats = { total_realized_pnl: '0' };
  await ui.run('initConsole()');
  assert.equal(cardMetric(ui, 'SOLUSDC', '持仓金额').value, '0.00');
  assert.equal(cardMetric(ui, 'SOLUSDC', '累计盈亏').value, '0.0000');
  for (const invalid of [undefined, null, '', 'invalid', 'Infinity']) {
    sol.position.size = invalid;
    sol.position.unrealized_pnl = invalid;
    await ui.run('loadOverview()');
    assert.equal(cardMetric(ui, 'SOLUSDC', '持仓金额').value, '--');
    assert.equal(cardMetric(ui, 'SOLUSDC', '累计盈亏').value, '--');
    sol.position.unrealized_pnl = '5';
    sol.stats.total_realized_pnl = invalid;
    await ui.run('loadOverview()');
    assert.equal(cardMetric(ui, 'SOLUSDC', '已实现盈亏').value, '--');
    assert.equal(cardMetric(ui, 'SOLUSDC', '累计盈亏').value, '--');
    sol.stats.total_realized_pnl = '0';
  }
});

test('position valuation requires a fresh matching mark price and pending realized PnL marks both totals', async () => {
  const ui = dashboard({ scoped: false });
  const sol = ui.states.get('SOLUSDC');
  sol.position = { size: '-2', unrealized_pnl: '5' };
  sol.stats = { total_realized_pnl: '10', pending_pnl_trades: 1 };
  sol.ticker = { symbol: 'SOLUSDC', mark_price: '105', mark_update_time: new Date(ui.clock.now).toISOString() };
  await ui.run('initConsole()');
  assert.equal(cardMetric(ui, 'SOLUSDC', '持仓金额').value, '210.00');
  assert.equal(cardMetric(ui, 'SOLUSDC', '已实现盈亏').pending, true);
  assert.equal(cardMetric(ui, 'SOLUSDC', '累计盈亏').pending, true);
  for (const update of [
    { mark_update_time: new Date(ui.clock.now - 15000).toISOString() },
    { symbol: 'BTCUSDT' }, { mark_price: 'Infinity' }, { mark_price: null }, { mark_price: '0' },
  ]) {
    sol.ticker = { symbol: 'SOLUSDC', mark_price: '105', mark_update_time: new Date(ui.clock.now).toISOString(), ...update };
    await ui.run('loadOverview()');
    assert.equal(cardMetric(ui, 'SOLUSDC', '持仓金额').value, '--');
    assert.equal(cardMetric(ui, 'SOLUSDC', '累计盈亏').value, '+15.0000');
  }
});

test('external removal while a strategy save is pending falls back after saving finishes', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.run(`
    pendingSave = new Promise(resolve => resolveSave = resolve);
    originalAuthFetch = authFetch;
    authFetch = (url, options) => url === '/api/pairs/SOLUSDC/config' && options?.method === 'POST'
      ? pendingSave : originalAuthFetch(url, options);
  `);
  const saving = ui.run('submitConfig()');
  await new Promise(setImmediate);
  ui.removeExternalPair('SOLUSDC');
  await ui.run('loadOverview()');
  assert.equal(ui.run('pairManagementBusy'), true);
  ui.run("resolveSave({ ok: false, json: async () => ({ success: false, message: 'pair removed' }) })");
  await saving;
  await ui.run('loadOverview()');
  assert.equal(ui.run('selectedPair'), 'BTCUSDT');
  assert.equal(ui.element('pair-select').value, 'BTCUSDT');
  assert.equal(ui.element('cfg-symbol').value, 'BTCUSDT');
});

test('card controls address their own pair without changing the selected detail or navigation', async () => {
  const ui = dashboard();
  ui.states.get('BTCUSDT').status = 'PAUSED';
  await ui.run('initConsole()');
  await ui.run("controlOverviewPair('BTCUSDT', 'resume')");
  assert.equal(ui.states.get('BTCUSDT').status, 'RUNNING');
  assert.equal(ui.run('selectedPair'), 'SOLUSDC');
  assert.equal(ui.element('pair-select').value, 'SOLUSDC');
  assert.equal(ui.run('activeTab'), 'tab-overview');
  assert.equal(ui.run("overviewControls.get('BTCUSDT').confirmed"), true);
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).at(-1).url, '/api/pairs/BTCUSDT/control');
  await ui.run("controlOverviewPair('BTCUSDT', 'pause')");
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
  assert.equal(ui.states.get('SOLUSDC').status, 'RUNNING');
  assert.match(ui.element('overview-pairs').innerHTML, /暂停已确认/);
});

test('start all includes filtered-out paused pairs and skips strategies already running', async () => {
  const ui = dashboard();
  ui.states.get('SOLUSDC').status = 'PAUSED';
  ui.states.get('BTCUSDT').status = 'PAUSED';
  ui.addExternalPair('ETHUSDT', 'RUNNING');
  await ui.run('initConsole()');
  ui.element('overview-search').value = 'SOL';
  ui.run('renderOverview()');
  await ui.run("controlAllOverviewPairs('resume')");
  const requests = ui.requests.filter(request => request.url.endsWith('/control'));
  assert.equal(requests.length, 2);
  assert.deepEqual(requests.map(request => request.url).sort(), ['/api/pairs/BTCUSDT/control', '/api/pairs/SOLUSDC/control']);
  assert.equal(ui.states.get('SOLUSDC').status, 'RUNNING');
  assert.equal(ui.states.get('BTCUSDT').status, 'RUNNING');
  assert.equal(ui.run('selectedPair'), 'SOLUSDC');
  assert.equal(ui.element('overview-search').value, 'SOL');
  assert.match(ui.element('overview-control-scope').textContent, /全部 3 个交易对/);
  assert.match(ui.element('overview-control-report').textContent, /2 个已确认/);
  assert.equal(ui.element('btn-overview-resume-all').disabled, true);
  assert.equal(ui.element('btn-overview-pause-all').disabled, false);
});

test('pause all also cleans paused pairs with remaining orders and is usable without overview snapshots', async () => {
  const ui = dashboard({ fetchOverview: async () => { throw new Error('snapshot unavailable'); } });
  await ui.run('initConsole()');
  assert.equal(ui.element('btn-overview-resume-all').disabled, true);
  assert.equal(ui.element('btn-overview-pause-all').disabled, false);
  await ui.run("controlAllOverviewPairs('pause')");
  assert.equal(ui.states.get('SOLUSDC').status, 'PAUSED');
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 2);

  const healthy = dashboard();
  healthy.states.get('BTCUSDT').status = 'PAUSED';
  healthy.states.get('BTCUSDT').active_orders = [{ side: 'BUY' }];
  await healthy.run('initConsole()');
  await healthy.run("controlAllOverviewPairs('pause')");
  assert.equal(healthy.requests.filter(request => request.url.endsWith('/control')).length, 2);
  assert.match(healthy.element('overview-control-report').textContent, /2 个已确认/);
});

test('batch failures retain per-pair results and failed strategies can be retried individually', async () => {
  let failBtc = true;
  const ui = dashboard({ fetchControl: (symbol, action, states) => {
    if (symbol === 'BTCUSDT' && failBtc) return { ok: false, json: async () => ({ success: false, message: '拒绝启动 <retry>' }) };
    states.get(symbol).status = action === 'resume' ? 'RUNNING' : 'PAUSED';
    return response('accepted');
  } });
  ui.states.get('SOLUSDC').status = 'PAUSED';
  ui.states.get('BTCUSDT').status = 'PAUSED';
  await ui.run('initConsole()');
  await ui.run("controlAllOverviewPairs('resume')");
  assert.equal(ui.states.get('SOLUSDC').status, 'RUNNING');
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
  assert.match(ui.element('overview-control-report').textContent, /1 个已确认.*1 个未完成/);
  assert.match(ui.element('overview-pairs').innerHTML, /拒绝启动 &lt;retry&gt;/);
  failBtc = false;
  await ui.run("controlOverviewPair('BTCUSDT', 'resume')");
  assert.equal(ui.run("overviewControls.get('BTCUSDT').confirmed"), true);
  assert.equal(ui.states.get('BTCUSDT').status, 'RUNNING');
});

test('an in-flight card operation blocks duplicate card and batch submissions until confirmed', async () => {
  const pending = deferred();
  const ui = dashboard({ fetchControl: async (symbol, action, states) => {
    await pending.promise;
    states.get(symbol).status = action === 'resume' ? 'RUNNING' : 'PAUSED';
    return response('accepted');
  } });
  ui.states.get('BTCUSDT').status = 'PAUSED';
  await ui.run('initConsole()');
  const starting = ui.run("controlOverviewPair('BTCUSDT', 'resume')");
  await new Promise(setImmediate);
  assert.equal(ui.run("overviewControls.get('BTCUSDT').pending"), true);
  assert.equal(ui.element('btn-overview-pause-all').disabled, true);
  await ui.run("controlOverviewPair('BTCUSDT', 'resume')");
  await ui.run("controlAllOverviewPairs('pause')");
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 1);
  pending.resolve();
  await starting;
  assert.equal(ui.run("overviewControls.get('BTCUSDT').confirmed"), true);
  assert.equal(ui.element('btn-overview-pause-all').disabled, false);
});

test('accepted controls remain pending until new state confirms them and do not automatically retry', async () => {
  const ui = dashboard({ fetchControl: () => response('queued') });
  ui.states.get('BTCUSDT').status = 'PAUSED';
  await ui.run('initConsole()');
  await ui.run("controlOverviewPair('BTCUSDT', 'resume')");
  assert.equal(ui.run("overviewControls.get('BTCUSDT').pending"), true);
  assert.equal(ui.run("overviewControls.get('BTCUSDT').confirmed"), false);
  assert.equal(ui.states.get('BTCUSDT').status, 'PAUSED');
  ui.clock.now += 15001;
  await ui.run('loadOverview()');
  assert.equal(ui.run("overviewControls.get('BTCUSDT').pending"), false);
  assert.match(ui.element('overview-pairs').innerHTML, /启动尚未确认，请查看运行日志/);
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 1);
});

test('an overview read started before control acknowledgement cannot confirm its outcome', async () => {
  const oldRead = deferred();
  let reads = 0;
  const ui = dashboard({ fetchOverview: states => ++reads === 2 ? oldRead.promise : response(overviewData(states)) });
  ui.states.get('BTCUSDT').status = 'PAUSED';
  await ui.run('initConsole()');
  const reading = ui.run('loadOverview()');
  await ui.run("controlOverviewPair('BTCUSDT', 'resume')");
  oldRead.resolve(response(overviewData(ui.states)));
  await reading;
  assert.equal(ui.run("overviewControls.get('BTCUSDT').pending"), true);
  await ui.run('loadOverview()');
  assert.equal(ui.run("overviewControls.get('BTCUSDT').confirmed"), true);
});

test('live starts identify their scope for confirmation and cancellation sends no controls', async () => {
  const ui = dashboard({ confirmResult: false });
  for (const state of ui.states.values()) { state.status = 'PAUSED'; state.dry_run = false; }
  await ui.run('initConsole()');
  await ui.run("controlOverviewPair('BTCUSDT', 'resume')");
  await ui.run("controlAllOverviewPairs('resume')");
  assert.match(ui.confirmations[0], /BTCUSDT.*实盘/);
  assert.match(ui.confirmations[1], /全部 2 个.*2 个实盘.*筛选外/);
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 0);
  assert.equal(ui.run('overviewControls.size'), 0);
});

test('legacy card and bulk controls use the unscoped control endpoint', async () => {
  const ui = dashboard({ scoped: false });
  ui.states.get('SOLUSDC').status = 'PAUSED';
  await ui.run('initConsole()');
  await ui.run("controlOverviewPair('SOLUSDC', 'resume')");
  await ui.run("controlAllOverviewPairs('pause')");
  const requests = ui.requests.filter(request => request.url.endsWith('/control'));
  assert.equal(requests.length, 2);
  assert.ok(requests.every(request => request.url === '/api/control'));
  assert.equal(ui.states.get('SOLUSDC').status, 'PAUSED');
});

test('large batch controls limit concurrent requests while processing every strategy', async () => {
  let active = 0, maxActive = 0;
  const ui = dashboard({ fetchControl: async (symbol, action, states) => {
    active++;
    maxActive = Math.max(maxActive, active);
    await new Promise(setImmediate);
    states.get(symbol).status = 'PAUSED';
    active--;
    return response('accepted');
  } });
  for (const symbol of ['ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'DOGEUSDT']) ui.addExternalPair(symbol, 'RUNNING');
  await ui.run('initConsole()');
  await ui.run("controlAllOverviewPairs('pause')");
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 6);
  assert.equal(maxActive, 4);
  assert.match(ui.element('overview-control-report').textContent, /6 个已确认/);
});

test('timed-out or lost responses are checked against strategy state without repeating commands', async () => {
  for (const name of ['TimeoutError', 'TypeError']) {
    const ui = dashboard({ fetchControl: (symbol, action, states) => {
      states.get(symbol).status = 'RUNNING';
      const error = new Error('response lost');
      error.name = name;
      throw error;
    } });
    ui.states.get('BTCUSDT').status = 'PAUSED';
    await ui.run('initConsole()');
    await ui.run("controlOverviewPair('BTCUSDT', 'resume')");
    assert.equal(ui.run("overviewControls.get('BTCUSDT').confirmed"), true);
    assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 1);
  }
});

test('queued batch requests stop when the login session changes', async () => {
  const ui = dashboard({ fetchControl: () => {
    ui.run("setAuthToken('')");
    return response('queued');
  } });
  for (const symbol of ['ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'DOGEUSDT']) ui.addExternalPair(symbol, 'RUNNING');
  await ui.run('initConsole()');
  await ui.run("controlAllOverviewPairs('pause')");
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 1);
  assert.equal(ui.element('btn-overview-resume-all').disabled, true);
  assert.equal(ui.element('btn-overview-pause-all').disabled, true);
  assert.match(ui.element('overview-control-report').textContent, /6 个未完成/);
});

test('a queued start is skipped if its strategy changes execution mode before dispatch', async () => {
  const gate = deferred();
  const ui = dashboard({ fetchControl: async (symbol, action, states) => {
    await gate.promise;
    states.get(symbol).status = 'RUNNING';
    return response('accepted');
  } });
  for (const symbol of ['ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'DOGEUSDT']) ui.addExternalPair(symbol);
  for (const state of ui.states.values()) state.status = 'PAUSED';
  await ui.run('initConsole()');
  const starting = ui.run("controlAllOverviewPairs('resume')");
  await new Promise(setImmediate);
  assert.equal(ui.requests.filter(request => request.url.endsWith('/control')).length, 4);
  ui.states.get('BNBUSDT').dry_run = false;
  await ui.run('loadOverview()');
  gate.resolve();
  await starting;
  const controls = ui.requests.filter(request => request.url.endsWith('/control'));
  assert.equal(controls.length, 5);
  assert.ok(controls.every(request => request.url !== '/api/pairs/BNBUSDT/control'));
  assert.equal(ui.states.get('BNBUSDT').status, 'PAUSED');
  assert.match(ui.element('overview-control-report').textContent, /5 个已确认.*1 个未完成/);
  assert.match(ui.element('overview-pairs').innerHTML, /交易模式已变更/);
});
