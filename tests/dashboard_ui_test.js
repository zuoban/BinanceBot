const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../src/server/dashboard.html'), 'utf8')
  .match(/<script>([\s\S]*?)<\/script>/)[1];

function snapshot(symbol, status = 'RUNNING') {
  return {
    symbol, status, dry_run: true,
    ticker: { symbol }, stats: {}, position: {}, account: {},
    grid_config: { symbol, grid_interval: '1', order_amount_usdc: '100', buy_window: 5, sell_window: 5 },
    active_orders: [], recent_trades: [], recent_logs: [],
  };
}

function response(data, success = true) {
  return { ok: true, status: 200, json: async () => ({ success, data }) };
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

function dashboard({ scoped = true, fetchStatus } = {}) {
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) elements.set(id, {
      value: '', disabled: false, style: {}, textContent: '', innerHTML: '', children: [],
      classList: { add() {}, remove() {} },
      setAttribute() {}, focus() {}, replaceChildren(...children) { this.children = children; },
    });
    return elements.get(id);
  }
  const pairs = scoped ? ['SOLUSDC', 'BTCUSDT'] : ['SOLUSDC'];
  const states = new Map(pairs.map(symbol => [symbol, snapshot(symbol)]));
  const requests = [], sockets = [], intervals = [], confirmations = [];
  const clock = { now: Date.now() };
  const storage = new Map([['binance_bot_auth_token', 'test-token']]);
  let singleSymbol = 'SOLUSDC';
  class Clock extends Date { static now() { return clock.now; } }
  class Socket {
    static OPEN = 1;
    constructor() { this.readyState = 0; sockets.push(this); }
    close() { this.readyState = 3; this.onclose?.(); }
    sendSnapshot(data) {
      this.readyState = Socket.OPEN;
      this.onmessage({ data: JSON.stringify({ type: 'snapshot', data }) });
    }
  }
  const context = {
    document: { getElementById: element, querySelectorAll: () => [], createElement: () => ({}) },
    sessionStorage: {
      getItem: key => storage.get(key) || '', setItem: (key, value) => storage.set(key, value),
      removeItem: key => storage.delete(key),
    },
    window: { location: { protocol: 'http:', host: 'localhost' }, addEventListener() {} },
    Date: Clock, WebSocket: Socket, console: { log() {}, warn() {}, error() {} },
    setTimeout() {}, setInterval: (fn, ms) => { intervals.push({ fn, ms }); },
    confirm: text => { confirmations.push(text); return true; }, alert() {},
    fetch: async (url, options = {}) => {
      requests.push({ url, options });
      if (url === '/api/global/config') return response({ recv_window: 8000, sync_interval_secs: 7, has_api_key: true, api_key_preview: 'glob...key1', has_api_secret: true, telegram_enabled: true, has_telegram_bot_token: true, telegram_chat_id: '123456' });
      if (url === '/api/server/config') return response({ saved: { host: '0.0.0.0', port: 8080 }, active: { host: '0.0.0.0', port: 8080 } });
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
    renderTradesTable = () => {}; renderHourlyTradeChart = () => {}; renderLogs = () => {};
  `, context);
  return {
    element, requests, sockets, states, intervals, confirmations, clock,
    run: code => vm.runInContext(code, context),
    setSingleSymbol: symbol => { singleSymbol = symbol; states.set(symbol, snapshot(symbol)); },
    async poll() {
      intervals.find(timer => timer.ms === 3000).fn();
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

test('polling resumes after WebSocket silence and reflects completed controls', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.sockets.at(-1).sendSnapshot(snapshot('SOLUSDC'));
  const count = ui.requests.length;
  await ui.poll();
  assert.equal(ui.requests.length, count, 'fresh WebSocket snapshots should suppress HTTP polling');
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

test('server settings send an independent request and explain restart requirements', async () => {
  const ui = dashboard();
  await ui.run('initConsole()');
  ui.element('cfg-server-host').value = '127.0.0.1';
  ui.element('cfg-server-port').value = '9090';
  await ui.run('submitServerConfig()');
  const request = ui.requests.find(request => request.url === '/api/server/config' && request.options.method === 'POST');
  assert.deepEqual(JSON.parse(request.options.body), { host: '127.0.0.1', port: 9090 });
  assert.equal(ui.element('btn-save-server').disabled, false);
  assert.equal(ui.run('selectedPair'), 'SOLUSDC');
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
