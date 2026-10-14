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
      setAttribute() {}, replaceChildren(...children) { this.children = children; },
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
      if (url === '/api/pairs') return response(pairs.map(symbol => ({ symbol, scoped })));
      const symbol = scoped ? url.split('/')[3] : singleSymbol;
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
    renderMarkPrice = () => {}; renderLadder = () => {}; renderOrdersTable = () => {};
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
