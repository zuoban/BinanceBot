pub const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Binance Futures Grid Bot | SOLUSDC</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: #111827;
      --card-border: #1f293d;
      --card-hover: #162035;
      --text: #f3f4f6;
      --text-muted: #9ca3af;
      --text-dim: #6b7280;
      --green: #10b981;
      --green-bg: rgba(16, 185, 129, 0.12);
      --red: #f43f5e;
      --red-bg: rgba(244, 63, 94, 0.12);
      --accent: #3b82f6;
      --accent-glow: rgba(59, 130, 246, 0.25);
      --purple: #8b5cf6;
      --purple-bg: rgba(139, 92, 246, 0.15);
      --amber: #f59e0b;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      background-color: var(--bg);
      color: var(--text);
      line-height: 1.5;
      font-variant-numeric: tabular-nums;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }

    header {
      background: rgba(17, 24, 39, 0.85);
      backdrop-filter: blur(12px);
      border-bottom: 1px solid var(--card-border);
      padding: 12px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 100;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .brand-icon {
      width: 32px;
      height: 32px;
      border-radius: 8px;
      background: linear-gradient(135deg, #f59e0b, #e11d48);
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 900;
      color: #fff;
      font-size: 16px;
      box-shadow: 0 0 15px rgba(245, 158, 11, 0.3);
    }

    .brand-title {
      font-size: 18px;
      font-weight: 700;
      letter-spacing: -0.02em;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .badge {
      font-size: 11px;
      padding: 2px 8px;
      border-radius: 9999px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .badge-green { background: var(--green-bg); color: var(--green); border: 1px solid var(--green); }
    .badge-red { background: var(--red-bg); color: var(--red); border: 1px solid var(--red); }
    .badge-purple { background: var(--purple-bg); color: var(--purple); border: 1px solid var(--purple); }
    .badge-amber { background: rgba(245, 158, 11, 0.15); color: var(--amber); border: 1px solid var(--amber); }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    button {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      color: var(--text);
      padding: 7px 14px;
      border-radius: 6px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.2s ease;
    }

    button:hover {
      background: var(--card-hover);
      border-color: #374151;
    }

    button.btn-primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
      box-shadow: 0 0 10px var(--accent-glow);
    }
    button.btn-primary:hover { background: #2563eb; }

    button.btn-danger {
      background: rgba(244, 63, 94, 0.15);
      border-color: var(--red);
      color: var(--red);
    }
    button.btn-danger:hover { background: rgba(244, 63, 94, 0.25); }

    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--green);
      display: inline-block;
      box-shadow: 0 0 8px var(--green);
      animation: pulse 2s infinite;
    }

    @keyframes pulse {
      0% { opacity: 0.4; }
      50% { opacity: 1; }
      100% { opacity: 0.4; }
    }

    main {
      flex: 1;
      padding: 20px 24px;
      max-width: 1440px;
      margin: 0 auto;
      width: 100%;
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    /* Metric Cards Grid */
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 14px;
    }

    .metric-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 16px;
      position: relative;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .metric-title {
      font-size: 12px;
      color: var(--text-muted);
      font-weight: 500;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 6px;
    }

    .metric-value {
      font-size: 24px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }

    .metric-sub {
      font-size: 12px;
      color: var(--text-muted);
      margin-top: 4px;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    /* Visual Grid Ladder */
    .ladder-section {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      padding: 18px 20px;
    }

    .section-title {
      font-size: 14px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      margin-bottom: 14px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .ladder-container {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .ladder-row {
      display: grid;
      grid-template-columns: 80px 140px 100px 120px 1fr 100px;
      align-items: center;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 13px;
      position: relative;
      overflow: hidden;
    }

    .ladder-row.sell {
      background: rgba(244, 63, 94, 0.05);
      border-left: 3px solid var(--red);
    }

    .ladder-row.buy {
      background: rgba(16, 185, 129, 0.05);
      border-left: 3px solid var(--green);
    }

    .ladder-current {
      background: linear-gradient(90deg, rgba(59, 130, 246, 0.15), rgba(139, 92, 246, 0.15));
      border: 1px dashed var(--accent);
      border-radius: 6px;
      padding: 10px 14px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin: 6px 0;
      font-weight: 700;
      color: #fff;
    }

    .depth-bar {
      position: absolute;
      top: 0;
      bottom: 0;
      right: 0;
      opacity: 0.15;
      pointer-events: none;
    }

    .depth-bar.sell { background: var(--red); }
    .depth-bar.buy { background: var(--green); }

    /* Tabs Panel */
    .tabs-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      overflow: hidden;
      flex: 1;
    }

    .tab-nav {
      display: flex;
      background: rgba(13, 19, 33, 0.7);
      border-bottom: 1px solid var(--card-border);
      padding: 0 12px;
    }

    .tab-button {
      background: transparent;
      border: none;
      border-bottom: 2px solid transparent;
      border-radius: 0;
      padding: 12px 18px;
      color: var(--text-muted);
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
    }

    .tab-button.active {
      color: #fff;
      border-bottom-color: var(--accent);
    }

    .tab-content {
      padding: 16px 20px;
      display: none;
    }

    .tab-content.active {
      display: block;
    }

    /* Tables */
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    th {
      text-align: left;
      padding: 8px 12px;
      color: var(--text-dim);
      font-weight: 600;
      border-bottom: 1px solid var(--card-border);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    td {
      padding: 10px 12px;
      border-bottom: 1px solid rgba(31, 41, 61, 0.5);
    }

    tr:hover td {
      background: var(--card-hover);
    }

    .text-green { color: var(--green); }
    .text-red { color: var(--red); }
    .text-muted { color: var(--text-muted); }

    /* Log Console */
    .log-console {
      background: #060910;
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 12px 16px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      height: 280px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .log-line {
      display: flex;
      gap: 10px;
      line-height: 1.4;
    }

    .log-time { color: var(--text-dim); flex-shrink: 0; }
    .log-badge { font-weight: 700; padding: 0 4px; border-radius: 3px; font-size: 10px; }
    .log-badge.INFO { background: rgba(59, 130, 246, 0.2); color: #60a5fa; }
    .log-badge.SUCCESS { background: rgba(16, 185, 129, 0.2); color: var(--green); }
    .log-badge.WARN { background: rgba(245, 158, 11, 0.2); color: var(--amber); }
    .log-badge.ERROR { background: rgba(244, 63, 94, 0.2); color: var(--red); }
    .log-msg { color: #d1d5db; word-break: break-all; }

    /* Interactive Config Form */
    .config-container {
      max-width: 960px;
      margin: 0 auto;
    }

    .config-header {
      margin-bottom: 18px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--card-border);
    }

    .config-form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px 24px;
    }

    .form-group {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }

    .form-group label {
      font-size: 13px;
      font-weight: 600;
      color: var(--text);
    }

    .form-hint {
      font-size: 11px;
      color: var(--text-dim);
    }

    input[type="text"],
    input[type="number"],
    input[type="password"],
    select {
      background: #0b1120;
      border: 1px solid var(--card-border);
      border-radius: 6px;
      color: var(--text);
      padding: 9px 12px;
      font-size: 13px;
      outline: none;
      transition: border-color 0.2s;
    }

    input:focus, select:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 2px var(--accent-glow);
    }

    .form-actions {
      margin-top: 24px;
      padding-top: 18px;
      border-top: 1px solid var(--card-border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }

    .modal-overlay {
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: rgba(10, 14, 23, 0.88);
      backdrop-filter: blur(14px);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 99999;
      padding: 16px;
    }
    .modal-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 28px 24px;
      width: 100%;
      max-width: 440px;
      box-shadow: 0 24px 48px rgba(0, 0, 0, 0.6);
      animation: modalFadeIn 0.2s cubic-bezier(0.16, 1, 0.3, 1);
    }
    @keyframes modalFadeIn {
      from { opacity: 0; transform: translateY(-12px) scale(0.98); }
      to { opacity: 1; transform: translateY(0) scale(1); }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="brand-icon">⚡</div>
      <div class="brand-title">
        <span>Binance Grid Bot</span>
        <span class="badge badge-purple" id="symbol-badge">SOLUSDC PERP</span>
        <span class="badge badge-green" id="status-badge"><span class="pulse-dot"></span> RUNNING</span>
        <span class="badge badge-amber" id="mode-badge">PAPER TRADING</span>
      </div>
    </div>
    <div class="header-actions">
      <button class="btn-primary" onclick="openConfigTab()">⚙️ 修改策略参数</button>
      <button id="btn-pause-resume" onclick="togglePauseResume()">⏸️ 暂停策略</button>
      <button onclick="rebalanceGrid()">🔄 刷新网格</button>
      <button class="btn-danger" onclick="cancelAllOrders()">🛑 撤销全部挂单</button>
      <button onclick="openChangePasswordModal()" title="修改管理员密码">🔑 修改密码</button>
      <button onclick="logout()" title="安全退出登录" style="color: var(--red); border-color: rgba(244, 63, 94, 0.4);">🚪 退出</button>
    </div>
  </header>

  <main>
    <!-- Top Metric Cards -->
    <div class="metrics-grid">
      <div class="metric-card">
        <div class="metric-title">SOLUSDC 标记价格</div>
        <div class="metric-value" id="card-price">--</div>
        <div class="metric-sub">
          <span id="card-price-change">--</span>
          <span style="color: var(--text-dim);">| 24h 高: <span id="card-high">--</span> 低: <span id="card-low">--</span></span>
        </div>
      </div>

      <div class="metric-card">
        <div class="metric-title">网格已实现套利总利润</div>
        <div class="metric-value text-green" id="card-profit">+0.00 USDC</div>
        <div class="metric-sub">
          <span>完成套利循环: <strong id="card-cycles">0</strong> 次</span>
        </div>
      </div>

      <div class="metric-card">
        <div class="metric-title">当前持仓与未实现盈亏</div>
        <div class="metric-value" id="card-position">0.00 SOL</div>
        <div class="metric-sub">
          <span>未结盈亏: <span id="card-unrealized">0.00 USDC</span></span>
          <span style="color: var(--text-dim);">开仓均价: <span id="card-entry">--</span></span>
        </div>
      </div>

      <div class="metric-card">
        <div class="metric-title">账户权益 / 可用保证金</div>
        <div class="metric-value" id="card-balance">-- USDC</div>
        <div class="metric-sub">
          <span>可用: <span id="card-available">-- USDC</span></span>
        </div>
      </div>

      <div class="metric-card">
        <div class="metric-title">网格配置摘要</div>
        <div class="metric-value" style="font-size: 19px;" id="card-grid-info">0.1 USDC / 100 U</div>
        <div class="metric-sub">
          <span>窗口: <strong id="card-window-info">5 买 / 5 卖</strong> (Maker GTX)</span>
        </div>
      </div>
    </div>

    <!-- Visual Grid Ladder Depth -->
    <div class="ladder-section">
      <div class="section-title">
        <span>实时网格盘口深度分布 (Post-Only Maker 挂单窗口)</span>
        <span style="font-size: 12px; font-weight: normal; color: var(--text-dim);">每格间隔: <span id="ladder-step">0.1</span> USDC | 每单金额: <span id="ladder-amount">100</span> USDC</span>
      </div>
      <div class="ladder-container" id="ladder-container">
        <!-- Rendered dynamically -->
      </div>
    </div>

    <!-- Tabs Panel -->
    <div class="tabs-card">
      <div class="tab-nav">
        <button class="tab-button active" onclick="switchTab('tab-orders', this)">当前有效挂单 (<span id="count-orders">0</span>)</button>
        <button class="tab-button" onclick="switchTab('tab-trades', this)">成交与套利历史 (<span id="count-trades">0</span>)</button>
        <button class="tab-button" id="tab-btn-config" onclick="switchTab('tab-config', this)">⚙️ 策略参数在线配置</button>
        <button class="tab-button" onclick="switchTab('tab-logs', this)">系统运行日志</button>
      </div>

      <!-- Tab: Active Orders -->
      <div class="tab-content active" id="tab-orders">
        <table>
          <thead>
            <tr>
              <th>方向</th>
              <th>网格档位</th>
              <th>挂单价格 (USDC)</th>
              <th>数量 (SOL)</th>
              <th>订单金额 (USDC)</th>
              <th>与现价偏离</th>
              <th>订单性质</th>
              <th>客户端订单ID</th>
              <th>挂单时间</th>
            </tr>
          </thead>
          <tbody id="orders-tbody">
            <tr><td colspan="9" style="text-align: center; color: var(--text-dim); padding: 30px;">暂无挂单</td></tr>
          </tbody>
        </table>
      </div>

      <!-- Tab: Trade History -->
      <div class="tab-content" id="tab-trades">
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>方向</th>
              <th>成交均价 (USDC)</th>
              <th>数量 (SOL)</th>
              <th>成交金额 (USDC)</th>
              <th>网格周期利润</th>
              <th>角色</th>
              <th>说明</th>
            </tr>
          </thead>
          <tbody id="trades-tbody">
            <tr><td colspan="8" style="text-align: center; color: var(--text-dim); padding: 30px;">暂无成交记录</td></tr>
          </tbody>
        </table>
      </div>

      <!-- Tab: Strategy Config (Interactive Form) -->
      <div class="tab-content" id="tab-config">
        <div class="config-container">
          <div class="config-header">
            <h3>⚙️ 策略运行参数在线设置</h3>
            <p style="color: var(--text-dim); font-size: 13px; margin-top: 4px;">无需任何配置文件，修改后点击「保存并立即生效」将自动持久化至 SQLite 数据库 (<code>data/bot.db</code>)，并立即重新对齐和更新网格挂单。</p>
          </div>

          <form id="config-form" onsubmit="event.preventDefault(); submitConfig();">
            <div class="config-form-grid">
              <!-- 交易对 -->
              <div class="form-group">
                <label>交易合约币种 (Symbol)</label>
                <input type="text" id="cfg-symbol" required placeholder="如 SOLUSDC, BTCUSDT" />
                <span class="form-hint">币安合约交易对代码</span>
              </div>

              <!-- 网格间距 -->
              <div class="form-group">
                <label>网格间距 (Grid Interval, USDC)</label>
                <input type="number" id="cfg-interval" step="any" min="0.0001" required placeholder="0.1" />
                <span class="form-hint">每上涨该金额卖出，每下跌该金额买入</span>
              </div>

              <!-- 单格金额 -->
              <div class="form-group">
                <label>单格下单金额 (Order Amount, USDC)</label>
                <input type="number" id="cfg-amount" step="any" min="1" required placeholder="100.0" />
                <span class="form-hint">每次买入或卖出的名义金额 (U)</span>
              </div>

              <!-- 买单窗口 -->
              <div class="form-group">
                <label>买单挂单窗口数量 (Buy Window)</label>
                <input type="number" id="cfg-buy-window" min="1" max="25" required placeholder="5" />
                <span class="form-hint">当前市价下方保持的预挂买单笔数</span>
              </div>

              <!-- 卖单窗口 -->
              <div class="form-group">
                <label>卖单挂单窗口数量 (Sell Window)</label>
                <input type="number" id="cfg-sell-window" min="1" max="25" required placeholder="5" />
                <span class="form-hint">当前市价上方保持的预挂卖单笔数</span>
              </div>

              <!-- 运行模式 -->
              <div class="form-group">
                <label>运行模式 (Trading Mode)</label>
                <select id="cfg-mode">
                  <option value="dry_run">模拟交易 (Paper Trading / 零风险实盘仿真)</option>
                  <option value="testnet">币安合约测试网 (Testnet)</option>
                  <option value="live">币安合约实盘 (Live Real Trading)</option>
                </select>
                <span class="form-hint">模拟盘零风险体验，实盘需填写有效 API 密钥</span>
              </div>

              <!-- 只做 Maker -->
              <div class="form-group" style="justify-content: center;">
                <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
                  <input type="checkbox" id="cfg-post-only" checked style="width: 17px; height: 17px; accent-color: var(--accent);" />
                  <span>只做 Maker 挂单 (Post-Only GTX)</span>
                </label>
                <span class="form-hint">保证享受 Maker 费率且绝不吃单，节省手续费</span>
              </div>

              <!-- 最大持仓 -->
              <div class="form-group">
                <label>最大持仓名义价值 (USDC, 可选)</label>
                <input type="number" id="cfg-max-position" step="any" placeholder="例如 2000" />
                <span class="form-hint">持仓达到该名义价值后暂停同向开单</span>
              </div>

              <!-- 价格下限 -->
              <div class="form-group">
                <label>网格价格下限 (Min Price, 可选)</label>
                <input type="number" id="cfg-min-price" step="any" placeholder="例如 80.0" />
                <span class="form-hint">跌破该价格停止下单</span>
              </div>

              <!-- 价格上限 -->
              <div class="form-group">
                <label>网格价格上限 (Max Price, 可选)</label>
                <input type="number" id="cfg-max-price" step="any" placeholder="例如 200.0" />
                <span class="form-hint">涨超该价格停止下单</span>
              </div>

              <!-- API Key -->
              <div class="form-group">
                <label>币安 API Key (实盘使用)</label>
                <input type="password" id="cfg-api-key" placeholder="留空则保持当前配置不变" />
                <span class="form-hint" id="cfg-api-key-status">当前状态: 未配置</span>
              </div>

              <!-- API Secret -->
              <div class="form-group">
                <label>币安 API Secret (实盘使用)</label>
                <input type="password" id="cfg-api-secret" placeholder="留空则保持当前配置不变" />
                <span class="form-hint" id="cfg-api-secret-status">当前状态: 未配置</span>
              </div>
            </div>

            <!-- 底部保存栏 -->
            <div class="form-actions">
              <div style="display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--text-dim);">
                <span style="color: var(--accent);">💾 数据持久化:</span> 本地 SQLite 数据库 (<code>data/bot.db</code>，重启自动保留，无需配置文件)
              </div>
              <div style="display: flex; gap: 10px;">
                <button type="button" onclick="loadConfigForm()">↺ 还原当前配置</button>
                <button type="submit" class="btn-primary" id="btn-save-cfg">💾 保存并立即生效</button>
              </div>
            </div>
            <div id="cfg-alert" style="display: none; margin-top: 14px; padding: 12px 16px; border-radius: 6px; font-size: 13px;"></div>
          </form>
        </div>
      </div>

      <!-- Tab: Logs -->
      <div class="tab-content" id="tab-logs">
        <div class="log-console" id="log-console">
          <!-- Rendered dynamically -->
        </div>
      </div>
    </div>
  </main>

  <!-- Modal: Initial Admin Password Setup -->
  <div id="modal-auth-setup" class="modal-overlay" style="display: none;">
    <div class="modal-card">
      <div style="text-align: center; margin-bottom: 20px;">
        <div style="font-size: 38px; margin-bottom: 8px;">🛡️</div>
        <h3 style="font-size: 20px; font-weight: 700; color: #fff;">首次启动初始化</h3>
        <p style="color: var(--text-dim); font-size: 13px; margin-top: 6px; line-height: 1.5;">
          为保障您的币安 API 密钥与交易资产安全，首次使用请先设定系统管理员访问密码（至少 6 位）。
        </p>
      </div>
      <form id="form-auth-setup" onsubmit="event.preventDefault(); submitAuthSetup();">
        <div class="form-group" style="margin-bottom: 14px;">
          <label style="font-size: 12px; color: var(--text-dim); margin-bottom: 6px; display: block;">设置管理员密码 (至少 6 位)</label>
          <input type="password" id="setup-pwd" required minlength="6" placeholder="输入管理员密码" style="width: 100%; box-sizing: border-box;" autofocus />
        </div>
        <div class="form-group" style="margin-bottom: 18px;">
          <label style="font-size: 12px; color: var(--text-dim); margin-bottom: 6px; display: block;">确认管理员密码</label>
          <input type="password" id="setup-pwd-confirm" required minlength="6" placeholder="再次输入密码进行确认" style="width: 100%; box-sizing: border-box;" />
        </div>
        <div id="setup-alert" style="display: none; padding: 10px 14px; border-radius: 6px; font-size: 13px; margin-bottom: 14px;"></div>
        <button type="submit" class="btn-primary" id="btn-submit-setup" style="width: 100%; padding: 12px; font-size: 14px; font-weight: 600;">
          🚀 设置密码并进入系统
        </button>
      </form>
    </div>
  </div>

  <!-- Modal: Admin Login -->
  <div id="modal-auth-login" class="modal-overlay" style="display: none;">
    <div class="modal-card">
      <div style="text-align: center; margin-bottom: 20px;">
        <div style="font-size: 38px; margin-bottom: 8px;">🔐</div>
        <h3 style="font-size: 20px; font-weight: 700; color: #fff;">管理员身份认证</h3>
        <p style="color: var(--text-dim); font-size: 13px; margin-top: 6px; line-height: 1.5;">
          控制台已受密码保护，请输入管理员密码以进入仪表盘与管理网格策略。
        </p>
      </div>
      <form id="form-auth-login" onsubmit="event.preventDefault(); submitAuthLogin();">
        <div class="form-group" style="margin-bottom: 18px;">
          <label style="font-size: 12px; color: var(--text-dim); margin-bottom: 6px; display: block;">管理员密码</label>
          <input type="password" id="login-pwd" required placeholder="请输入管理员密码" style="width: 100%; box-sizing: border-box;" autofocus />
        </div>
        <div id="login-alert" style="display: none; padding: 10px 14px; border-radius: 6px; font-size: 13px; margin-bottom: 14px;"></div>
        <button type="submit" class="btn-primary" id="btn-submit-login" style="width: 100%; padding: 12px; font-size: 14px; font-weight: 600;">
          🔓 验证并登录
        </button>
      </form>
    </div>
  </div>

  <!-- Modal: Change Password -->
  <div id="modal-change-password" class="modal-overlay" style="display: none;">
    <div class="modal-card">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
        <h3 style="font-size: 18px; font-weight: 700; color: #fff;">🔑 修改管理员密码</h3>
        <button type="button" onclick="closeChangePasswordModal()" style="background: none; border: none; color: var(--text-dim); font-size: 20px; cursor: pointer; padding: 4px 8px;">✕</button>
      </div>
      <form id="form-change-pwd" onsubmit="event.preventDefault(); submitChangePassword();">
        <div class="form-group" style="margin-bottom: 12px;">
          <label style="font-size: 12px; color: var(--text-dim); margin-bottom: 6px; display: block;">当前原密码</label>
          <input type="password" id="chg-old-pwd" required placeholder="输入当前使用的密码" style="width: 100%; box-sizing: border-box;" />
        </div>
        <div class="form-group" style="margin-bottom: 12px;">
          <label style="font-size: 12px; color: var(--text-dim); margin-bottom: 6px; display: block;">新管理员密码 (至少 6 位)</label>
          <input type="password" id="chg-new-pwd" required minlength="6" placeholder="输入新密码" style="width: 100%; box-sizing: border-box;" />
        </div>
        <div class="form-group" style="margin-bottom: 18px;">
          <label style="font-size: 12px; color: var(--text-dim); margin-bottom: 6px; display: block;">确认新密码</label>
          <input type="password" id="chg-new-pwd-confirm" required minlength="6" placeholder="再次输入新密码" style="width: 100%; box-sizing: border-box;" />
        </div>
        <div id="chg-alert" style="display: none; padding: 10px 14px; border-radius: 6px; font-size: 13px; margin-bottom: 14px;"></div>
        <div style="display: flex; gap: 10px; justify-content: flex-end;">
          <button type="button" onclick="closeChangePasswordModal()">取消</button>
          <button type="submit" class="btn-primary" id="btn-submit-chg">确认修改</button>
        </div>
      </form>
    </div>
  </div>


  <script>

    const AUTH_TOKEN_KEY = "binance_bot_auth_token";

    function getAuthToken() {
      return localStorage.getItem(AUTH_TOKEN_KEY) || "";
    }

    function setAuthToken(token) {
      if (token) {
        localStorage.setItem(AUTH_TOKEN_KEY, token);
      } else {
        localStorage.removeItem(AUTH_TOKEN_KEY);
      }
    }

    async function authFetch(url, options = {}) {
      options.headers = options.headers || {};
      const token = getAuthToken();
      if (token) {
        options.headers["Authorization"] = `Bearer ${token}`;
      }
      const res = await fetch(url, options);
      if (res.status === 401) {
        setAuthToken("");
        if (ws) {
          try { ws.close(); } catch (e) {}
          ws = null;
        }
        checkAuthAndInit();
        throw new Error("未授权或登录已过期");
      }
      return res;
    }

    async function checkAuthAndInit() {
      try {
        const token = getAuthToken();
        const headers = token ? { "Authorization": `Bearer ${token}` } : {};
        const res = await fetch("/api/auth/status", { headers });
        const json = await res.json();

        if (!json.success || !json.data) {
          showLoginModal();
          return;
        }

        const { initialized, authenticated } = json.data;
        if (!initialized) {
          showSetupModal();
        } else if (!authenticated) {
          showLoginModal();
        } else {
          hideAllModals();
          initWebSocket();
          loadConfigForm();
        }
      } catch (e) {
        console.error("Failed to check auth status:", e);
        showLoginModal();
      }
    }

    function showSetupModal() {
      document.getElementById("modal-auth-setup").style.display = "flex";
      document.getElementById("modal-auth-login").style.display = "none";
      document.getElementById("setup-pwd").focus();
    }

    function showLoginModal(alertMsg = "") {
      document.getElementById("modal-auth-setup").style.display = "none";
      const modal = document.getElementById("modal-auth-login");
      modal.style.display = "flex";
      const alertBox = document.getElementById("login-alert");
      if (alertMsg) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = alertMsg;
      } else {
        alertBox.style.display = "none";
      }
      document.getElementById("login-pwd").focus();
    }

    function hideAllModals() {
      document.getElementById("modal-auth-setup").style.display = "none";
      document.getElementById("modal-auth-login").style.display = "none";
      document.getElementById("modal-change-password").style.display = "none";
    }

    async function submitAuthSetup() {
      const pwd = document.getElementById("setup-pwd").value;
      const pwdConfirm = document.getElementById("setup-pwd-confirm").value;
      const alertBox = document.getElementById("setup-alert");
      alertBox.style.display = "none";

      if (pwd.length < 6) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = "❌ 密码长度不能少于 6 位";
        return;
      }

      if (pwd !== pwdConfirm) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = "❌ 两次输入的密码不一致，请重新检查";
        return;
      }

      const btn = document.getElementById("btn-submit-setup");
      btn.textContent = "⏳ 设置中...";
      btn.disabled = true;

      try {
        const res = await fetch("/api/auth/setup", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ password: pwd })
        });
        const json = await res.json();
        if (json.success && json.data && json.data.token) {
          setAuthToken(json.data.token);
          hideAllModals();
          initWebSocket();
          loadConfigForm();
        } else {
          alertBox.style.display = "block";
          alertBox.style.background = "rgba(244, 63, 94, 0.15)";
          alertBox.style.border = "1px solid var(--red)";
          alertBox.style.color = "var(--red)";
          alertBox.textContent = "❌ " + (json.message || "初始化密码失败");
        }
      } catch (e) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = "❌ 请求异常: " + e.message;
      } finally {
        btn.textContent = "🚀 设置密码并进入系统";
        btn.disabled = false;
      }
    }

    async function submitAuthLogin() {
      const pwd = document.getElementById("login-pwd").value;
      const alertBox = document.getElementById("login-alert");
      alertBox.style.display = "none";

      const btn = document.getElementById("btn-submit-login");
      btn.textContent = "⏳ 认证中...";
      btn.disabled = true;

      try {
        const res = await fetch("/api/auth/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ password: pwd })
        });
        const json = await res.json();
        if (json.success && json.data && json.data.token) {
          setAuthToken(json.data.token);
          hideAllModals();
          initWebSocket();
          loadConfigForm();
        } else {
          alertBox.style.display = "block";
          alertBox.style.background = "rgba(244, 63, 94, 0.15)";
          alertBox.style.border = "1px solid var(--red)";
          alertBox.style.color = "var(--red)";
          alertBox.textContent = "❌ " + (json.message || "密码错误");
        }
      } catch (e) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = "❌ 请求异常: " + e.message;
      } finally {
        btn.textContent = "🔓 验证并登录";
        btn.disabled = false;
      }
    }

    async function logout() {
      if (!confirm("确定要退出当前管理员登录吗？")) return;
      try {
        await authFetch("/api/auth/logout", { method: "POST" });
      } catch (e) {}
      setAuthToken("");
      if (ws) {
        try { ws.close(); } catch (e) {}
        ws = null;
      }
      showLoginModal("已安全退出登录");
    }

    function openChangePasswordModal() {
      document.getElementById("modal-change-password").style.display = "flex";
      document.getElementById("chg-old-pwd").value = "";
      document.getElementById("chg-new-pwd").value = "";
      document.getElementById("chg-new-pwd-confirm").value = "";
      document.getElementById("chg-alert").style.display = "none";
      document.getElementById("chg-old-pwd").focus();
    }

    function closeChangePasswordModal() {
      document.getElementById("modal-change-password").style.display = "none";
    }

    async function submitChangePassword() {
      const oldPwd = document.getElementById("chg-old-pwd").value;
      const newPwd = document.getElementById("chg-new-pwd").value;
      const newPwdConfirm = document.getElementById("chg-new-pwd-confirm").value;
      const alertBox = document.getElementById("chg-alert");
      alertBox.style.display = "none";

      if (newPwd.length < 6) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = "❌ 新密码长度至少需为 6 位";
        return;
      }

      if (newPwd !== newPwdConfirm) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = "❌ 两次输入的新密码不一致";
        return;
      }

      const btn = document.getElementById("btn-submit-chg");
      btn.textContent = "⏳ 修改中...";
      btn.disabled = true;

      try {
        const res = await authFetch("/api/auth/change_password", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ old_password: oldPwd, new_password: newPwd })
        });
        const json = await res.json();
        if (json.success) {
          alert("密码修改成功，所有在线会话已重置，请使用新密码重新登录！");
          closeChangePasswordModal();
          setAuthToken("");
          if (ws) {
            try { ws.close(); } catch (e) {}
            ws = null;
          }
          showLoginModal("密码已修改，请使用新密码登录");
        } else {
          alertBox.style.display = "block";
          alertBox.style.background = "rgba(244, 63, 94, 0.15)";
          alertBox.style.border = "1px solid var(--red)";
          alertBox.style.color = "var(--red)";
          alertBox.textContent = "❌ " + (json.message || "修改失败");
        }
      } catch (e) {
        alertBox.style.display = "block";
        alertBox.style.background = "rgba(244, 63, 94, 0.15)";
        alertBox.style.border = "1px solid var(--red)";
        alertBox.style.color = "var(--red)";
        alertBox.textContent = "❌ 请求异常: " + e.message;
      } finally {
        btn.textContent = "确认修改";
        btn.disabled = false;
      }
    }

    let ws = null;
    let currentBotStatus = 'RUNNING';

    function initWebSocket() {
      const token = getAuthToken();
      if (!token) return;

      if (ws) {
        try { ws.close(); } catch (e) {}
      }

      const loc = window.location;
      const wsProto = loc.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${wsProto}//${loc.host}/ws?token=${encodeURIComponent(token)}`;

      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        console.log('Connected to WebSocket server');
      };

      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          if (payload.type === 'snapshot') {
            updateDashboard(payload.data);
          } else if (payload.type === 'log') {
            appendLog(payload.data);
          }
        } catch (e) {
          console.error('Error handling WS message:', e);
        }
      };

      ws.onclose = () => {
        console.warn('WebSocket disconnected, reconnecting in 2s...');
        setTimeout(initWebSocket, 2000);
      };

      ws.onerror = (err) => {
        console.error('WebSocket error:', err);
      };
    }

    function updateDashboard(data) {
      currentBotStatus = data.status;

      // Badges
      document.getElementById('symbol-badge').textContent = `${data.symbol} PERP`;

      const statusBadge = document.getElementById('status-badge');
      const pauseResumeBtn = document.getElementById('btn-pause-resume');

      if (data.status === 'RUNNING') {
        statusBadge.className = 'badge badge-green';
        statusBadge.innerHTML = '<span class="pulse-dot"></span> RUNNING';
        pauseResumeBtn.textContent = '⏸️ 暂停策略';
        pauseResumeBtn.className = '';
      } else {
        statusBadge.className = 'badge badge-amber';
        statusBadge.innerHTML = '⏸️ PAUSED';
        pauseResumeBtn.textContent = '▶️ 恢复策略';
        pauseResumeBtn.className = 'btn-primary';
      }

      const modeBadge = document.getElementById('mode-badge');
      if (data.dry_run) {
        modeBadge.className = 'badge badge-purple';
        modeBadge.textContent = 'PAPER TRADING (模拟实盘)';
      } else if (data.grid_config.is_testnet) {
        modeBadge.className = 'badge badge-amber';
        modeBadge.textContent = 'TESTNET (测试网)';
      } else {
        modeBadge.className = 'badge badge-green';
        modeBadge.textContent = 'REAL LIVE (实盘运行)';
      }

      // Ticker & Price Card
      const price = parseFloat(data.ticker.last_price || 0);
      document.getElementById('card-price').textContent = `$${price.toFixed(4)}`;

      const chgPct = parseFloat(data.ticker.change_percent_24h || 0);
      const chgVal = parseFloat(data.ticker.change_24h || 0);
      const chgEl = document.getElementById('card-price-change');
      chgEl.textContent = `${chgPct >= 0 ? '+' : ''}${chgPct.toFixed(2)}% (${chgVal >= 0 ? '+' : ''}${chgVal.toFixed(2)} USDC)`;
      chgEl.className = chgPct >= 0 ? 'text-green' : 'text-red';

      document.getElementById('card-high').textContent = parseFloat(data.ticker.high_24h || 0).toFixed(2);
      document.getElementById('card-low').textContent = parseFloat(data.ticker.low_24h || 0).toFixed(2);

      // Profit Card
      const profit = parseFloat(data.stats.total_realized_profit || 0);
      document.getElementById('card-profit').textContent = `${profit >= 0 ? '+' : ''}${profit.toFixed(4)} USDC`;
      document.getElementById('card-cycles').textContent = data.stats.completed_cycles;

      // Position Card
      const posSize = parseFloat(data.position.size || 0);
      const posEl = document.getElementById('card-position');
      posEl.textContent = `${posSize > 0 ? '+' : ''}${posSize.toFixed(2)} SOL`;
      posEl.className = posSize > 0 ? 'metric-value text-green' : (posSize < 0 ? 'metric-value text-red' : 'metric-value');

      const unPnl = parseFloat(data.position.unrealized_pnl || 0);
      const unPnlEl = document.getElementById('card-unrealized');
      unPnlEl.textContent = `${unPnl >= 0 ? '+' : ''}${unPnl.toFixed(4)} USDC`;
      unPnlEl.className = unPnl >= 0 ? 'text-green' : 'text-red';

      const entryPrice = parseFloat(data.position.entry_price || 0);
      document.getElementById('card-entry').textContent = entryPrice > 0 ? `$${entryPrice.toFixed(2)}` : '--';

      // Account Card
      const walletBal = parseFloat(data.account.total_wallet_balance || 0);
      const availBal = parseFloat(data.account.available_balance || 0);
      document.getElementById('card-balance').textContent = `${walletBal.toFixed(2)} USDC`;
      document.getElementById('card-available').textContent = `${availBal.toFixed(2)} USDC`;

      // Grid Info Card
      document.getElementById('card-grid-info').textContent = `${data.grid_config.grid_interval} USDC / ${data.grid_config.order_amount_usdc} U`;
      document.getElementById('card-window-info').textContent = `${data.grid_config.buy_window} 买 / ${data.grid_config.sell_window} 卖`;
      document.getElementById('ladder-step').textContent = data.grid_config.grid_interval;
      document.getElementById('ladder-amount').textContent = data.grid_config.order_amount_usdc;

      // Render Visual Ladder
      renderLadder(data.active_orders, price);

      // Render Orders Table
      renderOrdersTable(data.active_orders, price);
      document.getElementById('count-orders').textContent = data.active_orders.length;

      // Render Trades Table
      renderTradesTable(data.recent_trades);
      document.getElementById('count-trades').textContent = data.recent_trades.length;

      // Render Logs
      renderLogs(data.recent_logs);
    }

    function renderLadder(orders, currentPrice) {
      const container = document.getElementById('ladder-container');
      const sellOrders = orders.filter(o => o.side === 'SELL').sort((a, b) => parseFloat(b.price) - parseFloat(a.price));
      const buyOrders = orders.filter(o => o.side === 'BUY').sort((a, b) => parseFloat(b.price) - parseFloat(a.price));

      let html = '';

      // Sell ladder rows
      sellOrders.forEach(o => {
        const p = parseFloat(o.price);
        const diff = currentPrice > 0 ? (((p - currentPrice) / currentPrice) * 100).toFixed(2) : '0.00';
        html += `
          <div class="ladder-row sell">
            <span class="badge badge-red">SELL</span>
            <strong class="text-red">$${p.toFixed(4)}</strong>
            <span>${parseFloat(o.quantity).toFixed(2)} SOL</span>
            <span style="color: var(--text-muted);">${parseFloat(o.amount_usdc).toFixed(2)} USDC</span>
            <span style="color: var(--text-dim); font-size: 11px;">+${diff}%</span>
            <span style="text-align: right; color: var(--text-dim); font-size: 11px;">${o.is_take_profit ? '🎯 止盈单' : '挂单 Maker'}</span>
            <div class="depth-bar sell" style="width: 45%;"></div>
          </div>
        `;
      });

      // Current Price Row
      html += `
        <div class="ladder-current">
          <span>● 币安合约现价</span>
          <span style="font-size: 17px; letter-spacing: 0.05em;">$${currentPrice.toFixed(4)} USDC</span>
          <span>中间基准线</span>
        </div>
      `;

      // Buy ladder rows
      buyOrders.forEach(o => {
        const p = parseFloat(o.price);
        const diff = currentPrice > 0 ? (((currentPrice - p) / currentPrice) * 100).toFixed(2) : '0.00';
        html += `
          <div class="ladder-row buy">
            <span class="badge badge-green">BUY</span>
            <strong class="text-green">$${p.toFixed(4)}</strong>
            <span>${parseFloat(o.quantity).toFixed(2)} SOL</span>
            <span style="color: var(--text-muted);">${parseFloat(o.amount_usdc).toFixed(2)} USDC</span>
            <span style="color: var(--text-dim); font-size: 11px;">-${diff}%</span>
            <span style="text-align: right; color: var(--text-dim); font-size: 11px;">挂单 Maker</span>
            <div class="depth-bar buy" style="width: 45%;"></div>
          </div>
        `;
      });

      container.innerHTML = html;
    }

    function renderOrdersTable(orders, currentPrice) {
      const tbody = document.getElementById('orders-tbody');
      if (!orders || orders.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: var(--text-dim); padding: 30px;">暂无挂单</td></tr>';
        return;
      }

      let html = '';
      orders.forEach(o => {
        const p = parseFloat(o.price);
        const diff = currentPrice > 0 ? (((p - currentPrice) / currentPrice) * 100).toFixed(2) : '0.00';
        const isBuy = o.side === 'BUY';
        const timeStr = new Date(o.created_at).toLocaleTimeString();

        html += `
          <tr>
            <td><span class="badge ${isBuy ? 'badge-green' : 'badge-red'}">${o.side}</span></td>
            <td>${o.grid_level}</td>
            <td><strong class="${isBuy ? 'text-green' : 'text-red'}">$${p.toFixed(4)}</strong></td>
            <td>${parseFloat(o.quantity).toFixed(2)} SOL</td>
            <td>${parseFloat(o.amount_usdc).toFixed(2)} USDC</td>
            <td><span style="color: var(--text-muted);">${diff > 0 ? '+' : ''}${diff}%</span></td>
            <td>${o.is_take_profit ? '<span class="badge badge-purple">止盈单</span>' : '<span class="badge" style="background: rgba(255,255,255,0.08);">Maker GTX</span>'}</td>
            <td style="font-family: monospace; font-size: 11px; color: var(--text-dim);">${o.client_order_id}</td>
            <td style="color: var(--text-dim);">${timeStr}</td>
          </tr>
        `;
      });

      tbody.innerHTML = html;
    }

    function renderTradesTable(trades) {
      const tbody = document.getElementById('trades-tbody');
      if (!trades || trades.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-dim); padding: 30px;">暂无成交记录</td></tr>';
        return;
      }

      let html = '';
      trades.forEach(t => {
        const isBuy = t.side === 'BUY';
        const pnl = parseFloat(t.realized_pnl || 0);
        const timeStr = new Date(t.timestamp).toLocaleTimeString();

        html += `
          <tr>
            <td style="color: var(--text-dim);">${timeStr}</td>
            <td><span class="badge ${isBuy ? 'badge-green' : 'badge-red'}">${t.side}</span></td>
            <td><strong>$${parseFloat(t.price).toFixed(4)}</strong></td>
            <td>${parseFloat(t.quantity).toFixed(2)} SOL</td>
            <td>${parseFloat(t.amount_usdc).toFixed(2)} USDC</td>
            <td>
              ${pnl > 0 ? `<strong class="text-green">+${pnl.toFixed(4)} USDC</strong>` : '<span style="color: var(--text-dim);">--</span>'}
            </td>
            <td><span class="badge badge-purple">${t.is_maker ? 'Maker' : 'Taker'}</span></td>
            <td style="color: var(--text-muted);">${t.note}</td>
          </tr>
        `;
      });

      tbody.innerHTML = html;
    }

    function renderLogs(logs) {
      const consoleEl = document.getElementById('log-console');
      if (!logs || logs.length === 0) return;

      let html = '';
      logs.slice().reverse().forEach(log => {
        const timeStr = new Date(log.timestamp).toLocaleTimeString();
        html += `
          <div class="log-line">
            <span class="log-time">[${timeStr}]</span>
            <span class="log-badge ${log.level}">${log.level}</span>
            <span class="log-msg">${log.message}</span>
          </div>
        `;
      });
      consoleEl.innerHTML = html;
    }

    function appendLog(log) {
      const consoleEl = document.getElementById('log-console');
      const timeStr = new Date(log.timestamp).toLocaleTimeString();
      const line = document.createElement('div');
      line.className = 'log-line';
      line.innerHTML = `
        <span class="log-time">[${timeStr}]</span>
        <span class="log-badge ${log.level}">${log.level}</span>
        <span class="log-msg">${log.message}</span>
      `;
      consoleEl.insertBefore(line, consoleEl.firstChild);
    }

    function switchTab(tabId, btn) {
      document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById(tabId).classList.add('active');

      if (tabId === 'tab-config') {
        loadConfigForm();
      }
    }

    function openConfigTab() {
      const cfgBtn = document.getElementById('tab-btn-config');
      switchTab('tab-config', cfgBtn);
    }

    async function loadConfigForm() {
      try {
        const res = await authFetch('/api/config');
        const json = await res.json();
        if (json.success && json.data) {
          const c = json.data;
          document.getElementById('cfg-symbol').value = c.symbol;
          document.getElementById('cfg-interval').value = c.grid_interval;
          document.getElementById('cfg-amount').value = c.order_amount_usdc;
          document.getElementById('cfg-buy-window').value = c.buy_window;
          document.getElementById('cfg-sell-window').value = c.sell_window;
          document.getElementById('cfg-post-only').checked = c.post_only;

          if (c.dry_run) {
            document.getElementById('cfg-mode').value = 'dry_run';
          } else if (c.is_testnet) {
            document.getElementById('cfg-mode').value = 'testnet';
          } else {
            document.getElementById('cfg-mode').value = 'live';
          }

          document.getElementById('cfg-min-price').value = c.min_price || '';
          document.getElementById('cfg-max-price').value = c.max_price || '';
          document.getElementById('cfg-max-position').value = c.max_position_usdc || '';

          document.getElementById('cfg-api-key').value = '';
          document.getElementById('cfg-api-secret').value = '';

          document.getElementById('cfg-api-key-status').textContent = c.has_api_key ? `已配置 (${c.api_key_preview})` : '未配置';
          document.getElementById('cfg-api-secret-status').textContent = c.has_api_secret ? '已配置 (已隐藏保护)' : '未配置';
        }
      } catch (e) {
        console.error('Failed to load configuration:', e);
      }
    }

    async function submitConfig() {
      const alertBox = document.getElementById('cfg-alert');
      alertBox.style.display = 'none';

      const mode = document.getElementById('cfg-mode').value;
      const dry_run = mode === 'dry_run';
      const is_testnet = mode === 'testnet';

      const apiKey = document.getElementById('cfg-api-key').value.trim();
      const apiSecret = document.getElementById('cfg-api-secret').value.trim();

      const payload = {
        symbol: document.getElementById('cfg-symbol').value.trim().toUpperCase(),
        grid_interval: document.getElementById('cfg-interval').value,
        order_amount_usdc: document.getElementById('cfg-amount').value,
        buy_window: parseInt(document.getElementById('cfg-buy-window').value),
        sell_window: parseInt(document.getElementById('cfg-sell-window').value),
        post_only: document.getElementById('cfg-post-only').checked,
        dry_run: dry_run,
        is_testnet: is_testnet,
        api_key: apiKey ? apiKey : null,
        api_secret: apiSecret ? apiSecret : null,
        min_price: document.getElementById('cfg-min-price').value ? document.getElementById('cfg-min-price').value : null,
        max_price: document.getElementById('cfg-max-price').value ? document.getElementById('cfg-max-price').value : null,
        max_position_usdc: document.getElementById('cfg-max-position').value ? document.getElementById('cfg-max-position').value : null,
        save_to_file: true
      };

      const btn = document.getElementById('btn-save-cfg');
      btn.textContent = '⏳ 保存中...';
      btn.disabled = true;

      try {
        const res = await authFetch('/api/config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const resJson = await res.json();

        alertBox.style.display = 'block';
        if (resJson.success) {
          alertBox.style.background = 'rgba(16, 185, 129, 0.15)';
          alertBox.style.border = '1px solid var(--green)';
          alertBox.style.color = 'var(--green)';
          alertBox.textContent = '✅ ' + (resJson.message || '策略配置保存成功并已立即生效！');
          loadConfigForm();
        } else {
          alertBox.style.background = 'rgba(244, 63, 94, 0.15)';
          alertBox.style.border = '1px solid var(--red)';
          alertBox.style.color = 'var(--red)';
          alertBox.textContent = '❌ 配置保存失败: ' + (resJson.message || '未知错误');
        }
      } catch (e) {
        alertBox.style.display = 'block';
        alertBox.style.background = 'rgba(244, 63, 94, 0.15)';
        alertBox.style.border = '1px solid var(--red)';
        alertBox.style.color = 'var(--red)';
        alertBox.textContent = '❌ 请求异常: ' + e.message;
      } finally {
        btn.textContent = '💾 保存并立即生效';
        btn.disabled = false;
      }
    }

    async function sendControlAction(action) {
      try {
        const res = await authFetch('/api/control', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action })
        });
        const json = await res.json();
        console.log('Action response:', json);
      } catch (e) {
        alert('发送操作指令失败: ' + e.message);
      }
    }

    function togglePauseResume() {
      if (currentBotStatus === 'RUNNING') {
        sendControlAction('pause');
      } else {
        sendControlAction('resume');
      }
    }

    function rebalanceGrid() {
      if (confirm('确认以当前市价重新对齐和重置网格挂单？')) {
        sendControlAction('rebalance');
      }
    }

    function cancelAllOrders() {
      if (confirm('确定要撤销全部活跃网格挂单吗？')) {
        sendControlAction('cancel_all');
      }
    }

    window.addEventListener('DOMContentLoaded', () => {
      checkAuthAndInit();
    });
  </script>
</body>
</html>
"#;
