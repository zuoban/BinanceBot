# ⚡ 币安合约网格交易机器人 (Binance Futures Grid Trading Bot in Rust)

基于 Rust 编写的高性能币安 USDⓈ-M 合约网格量化交易机器人，针对 **SOLUSDC** 等合约进行自动化网格交易。内置现代化 Web 监控与控制面板，支持 WebSocket 毫秒级行情推送、只做 Maker（Post-Only GTX）挂单、动态滑动窗口与模拟盘/实盘双模式。

---

## 🌟 核心特性

1. **高并发与内存安全**：基于 Rust `tokio` 异步运行时与 `axum` Web 框架，提供零成本抽象与极低时延。
2. **严格的币安精度规则适配**：
   - 自动在启动时向币安查询 `exchangeInfo`。
   - 严格按币种的 `tickSize` 对价格规范化、按 `stepSize` 对数量截断，并严格校验 `minNotional`（最小下单金额），杜绝非法参数拒单。
3. **只做 Maker 单（Post-Only GTX）**：
   - 订单参数使用 `timeInForce: "GTX"`，确保所有订单均为挂单成交（Maker），享受极低或零手续费返还，避免吃单（Taker）产生高昂手续费。
   - 具备自动价格检查与防穿仓拒单重试机制。
4. **动态挂单窗口与自动对齐**：
   - 可自定义买入窗口（如预先在市价下方挂 5 笔买单）与卖出窗口（如预先在市价上方挂 5 笔卖单）。
   - 当买单成交后，自动在上方 $P + \Delta$ 挂对应的止盈卖单锁定利差，并在窗口末端补充新的买单。
   - 当卖单成交后，自动在下方 $P - \Delta$ 挂对应的买单，并在窗口顶端补充新的卖单。
   - 具备偏离订单自动修剪（Prune）机制，防止资金被远离当前价位的订单锁死。
5. **内嵌式实时 Web 监控控制台与网页在线配置**：
   - 零外部前端依赖，HTML/CSS/JS 静态资源直接编译进单个 Rust 二进制文件。
   - **支持网页直接配置策略参数**：交易对、网格间距、每格金额、买卖窗口、运行模式、API 密钥等，保存后自动实时重平衡挂单并支持持久化写入 `config.toml`。
   - 通过 WebSocket 实时展示：当前最新标记价格、24小时高低与涨跌、已实现网格总收益、完成套利循环次数、当前持仓量与未结盈亏、盘口挂单深度梯度梯子、有效挂单列表、成交历史与系统实时事件日志。
   - 支持网页端一键「暂停策略 / 恢复运行」、「手动对齐刷新网格」、「撤销全部挂单」。
6. **三种运行模式灵活切换**：
   - **Paper Trading（模拟盘 / Dry-Run）**：默认开启，实时订阅币安真实 WebSocket 行情，在本地内存中进行毫秒级撮合与收益统计，无需提供 API Key 即可零风险体验策略。
   - **Testnet（测试网）**：使用币安合约测试网 API 与资金进行联调。
   - **Live（实盘）**：连接币安合约主网执行真实真金白银挂单。
7. **全套 Docker 容器化与 GitHub Packages 支持**：
   - 包含多阶段构建 `Dockerfile` 与 `docker-compose.yml`。
   - 配置 GitHub Actions 自动化 CI/CD，自动编译多架构镜像发布至 GitHub Packages (`ghcr.io`)。

---

## 🐳 Docker 部署指南

### 1. 使用 Docker Compose 一键启动（推荐）

```bash
# 复制配置文件
cp config.example.toml config.toml

# 启动服务
docker compose up -d

# 查看运行日志
docker compose logs -f
```

启动后在浏览器打开控制台：
👉 **http://localhost:8080**

### 2. 使用 Docker 命令运行

```bash
docker run -d \
  --name binance-grid-bot \
  -p 8080:8080 \
  -v $(pwd)/config.toml:/app/config.toml \
  --restart unless-stopped \
  ghcr.io/zuoban/binancebot:latest
```

### 3. 从 GitHub Packages 拉取预构建镜像

```bash
docker pull ghcr.io/zuoban/binancebot:latest
```

---

## 🛠️ 本地编译与运行

### 1. 运行环境
- Rust 1.75+（推荐使用 `cargo`）

### 2. 编译与运行

#### 以模拟盘模式运行（默认，无需 API Key）
```bash
cargo run --release
```
程序启动后，打开浏览器访问控制台：
👉 **http://127.0.0.1:8080**

#### 指定参数快速启动
```bash
# 交易 SOLUSDC，网格间距 0.1 USDC，每格金额 100 USDC，买卖窗口各 5 单
cargo run --release -- --symbol SOLUSDC --interval 0.1 --amount 100 --buy-window 5 --sell-window 5
```

#### 实盘运行（需在 config.toml 填写 API Key & Secret）
```bash
cargo run --release -- --live
```

---

## ⚙️ 配置说明 (`config.toml`)

```toml
[exchange]
symbol = "SOLUSDC"             # 交易对名称
api_key = "YOUR_API_KEY"       # 币安 API Key (实盘使用)
api_secret = "YOUR_API_SECRET" # 币安 API Secret (实盘使用)
is_testnet = false             # 是否使用测试网
dry_run = true                 # 模拟盘模式 (true: 模拟, false: 实盘)
recv_window = 5000             # 时间窗口容差 (毫秒)
sync_interval_secs = 3         # 订单同步与对齐轮询间隔 (秒)

[grid]
grid_interval = 0.1            # 网格间距 (每上涨/下跌 0.1 触发交易)
order_amount_usdc = 100.0      # 每个网格订单下单金额 (100 USDC)
buy_window = 5                 # 买单预挂单窗口大小
sell_window = 5                # 卖单预挂单窗口大小
post_only = true               # 只做 Maker 单 (GTX)
max_position_usdc = 2000.0     # 最大持仓名义价值保护上限

[server]
host = "0.0.0.0"               # 监听地址 (容器内请使用 0.0.0.0)
port = 8080                    # Web 控制台端口
```

---

## 🧪 单元测试

运行项目测试套件：
```bash
cargo test
```
