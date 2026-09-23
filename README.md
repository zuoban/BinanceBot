# ⚡ 币安合约网格交易机器人 (Binance Futures Grid Trading Bot in Rust)

基于 Rust 编写的高性能币安 USDⓈ-M 合约网格量化交易机器人，针对 **SOLUSDC** 等合约进行自动化网格交易。内置现代化 Web 监控与控制面板，支持 WebSocket 毫秒级行情推送、只做 Maker（Post-Only GTX）挂单、动态滑动窗口、网页直接在线配置策略以及内嵌 **SQLite 数据库持久化**。

---

## 🌟 核心特性

1. **无需配置文件，网页直接配置**：
   - 启动无需任何 `config.toml` 或环境变量文件，直接打开浏览器控制台即可配置所有参数。
   - 可在网页上随时修改：交易对（如 SOLUSDC）、网格间距（如 0.1）、每格金额（如 100 USDC）、买入窗口深度、卖出窗口深度、Post-Only Maker 挂单、运行模式（模拟盘 / 测试网 / 实盘）、API Key & Secret 等。
   - 点击「保存并立即生效」，策略引擎自动重新对齐和更新挂单。
2. **内嵌 SQLite 数据库数据持久化**：
   - 使用内置嵌入式 SQLite（`data/bot.db`），零外部数据库依赖。
   - 自动持久化保存策略运行配置、历史成交记录、套利循环盈亏统计等。
   - 重启容器或程序后，历史数据与策略参数自动加载还原，绝不丢失。
3. **安全管理员密码认证与引导初始化**：
   - **首次启动安全引导**：若数据库中无管理员密码，用户访问 Web 控制台时将强制引导先设置管理员密码（至少 6 位）。
   - **全站 API 与 WebSocket 鉴权**：所有敏感操作（参数配置、策略启停、挂单撤销、密钥信息）及行情推送均受 Bearer 会话 Token 保护，杜绝未授权访问。
   - **加密哈希安全存储**：密码采用随机加盐 + 10,000 轮 SHA-256 迭代计算安全存储在 SQLite `admin_auth` 表中。
   - **在线会话管理**：支持页面一键安全退出登录，及在控制台随时修改管理员密码。
3. **只做 Maker 单（Post-Only GTX）**：
   - 订单参数使用 `timeInForce: "GTX"`，确保所有订单均为挂单成交（Maker），享受极低或零手续费返还，避免吃单（Taker）产生高昂手续费。
   - 具备自动价格检查与防穿仓拒单重试机制。
4. **严格的币安精度规则适配**：
   - 自动在启动时向币安查询 `exchangeInfo`。
   - 严格按币种的 `tickSize` 对价格规范化、按 `stepSize` 对数量截断，并严格校验 `minNotional`（最小下单金额），杜绝非法参数拒单。
5. **动态挂单窗口与自动对齐**：
   - 可自定义买入窗口（如预先在市价下方挂 5 笔买单）与卖出窗口（如预先在市价上方挂 5 笔卖单）。
   - 当买单成交后，自动在上方 $P + \Delta$ 挂对应的止盈卖单锁定利差，并在窗口末端补充新的买单。
   - 当卖单成交后，自动在下方 $P - \Delta$ 挂对应的买单，并在窗口顶端补充新的卖单。
   - 具备偏离订单自动修剪（Prune）机制，防止资金被远离当前价位的订单锁死。
6. **内嵌式实时 Web 监控控制台**：
   - 零外部前端依赖，HTML/CSS/JS 静态资源直接编译进单个 Rust 二进制文件。
   - 通过 WebSocket 实时展示：当前最新标记价格、24小时高低与涨跌、已实现网格总收益、完成套利循环次数、当前持仓量与未结盈亏、盘口挂单深度梯度梯子、有效挂单列表、成交历史与系统实时事件日志。
   - 支持网页端一键「暂停策略 / 恢复运行」、「手动对齐刷新网格」、「撤销全部挂单」。
7. **三种运行模式灵活切换**：
   - **Paper Trading（模拟盘 / Dry-Run）**：默认开启，实时订阅币安真实 WebSocket 行情，在本地内存中进行毫秒级撮合与收益统计，无需提供 API Key 即可零风险体验策略。
   - **Testnet（测试网）**：使用币安合约测试网 API 与资金进行联调。
   - **Live（实盘）**：连接币安合约主网执行真实真金白银挂单。
8. **全套 Docker 容器化与 GitHub Packages 支持**：
   - 包含多阶段构建 `Dockerfile` 与 `docker-compose.yml`。
   - 配置 GitHub Actions 自动化 CI/CD，自动编译多架构镜像发布至 GitHub Packages (`ghcr.io/zuoban/binancebot`)。
9. **Telegram 成交通知**：在网页配置 Bot Token、Chat ID 并启用后，每笔确认成交的买单或卖单都会发送通知，消息首行显示成交方向和价格。模拟盘、测试网及实盘均支持。

---

## 🐳 Docker 极简部署指南（无需配置文件）

### 1. 使用 Docker Compose 一键启动（推荐）

只需一行命令启动，无需创建任何配置文件：

```bash
docker compose up -d
```

启动后在浏览器打开控制台：
👉 **http://localhost:8080**

所有策略配置在页面上填写保存后，会自动存储在 `./data/bot.db` 中。

### 2. 使用 Docker 命令行运行

```bash
docker run -d \
  --name binance-grid-bot \
  -p 8080:8080 \
  -v $(pwd)/data:/app/data \
  --restart unless-stopped \
  ghcr.io/zuoban/binancebot:latest
```

### 3. 从 GitHub Packages 拉取最新预构建镜像

```bash
docker pull ghcr.io/zuoban/binancebot:latest
```

---

## 🛠️ 本地编译与运行

### 1. 运行环境
- Rust 1.75+（使用 `cargo`）

### 2. 编译与运行

#### 直接启动（自动初始化 SQLite 并以模拟盘模式运行）
```bash
cargo run --release
```
程序启动后，打开浏览器访问控制台：
👉 **http://127.0.0.1:8080**

在「策略参数在线配置」面板中自由调整参数并点击保存，策略引擎将实时热更新。

### Telegram 成交通知

1. 在 Telegram 使用 @BotFather 创建 Bot，复制 Bot Token。
2. 给 Bot 发送一条消息；打开 `https://api.telegram.org/bot<Bot Token>/getUpdates`，从返回内容中的 `message.chat.id` 获取 Chat ID。群组通知需先将 Bot 加入群组并获取该群组的 Chat ID。
3. 在网页「策略参数在线配置」填写 Bot Token 和 Chat ID，勾选「启用 Telegram 成交通知」，点击「保存并立即生效」。Bot Token 不会在网页回显；留空可保留已保存的 Token。

也可在首次启动时通过 `config.toml` 的 `[telegram]` 段配置，格式见 `config.example.toml`。配置会存入 SQLite，关闭开关即可暂停通知。

#### 命令行快速指定数据库路径或参数
```bash
# 指定 SQLite 数据文件路径
cargo run --release -- --db data/my_bot.db

# 命令行快速预设参数（会自动同步写入 SQLite）
cargo run --release -- --symbol SOLUSDC --interval 0.1 --amount 100 --buy-window 5 --sell-window 5
```

---

## 💾 数据库存储说明 (`SQLite`)

机器人的所有状态与数据自动保存在 SQLite 数据库中（默认路径 `data/bot.db`）：

- **`app_config` 表**：存储当前生效的交易所参数、网格间距、下单金额、滑动窗口、风控范围与 API 凭证。
- **`trades` 表**：记录每一笔买入/卖出成交明细（包含价格、数量、名义金额、实现盈亏、手续费、Maker 标识、时间戳）。
- **`grid_stats` 表**：累计总成交量、总成交笔数、已完成套利循环次数、累计实现净利润。

任何 SQLite 客户端（如 `sqlite3`, DBeaver, TablePlus）均可直接打开 `data/bot.db` 查阅和导出交易数据。

---

## 🌐 远程服务器访问排查指南

如果在远程服务器（阿里云、腾讯云、华为云、AWS EC2、GCP 等）部署后，在本地电脑浏览器访问 `http://<服务器公网IP>:8080` 提示「无法访问此网站」或「连接被拒绝 (Connection Refused)」，请按以下步骤排查：

### 1. 检查云厂商控制台「安全组 / 防火墙」规则（最常见原因 90%）
云服务器默认通常只开放 22 端口（SSH），需要手动放行 `8080` 端口：
- **阿里云 ECS / 轻量应用服务器**：控制台 -> 安全组 -> 入方向规则 -> 添加规则 -> 协议选择 `TCP`，端口范围填 `8080`，授权对象填 `0.0.0.0/0`。
- **腾讯云 CVM / Lighthouse**：控制台 -> 防火墙 / 安全组 -> 添加规则 -> 来源 `0.0.0.0/0`，协议 `TCP`，端口 `8080`，策略 `允许`。
- **AWS EC2**：Security Groups -> Inbound rules -> Edit inbound rules -> Type: Custom TCP, Port: `8080`, Source: `0.0.0.0/0`。

### 2. 检查服务器内部 Linux 系统防火墙
部分 Linux 发行版内置防火墙可能会拦截非标准端口：
```bash
# Ubuntu / Debian (UFW 防火墙)
sudo ufw status
sudo ufw allow 8080/tcp
sudo ufw reload

# CentOS / RHEL / Alibaba Cloud Linux (Firewalld 防火墙)
sudo firewall-cmd --zone=public --add-port=8080/tcp --permanent
sudo firewall-cmd --reload
```

### 3. 在服务器本地测试服务是否正常运行
登录服务器终端，执行命令测试本地回环是否可以正常响应：
```bash
curl -I http://127.0.0.1:8080
```
如果返回 `HTTP/1.1 200 OK`，说明机器人服务本身运行完全正常，外部无法访问必为第 1 步或第 2 步的**安全组/防火墙**未开放。

### 4. 检查 Docker 端口映射与监听地址
若使用 Docker 部署，请确保：
```bash
docker ps
```
查看容器的 `PORTS` 列是否显示为 `0.0.0.0:8080->8080/tcp` 或 `:::8080->8080/tcp`。
*(切勿写成 `-p 127.0.0.1:8080:8080`，否则只允许服务器本地回环访问)*

---

## 🧪 单元测试

运行项目完整测试套件：
```bash
cargo test
```
