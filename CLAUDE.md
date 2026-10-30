# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 开发命令

需要 Rust 1.88+、Node.js 22.12+。以下命令从仓库根目录执行，前端命令除外。

```bash
cargo run --release                    # 后端；默认数据库 data/bot.db
cargo run --release -- --db data/dev.db --host 127.0.0.1 --port 8080
cargo build --release
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test --locked --test multi_pair_api_test  # 单个集成测试文件
cargo test --locked test_name -- --nocapture   # 按名称筛选测试
```

```bash
cd frontend
npm ci
npm run dev                   # Vite，默认代理 /api 和 /ws 到 127.0.0.1:8080
npm test                      # Vitest 全量运行
npm test -- src/test/api.test.ts  # 单个测试文件；可追加 -t '测试名称'
npm run build                 # TypeScript 检查及 Vite 生产构建
```

没有独立的前端 lint 脚本。前端代理目标可通过 `API_PROXY_TARGET` 设置；开发时需分别启动前后端。`docker compose up -d --build` 启动两个容器，前端通过 localhost:8080 提供页面。CI 执行上述 Rust 格式、Clippy、测试及前端测试和构建。README 还记录了需显式运行的模拟多交易对负载测试。

## 架构

- `src/main.rs` 从 SQLite 加载配置，先绑定监听端口，再启动 `TradingRuntime`、小时快照任务和 Axum 服务。`--host`/`--port` 只覆盖当前进程的监听地址；策略和凭证从数据库读取，通过控制台管理，不从旧配置文件或交易参数环境变量读取。首次启动的管理员初始化码写在启动日志中。
- `src/runtime.rs` 管理交易对的动态增删与生命周期：每个交易对拥有独立的 `AppState`、控制通道、网格引擎和行情流，共享数据库、请求限流器及账户、持仓和行情缓存。全局配置需要协调所有引擎；各交易对的配置、运行模式和暂停状态独立。
- `src/strategy/grid.rs` 的事件循环接收控制指令和行情，并定期与交易所同步订单、账户和持仓。`src/exchange/` 封装 Binance REST、签名、WebSocket、缓存及请求预算；模拟盘使用真实行情和本地撮合，测试网及实盘执行交易所操作。价格和数量使用 `rust_decimal`，并遵守交易所步长和最小下单规则。
- `src/db.rs` 管理 SQLite 配置、交易记录、按交易对和模式划分的统计与运行状态、托管订单和恢复意图。重启后先恢复元数据并对账；交易所执行结果不明确时保留订单 ID、阻止新增订单直到状态确认。暂停和关闭会停止补单、撤销并核实机器人订单，同时保留持仓；行情过期或对账状态不可靠时也会阻止新增订单。修改交易逻辑时需保持这些边界。
- `src/server/` 将运行时和引擎状态暴露为 HTTP API、交易对 WebSocket 快照与账户流；无交易对作用域的旧接口指向首个交易对。认证和会话由 `src/auth.rs`、数据库及路由共同处理；WebSocket 单独鉴权。`src/snapshots.rs` 负责定期账户快照，`src/telegram.rs` 负责通知。
- `frontend/src/main.tsx` 设置路由、登录状态和 TanStack Query；`lib/api.ts` 管理会话 token、HTTP 和 WebSocket 地址，`lib/queries.ts` 将实时快照写入查询缓存，并在连接失效或数据过期时回退到 HTTP 轮询。总览使用聚合快照，详情使用按交易对的快照；生产环境由独立的 Nginx 前端容器提供静态文件并代理 API/WebSocket，Rust 服务不提供网页。

Rust 模块内测试与 `tests/` 集成测试共同覆盖策略及 API；前端测试位于 `frontend/src/test/`。涉及订单对账、交易安全、持久化和 API 行为的修改应运行对应的针对性测试。用户界面文案沿用中文。
