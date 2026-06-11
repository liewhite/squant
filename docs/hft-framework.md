# HFT 框架 (Scala / ox / sttp)

参考 Rust 项目 `hft-engine-rs` (kameo actor + tokio) 移植的事件驱动高频交易框架，
基于虚拟线程重新设计并发模型：

| 概念 | hft-engine-rs (Rust) | 本框架 (Scala) |
|---|---|---|
| 并发原语 | tokio task + kameo Actor | ox 虚拟线程 fork + Channel |
| 消息投递 | kameo PubSub (unbounded mailbox) | `EventBus` (每订阅者一条 `Channel.unlimited`) |
| 状态串行化 | actor mailbox | 每策略独占一个虚拟线程串行消费 |
| 监督 | spawn_link 级联退出 | ox `supervised` 作用域级联取消 |
| HTTP/WS | reqwest + tokio-tungstenite | sttp client4 `DefaultSyncBackend` (同步阻塞，虚拟线程友好) |

## 事件流

```
Connector (WS) ──┐
Clock ───────────┼─> incomeBus ─> Executor (Strategy + StateManager) ─> outcomeBus ─> OutcomeProcessor ─> REST
                 │                                                                          │
                 └────────────── OrderUpdate (撤单确认 / 下单失败回流) <─────────────────────┘
```

- **IncomeEvent**: 行情 (BBO/MarkPrice/IndexPrice/FundingRate)、账户 (Position/OrderUpdate/Fill/Balance/AccountInfo)、Clock。
  symbol 级事件按 `(exchange, symbol)` 定向路由，账户级事件与 Clock 广播。
- **OutcomeEvent**: 策略产出的 `PlaceOrders` / `CancelOrder` 信号。

## 模块结构 (`src/main/scala/hft/`)

| 模块 | 职责 |
|---|---|
| `domain` | 纯数据模型: Order/Position/BBO/FundingRate/SymbolMeta 等，零行为依赖 |
| `messaging` | `IncomeEvent`、`EventBus`、`SymbolState`/`StateManager` (策略视角的聚合状态) |
| `exchange` | 核心抽象: `ExchangeClient` (REST trait)、`ExchangeConnector` (WS trait)、`SubscriptionKind`、`WsLoop` (通用重连泵) |
| `engine` | `Engine` (装配/生命周期)、`Executor` (策略运行器)、`OutcomeProcessor` (信号执行) |
| `strategy` | `Strategy` trait + `OutcomeEvent` |
| `exchange/binance` | Binance USDⓈ-M 实现 (REST 签名、公共/私有 WS、jsoniter 编解码) |
| `demo` | `HftDemo` 入口 + `FundingWatchStrategy` 演示策略 |

## 核心设计

### 两个核心 trait

接入一个新交易所 = 实现 `ExchangeClient` + `ExchangeConnector`，框架其余部分零修改：

- `ExchangeClient`: 同步 REST (下单/撤单/查持仓/查挂单/元数据)，错误以 `Either[ExchangeError, A]` 显式返回。
- `ExchangeConnector`: 维护 WS 长连接，把原始推送解析为统一 `IncomeEvent` 发布到总线；
  断线自动重连并恢复订阅；配置凭证时自动接入私有流。

编写一个新策略 = 实现 `Strategy` (声明订阅 + 纯函数式 `onEvent`)。

### 并发模型

- 所有组件都是 `supervised` 作用域内的虚拟线程 fork，任一组件崩溃级联终止整个作用域 (对应 kameo spawn_link)。
- 每个策略一个 `Executor`，独占虚拟线程串行消费事件，策略与状态无锁。
- `WsLoop` 每条连接两个虚拟线程: 发送线程是连接唯一写入者 (Pong 回应也经出站 channel)，
  接收线程重组分片文本帧后回调；任一侧出错拆除整条连接，退避重连。
- `OutcomeProcessor` 每个 REST 调用 fork 独立虚拟线程，下单互不阻塞。

### 订单生命周期

1. 策略产出 `PlaceOrders` (币本位数量)
2. Executor 生成 `clientOrderId`，以原始币本位登记 pending，按 `SymbolMeta` 转换精度后发布
3. OutcomeProcessor 调 REST 下单；失败以 `OrderUpdate(Error)` 回流
4. 私有流推送 `OrderUpdate`/`Fill` 更新 pending 与仓位 (Fill 乐观更新)
5. Clock 事件驱动超时清理: `Created` 状态超过 `orderTimeoutMs` 未获确认即视为丢失移除

### 启动顺序保证 (addStrategies)

1. Executor 订阅总线 (之后的事件不丢)
2. REST 查初始持仓并发布 (未返回的 symbol 显式推 size=0)
3. REST 查现有挂单并发布 (接管遗留订单)
4. 向交易所订阅行情 (数据从此开始流动)

## Binance 接入说明

- REST: `https://fapi.binance.com`，私有接口 HMAC-SHA256 签名。
- WS 按 Binance 路由端点拆分连接:
  - `/public/ws`: bookTicker → BBO
  - `/market/ws`: markPrice@1s → MarkPrice + IndexPrice + FundingRate (一条消息拆三个事件)
  - `/private/ws/<listenKey>`: ORDER_TRADE_UPDATE → OrderUpdate/Fill, ACCOUNT_UPDATE → Balance/Position；
    listenKey 每 30 分钟续期，重连时重新获取
- 注意: 未路由的旧端点 `wss://fstream.binance.com/ws` 不再推送 markPrice 等 `/market` 路由的数据。

## 运行演示与测试

```bash
sbt "runMain hft.demo.HftDemo"   # dry-run 演示
sbt test                          # 单元测试 (domain 精度/费率、订单生命周期、Executor 链路)
```

dry-run 模式接入 Binance 公开行情 (无需 API key)，可观察完整闭环：
行情流入 → 状态聚合 → 资金费率信号 → dry-run 下单 → pending 订单 5 秒超时清理。
配置 `BINANCE_API_KEY` / `BINANCE_API_SECRET` 环境变量可接入私有流 (真实下单需去掉 dryRun)。
