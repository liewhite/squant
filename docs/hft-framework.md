# HFT 框架 (Scala / ox / sttp)

参考 Rust 项目 `hft-engine-rs` (kameo actor + tokio) 移植的事件驱动高频交易框架，
基于虚拟线程重新设计并发模型：

| 概念 | hft-engine-rs (Rust) | 本框架 (Scala) |
|---|---|---|
| 并发原语 | tokio task + kameo Actor | ox 虚拟线程 fork + Channel |
| 消息投递 | kameo PubSub (unbounded mailbox) | `EventBus` (按 (topic, key) 建索引，每订阅者一条 `Channel.unlimited`) |
| 事件扩展 | 封闭 enum + `CustomEvent` (类型擦除, 复用行情的 scope) | 开放的 `Topic[K, P]` (自带路由键类型与载荷类型) |
| 状态串行化 | actor mailbox | 每策略独占一个虚拟线程串行消费 |
| 监督 | spawn_link 级联退出 | ox `supervised` 作用域级联取消 |
| HTTP/WS | reqwest + tokio-tungstenite | sttp client4 `DefaultSyncBackend` (同步阻塞，虚拟线程友好) |

## 事件流

只有**一条**总线。行情、账户回报、时钟、策略下单意图都是它上面的事件，区别只在 topic：

```
Connector (WS) ──┐                    ┌─> Executor (Strategy + StateManager) ─┐
Clock ───────────┼─> bus (topic 路由) ┤                                       │ OrderIntent
执行回流 <────────┘                    └─> OutcomeProcessor ─> REST ────────────┘
```

下单意图不会回流给策略 —— 策略压根不订阅 `OrderIntent`，隔离由订阅关系保证，不靠总线拓扑。

### Topic: 事件的开放扩展点

一个 `Topic[K, P]` 同时钉死两件事：这一族事件按什么**路由** (`K`)、载荷是什么**类型** (`P`)。
新增事件类型只需声明一个 object，**框架一行不改**：

```scala
object AlphaSignal extends Topic[Symbol, Score]("alphaSignal"):
  def keyOf(p: Score): Symbol = p.symbol
```

约定与保证：

- **必须声明为 object**：框架按引用判别 topic 身份，这使"topic 相等 ⟹ 类型参数相同"成为结构保证，
  `Event.as` 的类型还原才有依据。
- **路由键由载荷派生** (`keyOf`)，`Event` 主构造器私有 —— 不存在"key 说 BTCUSDT、载荷里是 ETHUSDT"
  的静默错投。
- **消费侧用 `event.as(topic)`** 按 topic 还原静态类型，业务代码里不出现 `asInstanceOf`。

内置 topic 的三档路由维度（见 `hft.event.Topics`）：

| 维度 | topic | 说明 |
|---|---|---|
| `Instrument` | Bbo / Trade / MarkPrice / IndexPrice / FundingRate | 公共行情，需向交易所订阅 |
| `Instrument` | Position / OrderUpdate / Fill | 私有回报，账户流推送 |
| `Exchange` | Balance / AccountInfo / Greeks | 账户级读数 |
| 无 | Clock | 全局节拍，用 `Interest.All` 订阅 |

账户级读数**按交易所路由而非广播**，是一条越界防线：否则策略能读到自己没订阅的交易所的净值，
而杠杆闸门正是拿净值算的。

### 订阅是数据，不是谓词

`Interest` / `Subscription` 是可枚举、可哈希的数据结构，因为同一份声明有三个下游：

1. `EventBus` 据此建投递索引（两次哈希查表，不遍历订阅者）；
2. 回测在单线程循环里用 `Subscription.accepts` 过滤（没有总线，但判据必须与实盘同一份）；
3. `SubscriptionKind.from` 据此派生要向交易所订阅的行情流。

写成 `Event => Boolean` 会同时丢掉可索引与可自省，后者意味着策略得把订阅范围再声明一遍 ——
同一事实两处写，迟早错开。

策略只声明公共行情，框架自动补齐**不该由策略选择**的三类订阅（见 `StrategyRunner.subscriptionFor`）：
所声明标的的私有回报、所涉交易所的账户级读数、时钟。漏订一条 Fill 会让本地仓位与交易所静默发散，
这种事不能留给策略作者记得。

## 模块结构 (`src/main/scala/hft/`)

| 模块 | 职责 |
|---|---|
| `domain` | 纯数据模型: Order/Position/BBO/FundingRate/SymbolMeta 等，零行为依赖 |
| `event` | `Topic`/`Event`/`Interest`/`Subscription`/`EventBus` — 事件与投递的全部基础设施 |
| `state` | `SymbolState`/`StateManager` (策略视角的聚合状态) |
| `exchange` | 核心抽象: `ExchangeClient` (REST trait)、`ExchangeConnector` (WS trait)、`SubscriptionKind`、`WsLoop` (通用重连泵) |
| `engine` | `Engine` (装配/生命周期)、`Executor` (策略运行器)、`OutcomeProcessor` (信号执行) |
| `strategy` | `Strategy` trait + `OutcomeEvent`；`BboMakerStrategy` (BBO 外被动做市 + 杠杆率风控) |
| `exchange/binance` | Binance USDⓈ-M 实现 (REST 签名、公共/私有 WS、jsoniter 编解码) |
| `demo` | `HftDemo` (公开行情演示) / `MakerDemo` (做市策略) 入口 + `FundingWatchStrategy` 演示策略 |

## 核心设计

### 错误处理哲学: fail-fast，不做任何"没有把握"的恢复

正确性模型：**启动对齐必须成功 + 信任交易所推送不丢 + 一切异常逐层上抛、进程报错退出**，
由外层 (systemd/k8s) 重新拉起，重启后的启动对齐保证状态一致。不做快照覆盖式对账——
快照与 WS 推送存在时序竞争 (覆盖后收到覆盖前的 Fill 会双重计数)，无法做对。

具体体现：

- **不重连**: WS 断开 (含服务端 24h 强断、listenKey 过期/续期失败) 直接终止。
  私有流断线期间的推送无法回放，"重连恢复"等于静默的状态发散。
  TCP 半开连接不产生任何可抛的错误，由 WsLoop 空闲 watchdog (默认 5 分钟无帧)
  把静默停滞转化为错误；限频 (HTTP 429/418) 说明请求节奏假设被打破，同样致命。
- **不丢弃**: 消息解析失败、未知事件类型、未知订单状态，一律抛错。静默丢弃一条
  私有流消息等于丢一笔成交。
- **不确定即终止**: 下单/撤单遇到网络错误或超时，订单是否成立**不确定**，立即终止；
  只有交易所明确拒绝 (HTTP 4xx) 才作为正常业务结果以 OrderUpdate(Error) 回流策略。
  REST 超时 (3s) 必须小于订单超时 (orderTimeoutMs)，使 Created 订单超时未确认
  成为"不可能事件"——一旦发生即假设被破坏，终止。
- **配置错误即终止**: 缺 SymbolMeta、缺 client/connector、启动对齐失败 (除"未配置
  凭证"这一确定安全的例外) 都在装配/首次使用时抛错。

ox 监督树天然支撑该模型：所有组件都是 `supervised` 作用域内的 fork，异常到达 fork
边界即级联取消整个作用域并从 main 抛出，进程以非零码退出。

### 两个核心 trait

接入一个新交易所 = 实现 `ExchangeClient` + `ExchangeConnector`，框架其余部分零修改：

- `ExchangeClient`: 同步 REST (下单/撤单/查持仓/查挂单/元数据)，错误以 `Either[ExchangeError, A]` 显式返回。
- `ExchangeConnector`: 维护 WS 长连接，把原始推送解析为统一 `Event` 发布到总线；
  配置凭证时自动接入私有流；遵循 fail-fast 契约 (断线/解析失败即抛错终止)。

编写一个新策略 = 实现 `Strategy` (声明订阅 + 纯函数式 `onEvent`)。

### 并发模型

- 所有组件都是 `supervised` 作用域内的虚拟线程 fork，任一组件崩溃级联终止整个作用域 (对应 kameo spawn_link)。
- 每个策略一个 `Executor`，独占虚拟线程串行消费事件，策略与状态无锁。
- `WsLoop` 每条连接两个虚拟线程: 发送线程是连接唯一写入者 (Pong 回应也经出站 channel)，
  接收线程重组分片文本帧后回调；任一侧出错即异常上抛终止 (不重连)。
- `OutcomeProcessor` 每个 REST 调用 fork 独立虚拟线程，下单互不阻塞。

### 订单生命周期

1. 策略产出 `PlaceOrders` (币本位数量)
2. Executor 生成 `clientOrderId`，以原始币本位登记 pending，按 `SymbolMeta` 转换精度后发布
3. OutcomeProcessor 调 REST 下单；交易所明确拒绝 (4xx) 以 `OrderUpdate(Error)` 回流，
   结果不确定 (网络/超时/5xx) 直接终止
4. 私有流推送 `OrderUpdate`/`Fill` 更新 pending 与仓位 (Fill 乐观更新)；
   撤单终态同样以私有流推送为准，框架不合成确认事件
5. Clock 事件驱动超时校验: `Created` 状态超过 `orderTimeoutMs` 未获确认 = 结果不确定，
   抛错终止 (REST 超时更短，正常情况下到不了这里)

### 启动顺序保证 (addStrategies)

1. Executor 按自己的 `Subscription` 订阅总线 (之后的事件不丢)
2. REST 查初始持仓并发布 (未返回的 symbol 显式推 size=0)
3. REST 查账户信息 (净值/名义价值) 并发布 (杠杆率等风控决策依赖)
4. REST 查现有挂单并发布 (接管遗留订单)
5. 向交易所订阅行情 (数据从此开始流动)

账户净值随行情持续变动且无对应 WS 推送，Engine 以周期 REST 刷新
(`accountRefreshMs`，默认 10s) 持续发布 `Topics.AccountInfo` 事件保证风控数据新鲜。
注意净值最多滞后一个刷新周期，临界风控阈值 (如杠杆率上限) 应自留余量。

dry-run 模式下信号以 Error 事件即时清理 pending，做市类策略会随行情 tick 高频空转，
dry-run 仅适合验证接线，策略行为观察请用小额实盘。

## Binance 接入说明

- REST: `https://fapi.binance.com`，私有接口 HMAC-SHA256 签名。
- WS 按 Binance 路由端点拆分连接:
  - `/public/ws`: bookTicker → BBO
  - `/market/ws`: markPrice@1s → MarkPrice + IndexPrice + FundingRate (一条消息拆三个事件)
  - `/private/ws/<listenKey>`: ORDER_TRADE_UPDATE → OrderUpdate/Fill, ACCOUNT_UPDATE → Balance/Position；
    listenKey 每 30 分钟续期，续期失败/过期即终止
- 注意: 未路由的旧端点 `wss://fstream.binance.com/ws` 不再推送 markPrice 等 `/market` 路由的数据。
- Binance 每 24 小时强制断开连接——fail-fast 模型下进程至少每日重启一次，需外层自动拉起。

## 运行演示与测试

```bash
sbt "runMain hft.demo.HftDemo"    # 公开行情 dry-run 演示 (无需 API key)
sbt test                          # 单元测试 (domain 精度/费率、订单生命周期、Executor 链路、策略行为)

# BBO 做市策略 (需 API key；默认 dry-run，LIVE=1 真实下单)
BINANCE_API_KEY=.. BINANCE_API_SECRET=.. sbt "runMain hft.demo.MakerDemo"
```

dry-run 模式接入 Binance 公开行情 (无需 API key)，可观察完整闭环：
行情流入 → 状态聚合 → 资金费率信号 → dry-run 下单 → pending 订单 5 秒超时清理。
配置 `BINANCE_API_KEY` / `BINANCE_API_SECRET` 环境变量可接入私有流 (真实下单需去掉 dryRun)。
