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
| `Instrument` | Bbo / Trade / MarkPrice / IndexPrice / FundingRate | 公共行情，**无账户归属**，一份服务所有账户 |
| `AccountInstrument` | Position / OrderUpdate / Fill | 私有回报，账户流推送 |
| `AccountExchange` | Balance / AccountInfo / Greeks | 账户级读数 |
| `AccountId` | OrderIntent | 策略信号，按账户路由到各自出口 |
| 无 | Clock | 全局节拍，用 `Interest.All` 订阅 |

### 账户维度

`AccountId` 是 `Live` 或 `Paper(n)`。同一份策略逻辑可以同时跑在实盘与若干影子账户上 ——
它们看同一份行情、下同样的单，只有账户不同。所以**账户不是策略的属性，而是装配期绑定的**：
策略自己不知道也不该知道它跑在哪个账户上，否则同一份逻辑就没法既做实盘又做影子盘。

私有回报带账户维度，是订单归属的**结构保证**：实盘实例与影子实例即便交易同一标的，
也从投递层就收不到对方的成交与订单回报。只按标的路由的话两者会互相收养对方的挂单，
各自还把账户总仓位当成自己的敞口 —— 决策依据错了却没有任何症状。

账户级读数同时按交易所过滤，是另一条越界防线：策略读不到自己没订阅的交易所的净值，
而杠杆闸门正是拿净值算的。

`OrderIntent` 也按账户路由，于是"这条信号该由谁执行"由投递层回答：实盘出口订阅
`{Live}`，每个虚拟柜台订阅自己那个 `Paper(n)`，两个出口互不知情。若改成全量订阅再各自
过滤，新增一类账户时两处都不会编译失败，失效方式是静默双执行或静默不执行。

**唯一性约束**（`InstrumentClaims`，装配期 fail-fast）：一个 `(账户, 标的)` 最多归一个策略
实例。实盘与影子盘跑同一标的是允许的 —— 账户不同，键就不同。

### 影子盘：与实盘并行的虚拟柜台

`PaperCounter` 是一个 actor，订阅自己那个 `Paper(n)` 的下单意图 + 公共行情，用与回测同一个
撮合内核（`SimState`）成交，回报标自己的账户发回总线。它与 `SimulatedExchange` 的区别是
**定位而非撮合**：后者替换整个 gateway（策略对真假无感知），前者与实盘同时存在。

```
                    ┌─> Executor@Live    ─┐            ┌─> OutcomeProcessor@Live ─> REST
真实行情 ─> bus ────┤   (同一份策略逻辑)   ├─ OrderIntent ┤
                    └─> Executor@Paper(1) ┘            └─> PaperCounter@Paper(1) ─> 撮合
```

两条出口互不知情 —— 分发由 `OrderIntent` 的账户路由完成。

**建模延迟**：影子盘存在的理由是预测实盘表现，没有下单在途与回报回传的延迟就会系统性
偏乐观，据此得出的结论无法外推。延迟用 `ActorContext.scheduleEvent` 表达：定时器只把发布
推迟到点，撮合仍在 actor 线程串行进行（单线程定时器是顺序保证的承重墙）。

**如实声明的偏差**：撮合不建模**队列位置** —— 只要行情越过挂单价就算成交。成交价格是对的，
成交机会偏多，真实盘口里排在后面的单可能根本轮不到。所以影子盘的成交率与盈亏系统性偏高，
拿它做晋升判据时门槛要留余量；晋升后实盘与影子并行，两边成交率之差正是校准这个偏差的数据。

**影子账户的启动对齐**：它从零开始，没有历史仓位与挂单要恢复，唯一的初值（净值）由柜台
周期发布。绝不能拿真实交易所的持仓去对齐一个模拟账户 —— 那是别人的仓位。

**单位的边界职责**：进入撮合的订单是**交易所格式**（合约张数，`StrategyRunner` 换算过），
而账本、仓位、回报一律**币本位** —— 真实网关正是在回报侧 `qtyToCoin` 还原的。虚拟柜台
必须对称，否则 `contractSize ≠ 1` 的交易所上影子盘的成交量、盈亏、仓位整体差一个
`contractSize` 倍，与实盘不可比。这个还原收在 `SimState.onOrderArrived` 一处入口。
（Binance `contractSize = 1`，会掩盖这类症状 —— 只用它测是测不出来的。）

### 绩效与晋升调度

```
        共享行情 ──> 影子实例 ──> 虚拟柜台 ──> 影子成交 ─┐
                └──> 实盘实例 ──> 交易所   ──> 实盘成交 ─┤
                                                       ↓
                                            PerformanceTracker
                                                       ↓
                                     PromotionPolicy 决定开/关实盘
```

`PerformanceTracker` 从成交流用 `Ledger`（回测同一个）重建每个 `(账户, 标的)` 的战绩。
**不用交易所推的账户净值** —— 那是整个账户的，含其他策略与手工仓位。手续费按名义费率
估算而非账单实数：两边都估算，共同偏差在比较中抵消；若"影子精确、实盘记 0"，两个账户
的数字反而失去可比性。

`PromotionPolicy` 是扩展点，**框架不预设任何阈值**，默认 `NeverPromote` —— 一个"看起来
合理"的内置阈值比没有阈值更危险，它会被当成经过验证的默认值直接用上实盘。写判据前必读
`PromotionPolicy` 的文档，那里记了两个统计陷阱（多重比较、影子盘成交偏乐观）。

`Supervisor` 只做三件事：记录战绩、按节拍问判据、执行决定。几个要点：

- **"实盘在不在跑"只有一个载体**（句柄）。用"有没有战绩"判定会有两个洞：晋升到首笔成交
  之间没有战绩，防重失效会重复拉起实例、旧句柄被覆盖后无人能停；降级后跟踪器仍在发布
  历史战绩，会让实盘状态起死回生、该标的永远无法再晋升。
- **判据看到的是本轮战绩**：跟踪器的账本跨轮存续，不分段的话第二轮实盘的数字里混着第一轮
  的盈亏和降级平仓的成交。晋升时记基线，`SymbolPerformance.live` 给的是增量。
- **降级要平仓，且由监督者做**：`Executor.onStop` 只撤单不平仓（平不平是策略之外的决定），
  而监督者恰恰是做这个决定的那一层。顺序是先撤下再平仓 —— 反过来的话还活着的策略会看见
  平仓成交并可能立刻反手补回去。
- **平仓不是发一单就完事**：仓位读数最迟落后一个发布周期、撤单是异步的，`reduceOnly` 只防
  多平不防少平。所以降级后持续盯该标的的敞口直到归零，平仓单有终态超时，两者都**反复**
  告警 —— 单次日志在无人盯屏时等于没有。

### 策略只声明一处

`Strategy` 的 `handlers` 同时给出"订阅什么"与"怎么处理" —— 订阅声明**从处理器派生**，
因此不存在"订阅了不处理"或"处理了没订阅"。处理器拿到的载荷已是具体类型，
不需要 `as`、不需要模式匹配、也不会漏掉兜底分支。

```scala
def handlers = StrategyHandlers.empty
  .market(Topics.Bbo, instrument) { (bbo, ctx, _) => ctx.place(quote(bbo), "quote") }
  .own(Topics.Fill)               { (fill, ctx, _) => Vector.empty }
  .account(Topics.Greeks)         { (g, ctx, _) => hedge(g, ctx) }
```

`market` / `own` / `account` 三种声明形式对应三档路由维度。策略用**账户无关**的语气说话
（"我自己的成交"而不是"Paper(1) 的成交"），装配期才绑定账户 —— 同一份逻辑因此能同时跑
实盘与影子盘。`market` 的参数限定为 `MarketTopic`：用它声明即表示**交易该标的**，
只是想读一条按标的路由的自定义事件请用 `custom`，那不代表交易。

**`StrategyContext`：账户是构造能力而非可读数据。** 策略能用 `ctx.place` 发单却读不到账户
值；`AccountOutcome` 的构造器是 `private[hft]`，策略连自己拼一条下单意图都做不到 ——
既绕不过 clientOrderId 生成 / pending 登记 / 精度换算，也冒充不了别的账户。

`ctx.emit(topic, payload)` 是**策略对外输出**的通道：外部订阅那个 topic 即可消费，
框架不需要知情。时间戳取自本次事件的处理时刻而非墙钟，回测才能同一输入必得同一结果。

一条纪律：`ctx.place` 只**构造**，副作用（pending 登记）由框架对处理器**真正返回**的意图
施加。若在构造时就登记，策略把结果丢弃或中途抛异常就会留下永远不会发出的幽灵挂单。

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
| `actor` | `Actor`/`ActorContext`/`ActorSystem` — 组件的装配与生命周期树 |
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

### Actor: 组件的统一形态

引擎里所有有生命周期的东西都是 `Actor` —— 策略执行器、下单出口、时钟、账户轮询、
监控与指标导出。此前它们各写各的 `fork { while true ... }`，新增一个就要回到装配处插代码；
现在只需实现 trait 并 `ActorSystem.spawn`。

两种形态共用一个 trait：

- **事件驱动**（策略、下单出口、指标 sink）：声明 `interests`，在 `onEvent` 里消费并产出
  事件。纯函数形态 —— 可单测、可回测、**可被动态起停**。
- **自驱动**（WS 连接、定时器、REST 轮询）：在 `onStart` 里 fork 自己的常驻线程。

**生命周期树**：`ActorContext.spawn` 起的是子 actor。停一个 actor 时先递归停完它的子孙、
等到它们的 `onStop` 真正跑完，再停它自己 —— 没有任何地方需要知道整棵树的形状，
每层只管自己那层，递归自然成立。

**停机是协作式的**，不用中断：中断会把 actor 打断在任意一行上，而它可能正处在
"已发出下单请求、尚未登记 pending"这类不能被腰斩的位置。自驱动循环用
`ctx.sleepUnlessStopped(ms)` 代替裸 `Thread.sleep` 就能被叫停。

**收尾在退订之前**：`onStop` 可以产出最后一批事件（`Executor` 在这里撤掉本策略的挂单），
那时总线与下游都还活着。顺序反了就是漏发指令。

#### 已知限制

- `onStart` 里 fork 的线程不受 `stop` 控制，生命周期绑在根作用域上。
  一个阻塞在 socket 读上的线程没法被协作式叫停，假装能停会让停机链在那里静默等下去。
  要能动态起停的 actor 必须走事件驱动形态。

### 并发模型

- 所有组件都是 `supervised` 作用域内的虚拟线程 fork，任一组件崩溃级联终止整个作用域 (对应 kameo spawn_link)。
  **不做局部重启** —— 一个崩掉的策略留下的挂单与仓位归谁管没有好答案，而重启后的启动对齐有答案。
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
