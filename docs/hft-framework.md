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

进程 = **一条总线 + 若干插件**。行情、账户回报、时钟、下单指令、控制指令都是总线上的
事件，区别只在 topic：

```
行情插件 ──┐                       ┌── Executor (Strategy + StateManager) ──┐
柜台插件 ──┼──> bus (topic 路由) ──┤                                        │ 下单指令
时钟插件 ──┘                       └── 观察者                               │
     ▲                                                                      │
     └──────────────────────────────────────────────────────────────────────┘
```

下单指令不会回流给策略 —— 策略压根不订阅它，隔离由订阅关系保证，不靠总线拓扑。

**引擎不知道交易所**：它只发指令（行情订阅、账户对齐、下单撤单），谁接、有几个接、
接的是真交易所还是虚拟柜台，一概不需要知道。完整设计见
[插件式事件总线架构](plugin-bus-architecture.md)。

### 数据面与指令面

总线上流动两类事件，**校验规则不同**：

| | 数据面 | 指令面 |
|---|---|---|
| 语义 | 发生了什么 | 请做什么 |
| 例子 | 盘口、成交、持仓、回报、净值、时钟 | 订阅行情、启动对齐、下单撤单 |
| 无人订阅 | 正常（某标的这刻没成交不是错误） | **致命**，且零症状 |
| 校验 | 不校验 | 装配期：每条指令必须有接单者 |

引擎在 `addStrategies` 时查一次总线"这条 (topic, key) 有没有订阅者"，没有就拒绝启动。
查的是**订阅事实本身**，因此不需要任何插件声明"我提供什么" —— 订阅是它为了工作本来
就必须做的事，多一份声明就多一处会漏写的事实。

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

`PaperCounter` 是一个柜台插件，订阅自己那个 `Paper(n)` 的下单指令 + 公共行情，用与回测
同一个撮合内核（`SimState`）成交，回报标自己的账户发回总线。它与 `SimulatedExchange` 的
区别是**定位而非撮合**：后者替换掉真实交易所的两个插件（策略对真假无感知），
前者与实盘同时存在。

```
                    ┌─> Executor@Live    ─┐            ┌─> 真实柜台@(Live, Binance)  ─> REST
真实行情 ─> bus ────┤   (同一份策略逻辑)   ├─ 下单指令 ──┤
                    └─> Executor@Paper(1) ┘            └─> PaperCounter@(Paper(1), Binance)
```

两个柜台互不知情 —— 分发由下单指令的 **(账户, 交易所)** 路由完成。少了交易所这一维，
拆出多个柜台后两边都会收到同一条意图：静默双执行，没有任何症状。

影子柜台**也按交易所精度对齐** —— 否则它的成交量与实盘系统性地差一个取整，
而它存在的全部理由就是预测实盘。

**建模延迟**：影子盘存在的理由是预测实盘表现，没有下单在途与回报回传的延迟就会系统性
偏乐观，据此得出的结论无法外推。延迟用 `ActorContext.scheduleEvent` 表达：定时器只把发布
推迟到点，撮合仍在 actor 线程串行进行（单线程定时器是顺序保证的承重墙）。

### 一个柜台核心，三个驱动

"虚拟柜台"这件事在系统里有三个出场：回测引擎、实盘替身 `SimulatedExchange`、影子盘
`PaperCounter`。它们共用的不只是撮合状态机，还有**延迟语义**——都在 `hft.sim.Counter`：

```
Counter.step(state, exchange, input: CounterInput, now, config)
  -> (SimState, Vector[Delayed[AnyEvent]])
```

命令形态（`CounterInput`：行情到达 / 下单到达 / 撤单到达）与"回报该晚多久送达"都由核心给出，
三个驱动只回答两个问题：**现在几点**（虚拟时间 / 墙钟）、**事件往哪送**（虚拟时间优先队列 /
定时器加总线）。

这不是为了少写代码。三者若各写一份延迟模型，改了其中一份不会有任何编译错误 ——
它们只是从此对同一个策略给出不同结论，而"结论应当一致"正是这套东西全部的价值所在。

**撮合不回显行情**：`SimState.onMarket` 只返回撮合产生的回报，不把输入的行情再吐一遍。
转发行情是**网关**职责：实盘替身要转发（策略的行情只有它这一个来源），影子盘不能转发
（策略直接从总线读真实行情，转了就是重复投递），回测由引擎自己转发。此前行情混在回报里
返回，影子盘不得不再滤掉一次 —— 一个本不属于撮合的事实污染了撮合的输出形态。
"行情先于它引发的成交送达策略"这条保证因此落在驱动层，测试也在那里。

**如实声明的偏差**：撮合不建模**队列位置**，也不建模**盘口深度**，两侧刻意取不同方向的
近似（`Matcher`，回测与影子盘共用）：

- **maker（被动挂单）取悲观侧**：价格必须**严格穿越**挂单价才算成交，仅仅触及不算 ——
  真实盘口里那个价位上排着队，价格没穿过去意味着队列没消化到我。行情是 L1 盘口还是逐笔
  成交，判据同一份（`crossedByBbo` / `crossedByTrade`）。
- **taker（主动吃单）取乐观侧**：与盘口**价格重合即全量成交**（`marketable`），在对手价
  一次成交，不看盘口挂单量、不建模吃穿多档的冲击成本。

两侧判据只差一个等号，中间留出一格**有意的间隙**：买单挂在 L、盘口 ask 恰好等于 L 时，
订单此刻到达就按 taker 成交，早已在簿上就不成交。这不是漏洞，正是"主动吃 vs 被动等"的
真实差别。净效果是 maker 成交机会偏少、taker 成本偏低，所以拿影子盘做晋升判据时门槛要留
余量；晋升后实盘与影子并行，两边成交率之差正是校准这个偏差的数据。

**影子账户的启动对齐**：它从零开始，没有历史仓位与挂单要恢复，唯一的初值（净值）由柜台
周期发布。绝不能拿真实交易所的持仓去对齐一个模拟账户 —— 那是别人的仓位。

**单位由类型保证**：`Coin`（币本位）与 `Contracts`（合约张数）是两个 opaque type，
运行时都是 `double`（零开销），但**不可互换**。

- 框架内一律 `Coin` —— 策略、账本、仓位、回报、撮合、`OrderIntent`。
- `Contracts` 只出现在 exchange 适配层内部：下单前 `toContracts`、解析回报时 `toCoin`。
- `ExchangeClient.placeOrder` 只收 `ExchangeOrder`（数量是 `Contracts`），于是"忘了换算
  就直接下单"在类型上写不出来。
- 取整分两步：`roundToExchangePrecision` 把数量对齐到交易所精度但**仍是币本位**
  （回测与实盘因此看到同一个数），`toExchangeOrder` 才换成张数。

这条边界此前靠惯例：OKX 适配层做了 `qtyToCoin`，Binance/Bybit **根本没做** —— 只是它们
`contractSize = 1` 掩盖着，虚拟柜台漏做同样被掩盖。现在这类混用一律编译失败。

代价是跨概念运算要显式：`qty.notional(price)` 而不是 `qty * price`（数量 × 价格不再是数量）。
`Coin` 不设 `<: Double` 上界正是为此 —— 有上界时 `Coin + Coin` 会优先匹配 `Double` 的 `+`
并悄悄退化，等于白设。实测代价很小：三个业务策略加起来只多了 10 处显式解包。

**换算精度**：发单路径的换算全程走 BigDecimal（`roundCoinDown` / `toExchangeContracts`）。
裸浮点除法会失真到**整整一档** —— `0.3 / 0.1 = 2.9999999999999996`，FLOOR 之后是 2 而不是 3；
`0.07 / 0.01 = 7.000000000000001`，而 client 的格式化不再取整，交易所会按 lot size 拒单。
**取整一律就近 (HALF_UP)，不向下**。交易所是十进制记账的：1000 笔 0.1 的成交，它那边持仓
精确等于 100，而我们的浮点求和会漂成 `99.99999999999860` —— 错的是我们，不是它。对齐到 step
网格恰恰是把真值恢复回来，**取整后的数字才是那个真实的决策**。

向下取整会把这点漂移放大成整整一档：`99.99999999999860` FLOOR 成 `99.999`，平仓就留下
`0.000999` 的残仓；它比 `Position.Epsilon` 大七个数量级（判不出"已归零"），又小于最小下单量
（再也发不出单），于是永远平不掉、`Supervisor` 永远告警。BigDecimal 只保证除法不引入**新**误差，
救不了本身就带累加误差的输入，取整方向才是决定因素。

多取一档的代价小得多：reduceOnly 由撮合层与交易所双重截断，开仓方向也只是多一个 step 的敞口。

解析与展示路径（`toContracts` / `toCoin`）保持裸浮点 —— 那里的值本身就是近似的，
且行情解析是热路径。

**每个适配层只声明一种原生单位约定**：Binance USDⓈ-M 与 Bybit linear 的原生数量就是币本位
（`contractSize = 1`），WS 与 REST 两条路径统一直接 `Coin`；OKX 原生是张数，两条路径统一
`toCoin`。同一适配层对"本所原生单位是什么"给两个答案，正是这次要消灭的病。

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

### 框架与策略的分界

策略接触到的框架面，只有这些：

| 给策略的 | 是什么 |
|---|---|
| `Strategy` / `StrategyHandlers` / `StrategyContext` | 实现契约与能力面 |
| `StateView` / `SymbolView` | **只读**状态视图 |
| `Topics` / `AnyEvent` | 订阅与产出事件 |
| `domain` / `indicator` / `option` | 纯数据与纯计算 |

**能力靠类型划界，不靠约定**。策略够不着的东西：

- `StateManager.apply`（事件应用）与 `addPendingOrder`（挂单登记）—— 这两件事框架在固定
  位置做，策略插一脚的后果是"同一条事件重复计入仓位"和"登记一条无主挂单"，都没有外在症状。
- `SymbolState` 的可变集合 —— 从前它们是 `public val mutable.Map`，策略能 `positions.clear()`
  或往 `bbos` 里塞假行情。现在 `SymbolView` 上根本没有这些成员。
- `AccountOutcome` 的构造器（`private[hft]`）—— 策略拼不出下单意图，因此绕不过
  `ctx.place` 的 clientOrderId 生成 / pending 登记 / 精度换算，也冒充不了别的账户。
- 账户值本身 —— `ctx` 能用它构造下单意图，读不到它是哪个账户，所以策略里写不出依赖
  "我是实盘还是影子"的分支。

依赖方向单向：`hft.*` 对具体策略零引用，策略只依赖抽象。

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
| `event` (指令面) | `Commands` — 行情订阅 / 账户对齐 / 下单撤单三条指令及其载荷 |
| `exchange` | 插件形态 `MarketFeed` / `TradingGateway` / `RestTradingGateway` / `AccountFeed`；传输层 `ExchangeClient` / `TradingClient`；`WsLoop` (通用连接泵) |
| `engine` | `Engine` (装配 + 契约校验 + 生命周期)、`Executor` (策略插件)、`StrategyRunner` (纯逻辑核心)、`Clock` |
| `strategy` | `Strategy` / `StrategyHandlers` / `StrategyContext` — 策略契约与能力面 |
| `sim` | `SimState` (撮合状态机) / `Ledger` / `Matcher` / `Counter` (柜台核心) / `SimulatedExchange` / `PaperCounter` |
| `backtest` | `MarketDataProvider` 抽象 + `BacktestEngine` (虚拟时间驱动) + 各交易所数据实现 |
| `perf` | `PerformanceTracker` / `PromotionPolicy` / `Supervisor` — 战绩统计与晋升调度 |
| `indicator` / `option` | 纯计算: K 线与技术指标 / Black-Scholes 与希腊字母 |
| `exchange/{binance,okx,bybit}` | 各所实现 (REST 签名、公共/私有 WS、jsoniter 编解码) |

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

### 两种插件形态

接入一个新交易所 = 装两个插件，框架其余部分零修改：

- **`MarketFeed`**（行情插件）：接本所的行情订阅指令，把公共行情发布到总线。
  订阅是**增量幂等**的 —— 策略动态添加，后来者声明的流集合与先前的必然重叠。
  数据源不必是交易所：历史回放、第三方数据商、跨进程桥接都可以是它的实现。
- **`TradingGateway`**（柜台插件）：**一个账户在一个交易所上的执行与汇报**。
  接下单指令与对齐指令，产出回报，并周期刷新净值。

柜台是"交易所侧知识"的唯一归属地 —— 精度、张数、最小下单量、账户当下什么样都在这里。
三家交易所在执行面几乎一样，收进 `RestTradingGateway` 一个具体类；差异只剩私有推送解析，
那是注入的 `AccountFeed`。

**虚拟柜台与真实柜台在总线上完全同形**：订同样的指令、产出同样的回报，策略无法分辨。
这是"同一份逻辑同时跑实盘与影子盘"的结构基础。

**没有凭证就不装柜台** —— "这个所能不能下单"因此是装配期的事实（装没装柜台），
由契约校验回答，不是运行时才发现的错误。

编写一个新策略 = 实现 `Strategy` (声明订阅 + 纯函数式处理器)。

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
- `RestTradingGateway` 每个 REST 调用 fork 独立虚拟线程，下单互不阻塞，也不阻塞柜台的事件循环。

### 仓位归柜台

仓位是**柜台的账本**：初值来自启动对齐，之后每笔成交进 `Ledger`。策略只读 `Topics.Position`。

从前这份账在策略侧（每个实例用 `Fill` 自己累加），而交易所推来的权威读数被全部丢弃。
搬到柜台之后，「我累加的」与「交易所报的」落在同一个角色手里，**对账**才成为可能：
差异超过一个最小变动单位就告警，但**不覆盖**（跨频道无顺序保证，覆盖会双重计数）。
修复走 REST 对齐——检测用推送，修复用 REST。

因此框架补齐给策略的私有回报是 `Position` + `OrderUpdate` 两样，**不含 `Fill`**；
要成交明细的策略自己声明 `own(Topics.Fill)`。

### 订单生命周期

1. 策略产出 `PlaceOrders` (币本位数量)；跨所的一次决策由框架按交易所自动拆成多条指令
2. `StrategyRunner` 生成 `clientOrderId` 并以币本位登记 pending —— **不做精度对齐**，
   策略这一侧从头到尾只有币本位的意图
3. 柜台按 `SymbolMeta` 对齐精度后调 REST 下单。三种确定性失败走**同一条**回流路径
   (`OrderUpdate(Error)`)：精度收不下、交易所明确拒绝 (4xx)、dry-run；
   结果不确定 (网络/超时/5xx) 与限频 (429/418) 直接终止
4. 柜台把成交记进自己的账本，产出**三条回报**：`Position` → `Fill` → `OrderUpdate`。
   仓位排第一是硬要求 —— 策略普遍在成交回调里读仓位决策，顺序反了读到的就是成交前的数。
   撤单终态同样以私有流推送为准，框架不合成确认事件
5. Clock 事件驱动超时校验: `Created` 状态超过 `orderTimeoutMs` 未获确认 = 结果不确定，
   抛错终止 (REST 超时更短，正常情况下到不了这里)

### 启动顺序：一条因果链，不是一串调用顺序

```
1. 标的独占检查        —— 冲突在策略启动之前拒绝
2. 指令契约校验        —— 它要发的每条指令都有人接吗
3. spawn, 订阅总线     —— 此后发布的事件不会丢
4. 发对齐指令, 等应答  —— 柜台推 持仓 → 净值 → 既有挂单 → 完成应答
5. 发行情订阅指令      —— 数据从此刻开始流动
```

第 4 步在第 5 步之前，保住的是：**策略在拿到初始仓位之前不会看到第一条行情**。
否则它基于"仓位为空"这个错误前提做第一次决策 —— 可能重复开仓，可能对着不存在的敞口对冲。
前两步都排在 spawn 之前：起来了再拒绝，就得再把它停回去。

对齐是**柜台的职责**：它就是那个知道账户当下什么样的角色。交易所没返回的标的由框架
显式推零仓 —— "没有推送"与"仓位为零"在策略看来无从分辨，它会一直等下去。
**影子账户不对齐**（从零开始，没有历史可恢复）。

请求—应答在发布订阅上通常别扭，因为失败无从表达。这里不别扭：对齐失败一律抛异常终止
进程，"没人接"已被第 2 步挡在前面 —— 不存在"既没成功也没失败"的第三种结局。

账户净值随行情持续变动且无对应 WS 推送，**柜台自己**周期 REST 刷新
(`accountRefreshMs`，默认 10s) 发布 `Topics.AccountInfo` 保证风控数据新鲜。
注意净值最多滞后一个刷新周期，临界风控阈值 (如杠杆率上限) 应自留余量。

## Binance 接入说明

- REST: `https://fapi.binance.com`，私有接口 HMAC-SHA256 签名。
- WS 按 Binance 路由端点拆分连接:
  - `/public/ws`: bookTicker → BBO
  - `/market/ws`: markPrice@1s → MarkPrice + IndexPrice + FundingRate (一条消息拆三个事件)
  - `/private/ws/<listenKey>`: ORDER_TRADE_UPDATE → OrderUpdate/Fill, ACCOUNT_UPDATE → Balance/Position；
    listenKey 每 30 分钟续期，续期失败/过期即终止
- 注意: 未路由的旧端点 `wss://fstream.binance.com/ws` 不再推送 markPrice 等 `/market` 路由的数据。
- Binance 每 24 小时强制断开连接——fail-fast 模型下进程至少每日重启一次，需外层自动拉起。

## 运行与测试

```bash
sbt test                          # 单元测试 (撮合语义、订单生命周期、事件路由、回测确定性)

# 回测 (读 data-cache/, 缺的日期自动从 data.binance.vision 下载)
sbt "runMain strategy.strategies.macdgrid.backtest.MacdGridBacktest ETHUSDT 2026-05-01 2026-05-31"
sbt "runMain strategy.strategies.gridsellhedge.backtest.GridSellHedgeBacktest"
sbt "runMain strategy.compare.WeeklySellVolBacktest"

# 实盘 / 影子盘启动器 (需 conf/ 下的凭证配置)
sbt "runMain strategy.strategies.volsell.live.VolSellLauncher"
sbt "runMain strategy.strategies.makerhedge.live.PerpHedgeEngineLauncher"
sbt "runMain strategy.strategies.ivsellhedge.live.OkxIvSellHedgeLauncher"   # IV 定量卖出 + KAMA 死区对冲
```

dry-run 模式下信号以 Error 事件即时清理 pending，做市类策略会随行情 tick 高频空转 ——
它只适合验证接线，策略行为观察请用小额实盘或影子盘。
