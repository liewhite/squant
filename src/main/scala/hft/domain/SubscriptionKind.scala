package hft.domain

/** 要向交易所订阅的一条公共行情流。
  *
  * 私有数据 (Position / Balance / OrderUpdate / AccountInfo) 不在其中：它们由账户流在连接
  * 建立时自动推送，不需要逐条订阅。
  *
  * ## 为什么在 domain 而不在 exchange
  *
  * 它是**描述**（要哪一路行情），不是传输。放在 domain 让
  * [[hft.event.MarketTopic]] 能够把"本行情族对应哪条流"作为**抽象成员**要求下来 ——
  * 于是"声明了一个行情 topic 却没说它该订哪条流"变成编译错误。
  *
  * 此前这层依赖是反的（exchange 依赖 event），只能在 exchange 侧手写一张
  * `topic -> 流` 的表，再用加载期 `require` 断言它与 `Topics.market` 一致。那张表有两个洞：
  * 断言只在**首次触碰**该对象时才跑；而且它比对的是框架内置的那个固定集合，
  * **用户自定义的 `MarketTopic` 根本不在比对范围内** —— 声明了、被认作交易标的、却永远
  * 订不到数据，没有任何症状。把方向倒过来之后这两个洞一起消失。
  */
enum SubscriptionKind:
  case FundingRate(instrument: Instrument)
  case BBO(instrument: Instrument)
  case MarkPrice(instrument: Instrument)
  case IndexPrice(instrument: Instrument)

  /** 公共成交印记 (逐笔成交)，作策略信号 (如 K 线/动量)，不参与撮合 */
  case Trade(instrument: Instrument)

  /** 订阅的是哪个标的 —— 带品种。
    *
    * 从前这里只有 symbol，于是适配层拼订阅参数时只能假定一种品种（OKX 侧就是恒拼
    * `-SWAP`）。同一个 `ETH` 上的期权盘口与永续盘口是两条不同的流，订阅参数也不同，
    * 少了品种就表达不出来 —— 而"订了个空"没有任何症状。
    */
  def subscribedInstrument: Instrument = this match
    case FundingRate(i) => i
    case BBO(i)         => i
    case MarkPrice(i)   => i
    case IndexPrice(i)  => i
    case Trade(i)       => i
