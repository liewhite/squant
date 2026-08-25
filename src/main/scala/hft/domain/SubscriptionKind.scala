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
  case FundingRate(symbol: Symbol)
  case BBO(symbol: Symbol)
  case MarkPrice(symbol: Symbol)
  case IndexPrice(symbol: Symbol)

  /** 公共成交印记 (逐笔成交)，作策略信号 (如 K 线/动量)，不参与撮合 */
  case Trade(symbol: Symbol)

  def subscribedSymbol: Symbol = this match
    case FundingRate(s) => s
    case BBO(s)         => s
    case MarkPrice(s)   => s
    case IndexPrice(s)  => s
    case Trade(s)       => s
