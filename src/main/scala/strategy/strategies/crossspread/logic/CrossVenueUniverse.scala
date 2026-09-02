package strategy.strategies.crossspread.logic

import hft.domain.{Exchange, Instrument, Symbol}

/** 从各所的合约清单推出**跨所价差对** —— 纯函数：同一份清单必得同一批对，可脱离网络单测。
  *
  * ## 为什么需要"候选集"这个参数
  *
  * 各所的清单回答的是"我上了哪些合约"，回答不了"这个代码在别的所指的是不是同一个东西"。
  * 加密永续与传统资产永续会撞代码，同一个代码在两个所也可能是两种资产。因此候选集由
  * **能自己分类的交易所**给出 (币安的 `TRADIFI_PERPETUAL`、Hyperliquid 的股票 dex)，
  * 只会命名不会分类的所 (OKX 的清单里加密与股票混在一起) 靠名字加入已知的候选。
  *
  * 命名判据只是第一道；配错的最终防线在价格上 —— 见 [[PairRejection]]。
  */
object CrossVenueUniverse:

  /** @param candidates 候选标的代码
    * @param listings   各所清单: 交易所 -> (标的代码 -> 该所的框架 Symbol)
    * @param minVenues  一个代码至少在几家上市才配对 (少于 2 家没有价差可言)
    * @return 全部两两组合，按 (代码, 交易所) 排序 —— 顺序确定，启动日志才可对比
    */
  def pairs(
      candidates: Set[Ticker],
      listings: Map[Exchange, Map[Ticker, Symbol]],
      minVenues: Int = 2,
  ): Vector[VenuePair] =
    require(minVenues >= 2, s"一个价差对至少要两家交易所, minVenues=$minVenues")
    candidates.toVector.sorted.flatMap { ticker =>
      val venues = listings.toVector
        .flatMap((exchange, symbols) => symbols.get(ticker).map(symbol => Instrument(exchange, symbol)))
        .sortBy(_.toString)
      if venues.sizeIs < minVenues then Vector.empty
      else venues.combinations(2).map(pair => VenuePair.of(ticker, pair(0), pair(1))).toVector
    }
