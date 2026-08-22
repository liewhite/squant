package hft.backtest
import hft.event.AnyEvent

/** 回测行情数据源：产出**全局按时间戳升序**的市场事件 (BBO / MarketTrade / ...)。
  *
  * 这是回测与"数据从哪来、怎么缓存"之间的唯一缝隙。[[BacktestEngine]] 只消费有序事件流，
  * 不关心是币安历史文件、内存假数据还是别的来源。
  */
trait MarketDataSource:
  def events(): Iterator[AnyEvent]
