# TrendRide v4 — 三周期 MACD 共振 (15m 边沿扳机) 回测结果

> 代码：`strategy.strategies.trendride.{logic.TrendRideLogic, logic.TrendRideStrategy}`
> 回测启动器：`strategy.strategies.trendride.backtest.TrendRideBacktest`
> 数据：Binance UM 永续 trades（`data-cache/futures/um/daily/trades/`，已下载，无需联网）

## 策略规则 (多空对称)

三个独立测量，全部用**已收盘**的 MACD 值（无前视、无盘中抖动）：

| 周期 | 看多 (Bull) | 看空 (Bear) | 中性 | 角色 |
|---|---|---|---|---|
| **15m** | 柱**连升 3 根** (`rising(2)`) | 柱**连降 3 根** (`falling(2)`) | 其余 | **扳机** (进场时机) |
| **1h** | 柱在零轴上 (>0) | 柱在零轴下 (<0) | 恰在零轴 | 确认 / 必要条件 |
| **4h** | 柱升 (`rising(1)`) **且** DEA 水上 (>0) | 柱降 **且** DEA 水下 (<0) | 二者不同向 | 确认 / 必要条件 |

- **开仓 (仅空仓时，边沿触发)**：15m 柱**刚出现**连升/连降的**那一拍**（`lastM15Lean` 的 false→true 跳变），且 1h、4h **当拍同向确认** → 开多 / 开空。
  强调"信号出现的瞬间"配合另两周期，**而非三者状态恰好同真的任意时刻**（后者会在 15m 早已连升、1h/4h 才补上时也开仓，非预期）。
- **离场 (持仓时，level，从快，不看扳机)**：不符合当前方向的周期 ≥2（反向**或**转中性）→ 平到 0。
- **反手**：平仓同一拍若又出现反向扳机 + 确认 → 一步反手；否则留平，等下次反向扳机。
- 仓位：离散三态（满多 / 平 / 满空），名义 = `leverage × 当时权益 / 进场价`（默认 1×，进场冻结）；全 taker 进出，fee 0.05%。

## 回测结果 (ETHUSDT，warmup 21d)

`edge` = 最终的边沿扳机版；`level` = 早期电平版（任意时刻三者同真即开），仅作对照。

| 区间 | 版本 | 策略收益 | Buy&Hold | 超额 | 最大回撤 | 成交 |
|---|---|---|---|---|---|---|
| **2025-05-01 → 2026-06-16 (13.5mo)** | **edge** | **+18.77%** | −0.02% | **+18.79%** | **39.35%** | 554 |
| | level | +22.32% | −0.02% | +22.34% | 41.87% | 592 |
| **2026-05-01 → 06-16 (急跌)** | **edge** | **+7.40%** | −20.57% | +27.97% | 8.21% | 64 |
| | level | +7.50% | −20.57% | +28.07% | 9.37% | 66 |
| 2026-05 单月 | level | −3.94% | −11.09% | +7.15% | 9.37% | 43 |
| BTCUSDT 2026-05 | level | −1.27% | −3.48% | +2.20% | 6.31% | 48 |

净值曲线（策略 vs buy&hold + 仓位方向带 + **价格线上买卖点** ▲买/▼卖，浏览器直接打开）：
- 全程 13.5 个月：[`v4_eth_2025-05_2026-06.html`](./v4_eth_2025-05_2026-06.html)
- 急跌段（含买卖点标记）：[`v4_eth_2026-05_06_marked.html`](./v4_eth_2026-05_06_marked.html)，成交明细 [`v4_eth_2026-05_06_fills.csv`](./v4_eth_2026-05_06_fills.csv)

> 出图/落盘已沉淀进框架 `strategy.utils.backtest.{BacktestRecorder, BacktestReport}`：任何回测只需把 `rec.observe`
> 放进 `BacktestEngine(observers=...)`，跑完 `BacktestReport.write(...)` 即自动产出 `*_equity.csv` / `*_fills.csv` /
> `*.html`（净值曲线 + 仓位带 + 买卖点）。买卖点标在 buy&hold 价格线上（纵值 = 成交价/起点价）。

## 结论

- **首个跨多 régime 转正的 TrendRide 版本**：13.5 个月里 ETH 大幅来回、最终几乎回到原点（b&h −0.02%），策略靠趋势骑乘抽出 **+18.77%**——正是趋势策略应有表现（v1/v2/v3 多周期均未转正，详见 [`analysis.md`](./analysis.md)）。
- **头号问题是回撤 ~39%**：alpha 真实，但大趋势之间被震荡 / whipsaw 反复甩，净值路径粗糙。
- edge vs level：边沿语义更严，收益略低、回撤略低、单数略少；差异不大（"迟到补开"约占进场 6%），但 edge 才是符合"15m 是扳机"语义的正确实现。
- **下一步若优化，应针对回撤而非收益**：例如震荡期空仓过滤（波动率 / 趋势强度门槛）、或仓位随信号强度缩放，而非满仓三态。

## 复现

```bash
# 指定区间，优先用已下载数据 (避免重新下载)
OUT_DIR=/tmp/trendride sbt 'runMain strategy.strategies.trendride.backtest.TrendRideBacktest ETHUSDT 2025-05-01 2026-06-16'
```

> 全程 13.5 个月纯处理约 27 分钟（无网络）。纯逻辑单测：`testOnly strategy.strategies.trendride.logic.TrendRideLogicSpec`（10/10 通过）。
