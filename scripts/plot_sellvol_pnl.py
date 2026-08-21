#!/usr/bin/env python3
"""卖方 short-vol 周度回测 PnL 图。

读 WeeklySellVolBacktest 产出的 curve CSV (trancheStart,trancheEnd,cumTotal,trancheTotal,
按到期日排序), 画两栏:
  上 = 累计已实现净值曲线 (按到期日);
  下 = 每个 1 周 tranche 的盈亏 (绿正红负)。
用法: python3 scripts/plot_sellvol_pnl.py [curve.csv] [out.png]
"""
import csv
import sys
from datetime import date

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# 中文字体兜底 (macOS)，须在绘图前设置
for _fam in ["PingFang SC", "Heiti SC", "Arial Unicode MS", "STHeiti"]:
    plt.rcParams["font.sans-serif"] = [_fam]
plt.rcParams["axes.unicode_minus"] = False

src = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sellvol_curve.csv"
out = sys.argv[2] if len(sys.argv) > 2 else "scripts/sellvol_pnl.png"

ends, cum, per = [], [], []
with open(src) as f:
    for row in csv.DictReader(f):
        ends.append(date.fromisoformat(row["trancheEnd"]))
        cum.append(float(row["cumTotal"]))
        per.append(float(row["trancheTotal"]))

total = cum[-1] if cum else 0.0
wins = sum(1 for p in per if p > 0)
peak, maxdd = 0.0, 0.0
for c in cum:
    peak = max(peak, c)
    maxdd = max(maxdd, peak - c)

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True, height_ratios=[2, 1])

ax1.plot(ends, cum, color="#1f77b4", lw=1.8, marker="o", ms=3)
ax1.axhline(0, color="grey", lw=0.8, ls="--")
ax1.fill_between(ends, cum, 0, where=[c >= 0 for c in cum], color="#2ca02c", alpha=0.12)
ax1.fill_between(ends, cum, 0, where=[c < 0 for c in cum], color="#d62728", alpha=0.12)
ax1.set_ylabel("累计已实现 PnL (USDT)")
ax1.set_title(
    f"卖方 short-vol 周度回测 (1周 tenor, IV=上周RV, maker对冲@BBO外0.01%, 1h MACD柱驱动带宽)\n"
    f"tranche={len(per)}  胜率={wins}/{len(per)} ({wins*100//max(1,len(per))}%)  "
    f"Σ总盈亏={total:+.0f}  最大回撤={maxdd:.0f}",
    fontsize=11,
)
ax1.grid(alpha=0.25)

colors = ["#2ca02c" if p >= 0 else "#d62728" for p in per]
ax2.bar(ends, per, width=5, color=colors, alpha=0.85)
ax2.axhline(0, color="grey", lw=0.8)
ax2.set_ylabel("单 tranche PnL")
ax2.set_xlabel("到期日 (trancheEnd)")
ax2.grid(alpha=0.25)

for ax in (ax1, ax2):
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(30)
        lbl.set_ha("right")

# 中文字体兜底 (macOS)

fig.tight_layout()
fig.savefig(out, dpi=130)
print(f"saved {out}  (总盈亏 {total:+.1f}, 胜率 {wins}/{len(per)}, 最大回撤 {maxdd:.1f})")
