#!/usr/bin/env python3
"""Compute realized volatility (RV) for ETHUSDT from Binance Vision trades zips.

CSV cols: id, price(1), qty, quote_qty, time_ms(4), is_buyer_maker
Samples last trade price per time-bucket at several frequencies (signature plot),
then RV = sqrt(sum(log-return^2) * annualization). Crypto = 365*24h calendar.
"""
import zipfile, io, math, sys
from pathlib import Path
from datetime import date, timedelta

DIR = Path("data-cache/futures/um/daily/trades/ETHUSDT")

def daterange(a, b):
    d = date.fromisoformat(a); end = date.fromisoformat(b)
    out = []
    while d <= end:
        out.append(d.isoformat()); d += timedelta(days=1)
    return out

# 用法: python3 scripts/rv.py [START YYYY-MM-DD] [END YYYY-MM-DD]; 缺省 06-14..06-16
if len(sys.argv) >= 3:
    DATES = daterange(sys.argv[1], sys.argv[2])
else:
    DATES = ["2026-06-14", "2026-06-15", "2026-06-16"]
BUCKETS_MS = {"1s": 1_000, "1min": 60_000, "5min": 300_000}

# bucket_key -> last (price) seen in that bucket, per frequency
last_in_bucket = {name: {} for name in BUCKETS_MS}
first_ts = None
last_ts = None
n_trades = 0
day_hilo = {}  # date -> (open, high, low, close) for Parkinson cross-check

skipped = []
for d in DATES:
    zp = DIR / f"ETHUSDT-trades-{d}.zip"
    if not zp.exists():
        skipped.append(d); continue
    o = h = l = c = None
    with zipfile.ZipFile(zp) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            for raw in io.TextIOWrapper(f, encoding="utf-8"):
                # split only enough fields
                parts = raw.split(",", 5)
                try:
                    price = float(parts[1]); ts = int(parts[4])
                except (IndexError, ValueError):
                    continue
                n_trades += 1
                if first_ts is None: first_ts = ts
                last_ts = ts
                if o is None: o = price
                c = price
                h = price if h is None or price > h else h
                l = price if l is None or price < l else l
                for name_b, ms in BUCKETS_MS.items():
                    last_in_bucket[name_b][ts // ms] = price
    day_hilo[d] = (o, h, l, c)

present = [d for d in DATES if d in day_hilo]
T_days = (last_ts - first_ts) / 86_400_000
print(f"窗口 {present[0]}..{present[-1]}  天数={len(present)} (跳过缺失 {len(skipped)}: {skipped})")
print(f"trades={n_trades:,}  span={T_days:.2f} days")
print(f"price: open={day_hilo[present[0]][0]:.2f}  close={day_hilo[present[-1]][3]:.2f}  "
      f"({(day_hilo[present[-1]][3]/day_hilo[present[0]][0]-1)*100:+.2f}%)")
print()

def rv(prices):
    rets = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
    rvar = sum(r*r for r in rets)                 # realized variance over window
    ann = rvar * (365.0 / T_days)                 # annualized variance
    return math.sqrt(rvar), math.sqrt(ann), len(rets)

print(f"{'sample':>6} {'n_ret':>10} {'RV_win':>9} {'RV_ann':>9} {'RV_week':>9}")
for name_b in BUCKETS_MS:
    keys = sorted(last_in_bucket[name_b])
    prices = [last_in_bucket[name_b][k] for k in keys]
    rvwin, ann, nret = rv(prices)
    week = math.sqrt((rvwin**2) * (7.0 / T_days))
    print(f"{name_b:>6} {nret:>10,} {rvwin*100:>8.3f}% {ann*100:>8.2f}% {week*100:>8.3f}%")

# Parkinson high-low estimator (daily avg), noise-robust cross-check
park_daily = []
for d in present:
    o, hi, lo, cl = day_hilo[d]
    park_daily.append((math.log(hi/lo)**2) / (4*math.log(2)))
park_var_day = sum(park_daily) / len(park_daily)
print(f"\nParkinson daily vol = {math.sqrt(park_var_day)*100:.3f}%  "
      f"annualized = {math.sqrt(park_var_day*365)*100:.2f}%")
