#!/usr/bin/env python3
"""ETHUSDT 周度已实现波动率 (RV) vs Bybit 隐含波动率 (IV) 对比。

RV:  data-cache 里 Binance ETHUSDT 逐笔成交 → 按 5min(及 1min)取末价 → 周内
     sqrt(Σ log-ret²) 年化(沿用 scripts/rv.py 的方法:实际覆盖时长年化, 365 日历)。
IV:  Bybit /v5/market/historical-volatility (category=option, quoteCoin=USDT, period=7)
     小时级 7 日年化 IV,分页拉取后按 ISO 周取均值。
对比按 ISO 周(周一为周首)对齐,输出 CSV + 图 + 汇总表。

口径提示:IV 是"滚动 7 日"年化波动在该周内的小时均值(其 7 日窗口会跨越周界、含周前历史),
RV 是该 ISO 周内 log 收益的积分年化。二者覆盖区间并不完全相同,corr 应按"周均 7d-IV vs
周内 RV"理解,而非严格同区间对比。残缺首/末周(rv_span_days < 6.5)RV 由实际时长年化放大,
表中以 * 标注。

用法:
  cd /Users/liewhite/projects/ox-demo
  python3 scripts/rv_iv_weekly.py [START YYYY-MM-DD] [END YYYY-MM-DD]
  # 缺省自动取 data-cache 中 ETHUSDT trades 的全部日期范围
"""
from __future__ import annotations

import json
import sys
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

DIR = Path("data-cache/futures/um/daily/trades/ETHUSDT")
OUT_CSV = Path("scripts/rv_iv_weekly.csv")
OUT_PNG = Path("scripts/rv_iv_weekly.png")

YEAR_MS = 365 * 24 * 3600 * 1000  # 年化用日历年(与 rv.py 一致)
FREQS_MS = {"5min": 300_000, "1min": 60_000}
PRIMARY_FREQ = "5min"  # 主对比频率(微观结构噪声稳健)

BYBIT_HV_URL = "https://api.bybit.com/v5/market/historical-volatility"
IV_PERIOD = 7          # 7 日 IV,与"周"对齐
IV_WINDOW_MS = 28 * 24 * 3600 * 1000  # 单次请求窗口(< 30 天上限)


# ---------- 数据范围 ----------

def available_dates() -> list[str]:
    ds = sorted(p.name[len("ETHUSDT-trades-"):-len(".zip")] for p in DIR.glob("ETHUSDT-trades-*.zip"))
    return ds


def date_range(start: str, end: str, available: set[str]) -> list[str]:
    d, e = date.fromisoformat(start), date.fromisoformat(end)
    out = []
    while d <= e:
        s = d.isoformat()
        if s in available:
            out.append(s)
        d += timedelta(days=1)
    return out


# ---------- RV:逐日采样末价 ----------

def sampled_prices(dates: list[str]) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    """返回每个频率的 (bucket_start_ms[], last_price[]) 全局有序序列。"""
    chunks = {f: [] for f in FREQS_MS}  # freq -> list of (bucket_ts, price) arrays
    for i, d in enumerate(dates):
        zp = DIR / f"ETHUSDT-trades-{d}.zip"
        df = pd.read_csv(zp, compression="zip", usecols=["price", "time"],
                         dtype={"price": "float64", "time": "int64"})
        t = df["time"].to_numpy()
        p = df["price"].to_numpy()
        for f, ms in FREQS_MS.items():
            bucket = t // ms
            # 每个 bucket 取最后一条(数据按时间升序);np 端高效去重取末
            last_idx = np.nonzero(np.r_[bucket[1:] != bucket[:-1], True])[0]
            chunks[f].append((bucket[last_idx] * ms, p[last_idx]))
        if (i + 1) % 30 == 0 or i + 1 == len(dates):
            print(f"  read {i+1}/{len(dates)} days ({d})", flush=True)
    out = {}
    for f in FREQS_MS:
        ts = np.concatenate([c[0] for c in chunks[f]])
        pr = np.concatenate([c[1] for c in chunks[f]])
        # 防御性排序:各日内已升序、dates 已排序,正常本已有序;此处仅对采样点
        # (5min≈12万 / 1min≈62万,开销可忽略)兜底,防缺日/乱序导致收益错乱。
        order = np.argsort(ts, kind="stable")
        out[f] = (ts[order], pr[order])
    return out


def week_monday(year: int, week: int) -> str:
    return date.fromisocalendar(year, week, 1).isoformat()


def _add_iso_week(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """向量化地按 UTC 时间戳列加上 ISO (year, week) 两列。"""
    iso = pd.to_datetime(df[ts_col], unit="ms", utc=True).dt.isocalendar()
    df["year"] = iso["year"].to_numpy()
    df["week"] = iso["week"].to_numpy()
    return df


def weekly_rv(ts: np.ndarray, prices: np.ndarray) -> pd.DataFrame:
    """对采样价序列计算每个 ISO 周的年化 RV(%)。"""
    ret = np.diff(np.log(prices))
    df = pd.DataFrame({"sq": ret * ret, "ts": ts[1:]})  # 收益归属到其结束时刻所在的周
    _add_iso_week(df, "ts")
    rec = []
    for (y, w), g in df.groupby(["year", "week"]):
        span = int(g["ts"].max() - g["ts"].min())
        if span <= 0:
            continue
        ann_var = g["sq"].sum() * (YEAR_MS / span)
        rec.append({"year": int(y), "week": int(w), "week_start": week_monday(int(y), int(w)),
                    "rv_ann_pct": float(np.sqrt(ann_var)) * 100, "rv_n_ret": len(g),
                    "rv_span_days": span / 86_400_000})
    return pd.DataFrame(rec).sort_values(["year", "week"]).reset_index(drop=True)


# ---------- Bybit IV ----------

def fetch_bybit_iv(start_ms: int, end_ms: int) -> pd.DataFrame:
    """分页拉取 Bybit ETH 7 日 IV(小时级),返回 DataFrame[time_ms, iv]。"""
    seen: dict[int, float] = {}
    cur = start_ms
    while cur < end_ms:
        hi = min(cur + IV_WINDOW_MS, end_ms)
        params = {"category": "option", "baseCoin": "ETH", "quoteCoin": "USDT",
                  "period": IV_PERIOD, "startTime": cur, "endTime": hi}
        url = BYBIT_HV_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        for attempt in range(3):
            try:
                r = json.load(urllib.request.urlopen(url, timeout=30))
                break
            except (urllib.error.URLError, TimeoutError) as e:
                if attempt == 2:
                    raise
                print(f"  retry bybit ({e})", flush=True)
        if r.get("retCode") != 0:
            raise RuntimeError(f"bybit error: {r.get('retMsg')}")
        for x in r.get("result", []):
            seen[int(x["time"])] = float(x["value"])
        cur = hi
    df = pd.DataFrame(sorted(seen.items()), columns=["time_ms", "iv"])
    print(f"  bybit IV points: {len(df)}", flush=True)
    return df


def weekly_iv(iv: pd.DataFrame) -> pd.DataFrame:
    iv = _add_iso_week(iv.copy(), "time_ms")
    rec = []
    for (y, w), g in iv.groupby(["year", "week"]):
        rec.append({"year": int(y), "week": int(w),
                    "iv_ann_pct": g["iv"].mean() * 100, "iv_n": len(g)})
    return pd.DataFrame(rec)


# ---------- 主流程 ----------

def main() -> int:
    avail = available_dates()
    if not avail:
        print("no ETHUSDT trades data found", file=sys.stderr)
        return 1
    if len(sys.argv) == 2:
        print("用法:rv_iv_weekly.py [START END];需成对传入,或不传(默认全量范围)",
              file=sys.stderr)
        return 2
    start = sys.argv[1] if len(sys.argv) >= 3 else avail[0]
    end = sys.argv[2] if len(sys.argv) >= 3 else avail[-1]
    dates = date_range(start, end, set(avail))
    print(f"数据范围 {dates[0]}..{dates[-1]}  天数={len(dates)}")

    print("[1/3] 读取成交、采样末价 ...")
    series = sampled_prices(dates)

    print("[2/3] 计算各频率周度 RV ...")
    rv_tables = {f: weekly_rv(*series[f]) for f in FREQS_MS}
    rv = rv_tables[PRIMARY_FREQ].rename(columns={"rv_ann_pct": f"rv_{PRIMARY_FREQ}_pct"})
    for f in FREQS_MS:
        if f == PRIMARY_FREQ:
            continue
        rv = rv.merge(rv_tables[f][["year", "week", "rv_ann_pct"]]
                      .rename(columns={"rv_ann_pct": f"rv_{f}_pct"}), on=["year", "week"], how="left")

    print("[3/3] 下载 Bybit IV 并对齐 ...")
    start_ms = int(datetime.fromisoformat(dates[0]).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int((datetime.fromisoformat(dates[-1]).replace(tzinfo=timezone.utc) + timedelta(days=1)).timestamp() * 1000)
    iv_hourly = fetch_bybit_iv(start_ms, end_ms)
    iv_w = weekly_iv(iv_hourly)

    out = rv.merge(iv_w, on=["year", "week"], how="left")
    out["spread_iv_minus_rv"] = out["iv_ann_pct"] - out[f"rv_{PRIMARY_FREQ}_pct"]
    out = out.sort_values(["year", "week"]).reset_index(drop=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False, float_format="%.3f")
    print(f"\n写出 {OUT_CSV}  ({len(out)} 周)")

    # 汇总
    rvp, ivp = out[f"rv_{PRIMARY_FREQ}_pct"], out["iv_ann_pct"]
    both = out.dropna(subset=[f"rv_{PRIMARY_FREQ}_pct", "iv_ann_pct"])
    print(f"\n=== 汇总({PRIMARY_FREQ} RV vs 7d IV,年化%)===")
    print(f"周数: RV={rvp.notna().sum()}  IV={ivp.notna().sum()}  重叠={len(both)}")
    print(f"RV 均值={rvp.mean():.1f}%  IV 均值={ivp.mean():.1f}%")
    if len(both) > 1:
        print(f"IV-RV 价差均值={both['spread_iv_minus_rv'].mean():+.1f}pp  "
              f"(IV>RV 周占比={100*(both['spread_iv_minus_rv']>0).mean():.0f}%)")
        print(f"相关系数 corr(RV,IV)={both[f'rv_{PRIMARY_FREQ}_pct'].corr(both['iv_ann_pct']):.3f}")
        print("注:IV=周均滚动7d,RV=周内积分,口径不同区间;* 为残缺周(span<6.5d,年化放大)")

    missing_iv = out[out["iv_ann_pct"].isna()]
    if len(missing_iv):
        print(f"缺 IV 的周 {len(missing_iv)}: {list(missing_iv['week_start'])}")
    partial = out[out["rv_span_days"] < 6.5]
    if len(partial):
        print(f"残缺周 {len(partial)}(span<6.5d): {list(partial['week_start'])}")

    print(f"\n{'week_start':>12} {'RV%':>7} {'IV%':>7} {'IV-RV':>7}  flag")
    for _, r in out.iterrows():
        iv = f"{r['iv_ann_pct']:.1f}" if pd.notna(r["iv_ann_pct"]) else "-"
        sp = f"{r['spread_iv_minus_rv']:+.1f}" if pd.notna(r["spread_iv_minus_rv"]) else "-"
        flag = "*" if r["rv_span_days"] < 6.5 else ""
        print(f"{r['week_start']:>12} {r[f'rv_{PRIMARY_FREQ}_pct']:>7.1f} {iv:>7} {sp:>7}  {flag}")

    _plot(out)
    return 0


def _plot(out: pd.DataFrame) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    x = pd.to_datetime(out["week_start"])
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7), height_ratios=[3, 1], sharex=True)
    ax1.plot(x, out[f"rv_{PRIMARY_FREQ}_pct"], label=f"RV {PRIMARY_FREQ} (annualized)", color="#1f77b4")
    ax1.plot(x, out["iv_ann_pct"], label="Bybit IV 7d (annualized)", color="#d62728")
    ax1.set_ylabel("annualized vol %")
    ax1.set_title("ETHUSDT weekly Realized Vol vs Bybit Implied Vol")
    ax1.legend(); ax1.grid(alpha=0.3)
    ax2.bar(x, out["spread_iv_minus_rv"], width=5, color="#2ca02c", alpha=0.6)
    ax2.axhline(0, color="k", lw=0.8)
    ax2.set_ylabel("IV - RV (pp)"); ax2.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT_PNG, dpi=110)
    print(f"写出图 {OUT_PNG}")


if __name__ == "__main__":
    raise SystemExit(main())
