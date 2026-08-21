#!/usr/bin/env python3
"""对冲腿 edge 检验 —— 自包含 HTML artifact。

三个配置 (均: ETHUSDT 周度卖方 short-vol, 不重叠 1 周 tranche, 常数 1× 仓位, maker 挂单 BBO 外
0.01%·5s 重挂, 手续费=0):
  - sym (基准) : 对称 1ATR delta 对冲 (无方向) · iv=rv -> **理论应盈亏平衡**, 校验回测口径;
  - rv  (核心) : MA20 不对称带 (价在 MA20 上→上带紧/下带松) · iv=rv -> 隔离方向带的对冲腿 edge;
  - lag (现实) : MA20 不对称带 · iv=上周RV -> 可交易基准 (含 VRP)。
matplotlib 图以 base64 PNG 内联, 无 CDN, 离线可开。
用法: python3 scripts/ivrv_edge_artifact.py [out.html]
"""
import base64
import csv
import io
import sys
from datetime import date

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

for _f in ["PingFang SC", "Heiti SC", "Arial Unicode MS", "STHeiti"]:
    plt.rcParams["font.sans-serif"] = [_f]
plt.rcParams["axes.unicode_minus"] = False

OUT = sys.argv[1] if len(sys.argv) > 1 else "scripts/ivrv_edge.html"
# key -> (csv, 标签, 颜色)
RUNS = {
    "sym": ("/tmp/sym_tranches.csv",  "对称 1ATR · 无方向 · iv=rv (基准)",        "#0891b2"),
    "rv":  ("/tmp/ivrv_tranches.csv", "MA20 不对称 · iv=rv (隔离方向 edge)",       "#2563eb"),
    "lag": ("/tmp/lag_tranches.csv",  "MA20 不对称 · iv=上周RV (可交易·含VRP)",   "#9333ea"),
}


def load(path):
    rows = list(csv.DictReader(open(path)))
    for r in rows:
        for k in ("premium", "optionPnl", "hedgePnl", "total", "rvThisWeek", "ivUsed"):
            r[k] = float(r[k])
        r["end"] = date.fromisoformat(r["weekEnd"])
    rows.sort(key=lambda r: r["end"])
    return rows


def stats(rows):
    n = len(rows)
    tot = sum(r["total"] for r in rows)
    prem = sum(abs(r["premium"]) for r in rows)
    opt = sum(r["optionPnl"] for r in rows)
    hed = sum(r["hedgePnl"] for r in rows)
    wins = sum(1 for r in rows if r["total"] > 0)
    cum = peak = mdd = 0.0
    for r in rows:
        cum += r["total"]; peak = max(peak, cum); mdd = max(mdd, peak - cum)
    span = (rows[-1]["end"] - rows[0]["end"]).days / 365.25 if n > 1 else 1.0
    return dict(n=n, tot=tot, prem=prem, opt=opt, hed=hed, wins=wins, mdd=mdd,
                mean=tot / n if n else 0, pct=100 * tot / prem if prem else 0, span=span,
                worst=min((r["total"] for r in rows), default=0),
                best=max((r["total"] for r in rows), default=0))


def png(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def cum_curve(data):
    fig, ax = plt.subplots(figsize=(11, 4.3))
    for key in ("sym", "rv", "lag"):
        if key not in data:
            continue
        _, label, color = RUNS[key]
        c = 0.0; xs = []; ys = []
        for r in data[key]:
            c += r["total"]; xs.append(r["end"]); ys.append(c)
        ax.plot(xs, ys, color=color, lw=1.9, marker="o", ms=2.4, label=label)
    ax.axhline(0, color="grey", lw=1.0, ls="--")
    ax.set_title("累计已实现 PnL (按到期日, 常数 1× 仓位, 手续费=0)")
    ax.set_ylabel("累计 PnL (USDT)"); ax.legend(fontsize=8.5); ax.grid(alpha=0.25)
    for l in ax.get_xticklabels():
        l.set_rotation(25); l.set_ha("right")
    return png(fig)


def tranche_bars(rows, label):
    fig, ax = plt.subplots(figsize=(11, 3.2))
    xs = [r["end"] for r in rows]; ys = [r["total"] for r in rows]
    ax.bar(xs, ys, width=5, color=["#16a34a" if v >= 0 else "#dc2626" for v in ys], alpha=0.85)
    ax.axhline(0, color="grey", lw=0.8)
    ax.set_title(f"单 tranche PnL · {label}"); ax.set_ylabel("PnL"); ax.grid(alpha=0.25)
    for l in ax.get_xticklabels():
        l.set_rotation(25); l.set_ha("right")
    return png(fig)


data = {}
for key, (path, _l, _c) in RUNS.items():
    try:
        data[key] = load(path)
    except FileNotFoundError:
        print(f"[warn] {path} 不存在, 跳过 {key}")

if "sym" not in data or "rv" not in data:
    sys.exit("缺少 sym 基准或 rv 核心结果, 无法生成 artifact")

st = {k: stats(v) for k, v in data.items()}
sym, rv = st["sym"], st["rv"]
lag = st.get("lag")

# sanity: 对称基准是否近似盈亏平衡 (|净/权利金| < 1%)
sane = abs(sym["pct"]) < 1.0
sane_color = "#16a34a" if sane else "#dc2626"
# 方向贡献 = MA20(iv=rv) − 对称基准(iv=rv): >0 方向带加分, <0 方向带拖累
dir_contrib = rv["tot"] - sym["tot"]
vrp = (lag["tot"] - rv["tot"]) if lag else None

img_cum = cum_curve(data)
img_bars = tranche_bars(data["sym"], RUNS["sym"][1])

def cards(s, color):
    items = [
        ("Σ 总盈亏", f"{s['tot']:+,.0f}", "USDT"),
        ("占权利金", f"{s['pct']:+.2f}%", "Σ净/Σ权利金"),
        ("胜率", f"{s['wins']}/{s['n']}", f"{100*s['wins']/s['n']:.0f}%"),
        ("均值/tranche", f"{s['mean']:+,.1f}", "USDT"),
        ("最大回撤", f"{s['mdd']:,.0f}", "USDT"),
        ("最差/最好", f"{s['worst']:+,.0f}/{s['best']:+,.0f}", "USDT"),
        ("期权腿", f"{s['opt']:+,.0f}", "Σ (与对冲规则无关)"),
        ("对冲腿", f"{s['hed']:+,.0f}", "Σ (规则差异全在此)"),
    ]
    h = "".join(
        f'<div class="card"><div class="ck">{k}</div><div class="cv" style="color:{color}">{v}</div><div class="cu">{u}</div></div>'
        for k, v, u in items)
    return h

sections = [
    f"""<h2>① 对称 1ATR 基准 (iv=rv) <span class="badge" style="background:{sane_color}">{'盈亏平衡 ✓' if sane else '偏离 0 ✗'}</span></h2>
<div class="sub2">无方向的纯 delta 阈值对冲, 公平定价(iv=rv)下期望应≈0 —— 校验回测口径。</div>
<div class="cards">{cards(sym, RUNS['sym'][2])}</div>""",
    f"""<h2>② MA20 不对称带 (iv=rv) <span class="badge" style="background:{'#16a34a' if rv['tot']>0 else '#dc2626'}">{'有正 edge' if rv['tot']>0 else '无正 edge'}</span></h2>
<div class="sub2">价在 MA20 上→上带紧 1ATR/下带松 2ATR, 下方镜像。相对基准的差额 = 方向带的对冲腿 edge。</div>
<div class="cards">{cards(rv, RUNS['rv'][2])}</div>""",
]
if lag:
    sections.append(
        f"""<h2>③ MA20 不对称带 (iv=上周RV, 可交易)</h2>
<div class="sub2">现实可交易口径, 卖出 IV 含波动风险溢价 (VRP)。</div>
<div class="cards">{cards(lag, RUNS['lag'][2])}</div>""")

vrp_row = (f"<tr><td>VRP 贡献 (lag − rv)</td><td>{vrp:+,.0f}</td><td>—</td>"
           f"<td>含 VRP 的口径比 iv=rv 多赚的部分 ≈ 卖在「贵于实际」的波动上</td></tr>") if lag else ""
lag_row = (f"<tr><td>③ MA20 · iv=上周RV</td><td>{lag['tot']:+,.0f}</td><td>{lag['pct']:+.2f}%</td>"
           f"<td>可交易基准 (含 VRP)</td></tr>") if lag else ""

# 结论文字
if not sane:
    head = (f"<b>⚠️ 基准未通过</b>: 对称 1ATR @ iv=rv 的 Σ={sym['tot']:+,.0f} ({sym['pct']:+.2f}%) 明显偏离 0, "
            f"说明回测口径有系统性偏差 (检查 maker 挂价改善/重挂/recenter), 下面的方向 edge 结论需谨慎。")
else:
    head = (f"<b>✓ 基准通过</b>: 对称 1ATR @ iv=rv 的 Σ={sym['tot']:+,.0f} ({sym['pct']:+.2f}%) 近似盈亏平衡, "
            f"证明回测口径正确 —— 公平定价下纯 delta 对冲不赚不赔。")
dir_word = "增厚" if dir_contrib > 0 else "拖累"
dir_msg = (f"MA20 方向带在 iv=rv 下 Σ={rv['tot']:+,.0f}, 相对对称基准{dir_word} <b>{dir_contrib:+,.0f}</b>。"
           + ("方向择时在公平定价下也能小幅增厚, 是真 alpha。" if dir_contrib > 0 else
              "即方向不对称在公平定价下反而<b>负贡献</b> —— 顺势紧/逆势松的择时在这些路径上没占到便宜, 离散追价是净成本。"))
vrp_msg = (f"<br>可交易口径 (iv=上周RV) Σ={lag['tot']:+,.0f} ({lag['pct']:+.2f}%), 其中 VRP 贡献≈<b>{vrp:+,.0f}</b> —— "
           f"现实盈利几乎全部来自卖贵波动, 而非对冲规则。" if lag else "")

html = f"""<!DOCTYPE html><html lang="zh"><head><meta charset="utf-8"/>
<title>对冲腿 edge 检验 · 对称基准 vs MA20</title>
<style>
 body{{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;color:#111827;background:#f9fafb}}
 .wrap{{max-width:1040px;margin:0 auto;padding:28px 22px 60px}}
 h1{{font-size:22px;margin:0 0 6px}}
 h2{{font-size:16px;margin:24px 0 4px;display:flex;align-items:center;gap:10px}}
 .sub{{color:#6b7280;font-size:13px;line-height:1.6}}
 .sub2{{color:#6b7280;font-size:12.5px;margin:0 0 10px}}
 .badge{{color:#fff;font-size:12px;font-weight:600;padding:3px 10px;border-radius:999px}}
 .note{{background:#fffbeb;border:1px solid #fde68a;border-radius:10px;padding:11px 15px;font-size:12.5px;color:#92400e;margin:14px 0}}
 .cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:9px}}
 .card{{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:11px 13px}}
 .ck{{color:#6b7280;font-size:12px}} .cv{{font-size:19px;font-weight:700;margin:2px 0}} .cu{{color:#9ca3af;font-size:11px}}
 img{{width:100%;border:1px solid #e5e7eb;border-radius:10px;background:#fff;margin:8px 0}}
 table{{width:100%;border-collapse:collapse;font-size:13px;margin-top:10px;background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden}}
 td,th{{padding:9px 12px;border-bottom:1px solid #f1f5f9;text-align:left}} th{{background:#f8fafc;color:#374151}}
 .verdict{{background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:16px 18px;margin:20px 0;font-size:14px;line-height:1.8}}
 @media(max-width:760px){{.cards{{grid-template-columns:repeat(2,1fr)}}}}
</style></head><body><div class="wrap">
<h1>对冲腿本身有没有 edge?(对称基准 vs MA20 方向带)</h1>
<div class="sub">ETHUSDT 卖方 short-vol · {rv['n']} 个不重叠 1 周 tranche (≈{rv['span']:.2f} 年) · 常数 1× 仓位 ·
maker 挂单(BBO 外 0.01%, 5s 重挂)· <b>手续费=0</b>。三配置只差"对冲带形状"与"IV 口径", 期权腿 PnL
只取决于价路径/IV/tenor(与对冲规则无关), 故规则差异<b>全部</b>体现在对冲腿。</div>
<div class="note">⚠️ iv=rv 用期权存续期的<u>实际</u> RV 定价(完美预知), <b>不可实盘交易</b>; 仅用于隔离对冲腿 edge。对称基准用于校验口径。</div>

{''.join(sections)}

<h2>累计净值 & 基准单 tranche</h2>
<img src="data:image/png;base64,{img_cum}"/>
<img src="data:image/png;base64,{img_bars}"/>

<table>
<tr><th>配置</th><th>Σ总盈亏</th><th>占权利金</th><th>说明</th></tr>
<tr><td>① 对称1ATR · iv=rv (基准)</td><td>{sym['tot']:+,.0f}</td><td>{sym['pct']:+.2f}%</td><td>公平定价纯 delta 对冲, 应≈0</td></tr>
<tr><td>② MA20 · iv=rv (核心)</td><td>{rv['tot']:+,.0f}</td><td>{rv['pct']:+.2f}%</td><td>方向带的对冲腿 edge</td></tr>
{lag_row}
<tr><td>方向贡献 (rv − sym)</td><td>{dir_contrib:+,.0f}</td><td>—</td><td>MA20 不对称相对对称基准的增减</td></tr>
{vrp_row}
</table>

<div class="verdict">
{head}<br>{dir_msg}{vrp_msg}
</div>
</div></body></html>"""

open(OUT, "w").write(html)
print(f"saved {OUT}")
for k in ("sym", "rv", "lag"):
    if k in st:
        s = st[k]
        print(f"  {k:4s}: Σtotal={s['tot']:+.0f}  pct={s['pct']:+.2f}%  win={s['wins']}/{s['n']}  opt={s['opt']:+.0f} hed={s['hed']:+.0f}")
print(f"  方向贡献(rv-sym)={dir_contrib:+.0f}" + (f"  VRP(lag-rv)={vrp:+.0f}" if vrp is not None else ""))
