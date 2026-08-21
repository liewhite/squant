package strategy.utils.viz

import java.nio.file.{Files, Path}
import java.time.{Instant, ZoneOffset}

/** 极简**自包含**净值曲线网页生成器 (内联 SVG, 无外部依赖/无 CDN, 离线可开)。
  *
  * 给定时间轴 + 若干条归一化净值曲线 (+ 可选的逐采样仓位方向带 + 摘要面板), 渲染成一张
  * 带网格/坐标/图例的折线图 HTML。供回测产出"净值曲线"工件直接在浏览器打开查看。 */
object EquityChartHtml:
  /** 一条曲线：名称、颜色 (CSS hex)、与时间轴等长的数值序列。 */
  final case class Line(name: String, color: String, values: Seq[Double])

  /** 价格/净值线上的成交标记：时间、买卖向 (true=买/绿↑、false=卖/红↓)、归一化纵值
    * (须与所标曲线同一归一化基准, 例如标在 buy&hold 价格线上即用 成交价/起点价)。 */
  final case class Marker(ts: Long, buy: Boolean, value: Double)

  /** HTML/SVG 文本转义 (防参数里的 & < > " 破坏 DOM / 注入)。 */
  private def esc(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  private val W = 1180.0
  private val H = 560.0
  private val (mL, mR, mT, mB) = (70.0, 24.0, 56.0, 96.0) // 边距 (底部留仓位带 + x 轴)
  private val plotW = W - mL - mR
  private val plotH = H - mT - mB
  private val bandH = 16.0 // 仓位方向带高度

  def write(
      path: String,
      title: String,
      ts: Seq[Long],
      lines: Seq[Line],
      posDir: Seq[Int],
      stats: Seq[(String, String)],
      markers: Seq[Marker] = Nil,
  ): Unit =
    val p = Path.of(path)
    Option(p.getParent).foreach(Files.createDirectories(_))
    Files.writeString(p, render(title, ts, lines, posDir, stats, markers))

  def render(
      title: String,
      ts: Seq[Long],
      lines: Seq[Line],
      posDir: Seq[Int],
      stats: Seq[(String, String)],
      markers: Seq[Marker] = Nil,
  ): String =
    if ts.sizeIs < 2 || lines.isEmpty then emptyDoc(title)
    else
      val tsMin = ts.head.toDouble; val tsMax = ts.last.toDouble
      val tsSpan = math.max(1.0, tsMax - tsMin)
      val allVals = lines.flatMap(_.values)
      val vMin0 = allVals.min; val vMax0 = allVals.max
      val pad = math.max(1e-9, (vMax0 - vMin0) * 0.08)
      val vMin = vMin0 - pad; val vMax = vMax0 + pad
      val vSpan = math.max(1e-9, vMax - vMin)

      def xOf(t: Long): Double = mL + (t.toDouble - tsMin) / tsSpan * plotW
      def yOf(v: Double): Double = mT + (vMax - v) / vSpan * plotH

      val sb = new StringBuilder
      // y 网格 + 标签 (5 档)
      val yTicks = 5
      for i <- 0 to yTicks do
        val v = vMin + vSpan * i / yTicks
        val y = yOf(v)
        sb.append(f"""<line x1="$mL%.1f" y1="$y%.1f" x2="${mL + plotW}%.1f" y2="$y%.1f" class="grid"/>""")
        sb.append(f"""<text x="${mL - 8}%.1f" y="${y + 4}%.1f" class="ylab">${v}%.3f</text>""")
      // x 网格 + 日期标签 (~7 档)
      val xTicks = 7
      for i <- 0 to xTicks do
        val t = (tsMin + tsSpan * i / xTicks).toLong
        val x = xOf(t)
        val d = Instant.ofEpochMilli(t).atZone(ZoneOffset.UTC).toLocalDate
        sb.append(f"""<line x1="$x%.1f" y1="$mT%.1f" x2="$x%.1f" y2="${mT + plotH}%.1f" class="grid"/>""")
        sb.append(f"""<text x="$x%.1f" y="${mT + plotH + 18}%.1f" class="xlab">$d</text>""")
      // 净值=1 基准线 (若在范围内)
      if vMin <= 1.0 && 1.0 <= vMax then
        sb.append(f"""<line x1="$mL%.1f" y1="${yOf(1.0)}%.1f" x2="${mL + plotW}%.1f" y2="${yOf(1.0)}%.1f" class="base"/>""")
      // 曲线
      lines.foreach { ln =>
        val pts = ts.zip(ln.values).map { case (t, v) => f"${xOf(t)}%.1f,${yOf(v)}%.1f" }.mkString(" ")
        sb.append(s"""<polyline points="$pts" fill="none" stroke="${ln.color}" stroke-width="1.8"/>""")
      }
      // 成交标记 (画在对应曲线上: 买=绿↑ 卖=红↓), 纵值越界则贴边
      markers.foreach { m =>
        val x = xOf(m.ts)
        val y = math.max(mT, math.min(mT + plotH, yOf(m.value)))
        val (pts, c) =
          if m.buy then (f"$x%.1f,${y - 6}%.1f ${x - 4.5}%.1f,${y + 3}%.1f ${x + 4.5}%.1f,${y + 3}%.1f", "#16a34a")
          else (f"$x%.1f,${y + 6}%.1f ${x - 4.5}%.1f,${y - 3}%.1f ${x + 4.5}%.1f,${y - 3}%.1f", "#dc2626")
        sb.append(s"""<polygon points="$pts" fill="$c" stroke="#fff" stroke-width="0.6" opacity="0.92"/>""")
      }
      // 仓位方向带 (绿=多 红=空 灰=平), 每相邻采样一段, 颜色取左端
      val bandY = mT + plotH + 30
      if posDir.sizeIs == ts.size then
        ts.indices.dropRight(1).foreach { i =>
          val x1 = xOf(ts(i)); val x2 = xOf(ts(i + 1))
          val c = posDir(i) match { case d if d > 0 => "#16a34a"; case d if d < 0 => "#dc2626"; case _ => "#d1d5db" }
          sb.append(f"""<rect x="$x1%.1f" y="$bandY%.1f" width="${math.max(0.5, x2 - x1)}%.1f" height="$bandH%.1f" fill="$c"/>""")
        }
        sb.append(f"""<text x="$mL%.1f" y="${bandY + bandH + 14}%.1f" class="xlab">仓位: <tspan fill="#16a34a">■多</tspan> <tspan fill="#dc2626">■空</tspan> <tspan fill="#9ca3af">■平</tspan></text>""")
      // 图例
      val legend = lines.zipWithIndex.map { case (ln, i) =>
        val lx = mL + 8 + i * 180
        f"""<rect x="$lx%.1f" y="${mT - 30}%.1f" width="14" height="14" fill="${ln.color}"/><text x="${lx + 20}%.1f" y="${mT - 18}%.1f" class="leg">${esc(ln.name)}</text>"""
      }.mkString

      val statRows = stats.map { case (k, v) => s"""<div class="stat"><span class="k">${esc(k)}</span><span class="v">${esc(v)}</span></div>""" }.mkString
      s"""<!DOCTYPE html><html lang="zh"><head><meta charset="utf-8"/>
<title>${esc(title)}</title>
<style>
 body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px;color:#1f2937;background:#fff}
 h1{font-size:18px;margin:0 0 4px}
 .sub{color:#6b7280;font-size:13px;margin-bottom:16px}
 .stats{display:flex;flex-wrap:wrap;gap:10px;margin:0 0 14px}
 .stat{background:#f3f4f6;border-radius:8px;padding:8px 12px;font-size:13px}
 .stat .k{color:#6b7280;margin-right:8px}
 .stat .v{font-weight:600}
 svg{border:1px solid #e5e7eb;border-radius:10px;background:#fff}
 .grid{stroke:#eef0f3;stroke-width:1}
 .base{stroke:#9ca3af;stroke-width:1;stroke-dasharray:4 4}
 .ylab{fill:#6b7280;font-size:11px;text-anchor:end}
 .xlab{fill:#6b7280;font-size:11px;text-anchor:middle}
 .leg{fill:#374151;font-size:13px}
</style></head>
<body>
<h1>${esc(title)}</h1>
<div class="sub">净值已归一化到起点=1.000；虚线为基准 1.0。横轴 UTC 日期。${
        if markers.nonEmpty then """ 成交点标在价格线上: <span style="color:#16a34a">▲买</span> / <span style="color:#dc2626">▼卖</span>。""" else ""
      }</div>
<div class="stats">$statRows</div>
<svg viewBox="0 0 ${W.toInt} ${H.toInt}" width="100%" preserveAspectRatio="xMidYMid meet">$legend${sb.toString}</svg>
</body></html>"""

  private def emptyDoc(title: String): String =
    s"""<!DOCTYPE html><html><head><meta charset="utf-8"/><title>${esc(title)}</title></head>
<body style="font-family:sans-serif"><h1>${esc(title)}</h1><p>数据点不足, 无法绘制净值曲线。</p></body></html>"""
