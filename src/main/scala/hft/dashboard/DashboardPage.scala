package hft.dashboard

/** 看板页面 —— 一个自包含的 HTML 字符串。
  *
  * **不引任何 CDN**: 这东西要跑在交易机上, 而交易机可能没有外网、或者外网正好是出问题的
  * 那一环。页面依赖一个第三方域名意味着"行情断了想看看板"的时候看板自己也打不开。
  *
  * 同理不做前端构建: 一个几百行的 HTML 字符串, 换来的是"编译出来就能跑, 没有第二套工具链"。
  * 页面复杂到需要构建工具的那天再说 —— 那时它也该是另一个仓库。
  */
object DashboardPage:
  val html: String =
    """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>squant 看板</title>
<style>
  :root {
    --bg:#0f1115; --panel:#171a21; --line:#252a33; --fg:#d8dee9; --dim:#7b8494;
    --long:#3fb950; --short:#f85149; --warn:#d29922; --stale:#6e7681;
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg);
         font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }
  header { display:flex; align-items:baseline; gap:16px; flex-wrap:wrap;
           padding:10px 16px; border-bottom:1px solid var(--line); position:sticky; top:0; background:var(--bg); }
  h1 { font-size:14px; margin:0; font-weight:600; letter-spacing:.5px; }
  .meta { color:var(--dim); font-size:12px; }
  .bad { color:var(--short); }
  .warn { color:var(--warn); }
  section { padding:14px 16px; }
  h2 { font-size:12px; color:var(--dim); text-transform:uppercase; letter-spacing:1px;
       margin:0 0 8px; font-weight:600; }
  table { border-collapse:collapse; width:100%; }
  th { text-align:left; font-weight:600; color:var(--dim); font-size:11px;
       text-transform:uppercase; letter-spacing:.5px; padding:6px 10px; border-bottom:1px solid var(--line); }
  td { padding:6px 10px; border-bottom:1px solid var(--panel); vertical-align:top; white-space:nowrap; }
  tr:hover td { background:var(--panel); }
  .num { text-align:right; font-variant-numeric:tabular-nums; }
  .long { color:var(--long); }
  .short { color:var(--short); }
  .none { color:var(--stale); }
  .age { color:var(--dim); font-size:11px; }
  .age.old { color:var(--warn); }
  .age.dead { color:var(--short); font-weight:600; }
  .sub { color:var(--dim); font-size:11px; }
  .empty { color:var(--dim); padding:20px 0; }
  .pill { display:inline-block; padding:0 6px; border-radius:3px; background:var(--panel);
          border:1px solid var(--line); font-size:11px; margin-right:4px; }
</style>
</head>
<body>
<header>
  <h1>squant 看板</h1>
  <span class="meta" id="status">连接中…</span>
  <span class="meta" id="heartbeat"></span>
  <span class="meta" id="events"></span>
</header>
<section>
  <h2>标的</h2>
  <div id="symbols"></div>
</section>
<section>
  <h2>账户</h2>
  <div id="accounts"></div>
</section>

<script>
// 陈旧阈值只对**流式读数**成立: 超过 5s 变黄、超过 30s 标红。
// 一个 4 分钟前的盘口和一个刚到的盘口长得一样, 就是把"不知道"显示成了正常值。
const WARN_MS = 5000, DEAD_MS = 30000;

// **年龄对两类读数的含义不同, 不能共用一套阈值。**
//
//   - 流式读数 (盘口/标记价/最近成交价): 交易所在持续推, 所以年龄 = 陈旧度。停了就是断流。
//   - 变更驱动读数 (仓位/挂单/最近成交/净值/余额): 只在**发生变化**时才推。仓位由柜台在成交
//     入账与启动对齐时发, 净值按 accountRefreshMs (默认 10s) 周期发。它们的年龄是
//     "距上次变化多久", 不是"数据死了"。
//
// 从前两类共用 5s/30s: 真实柜台下**每个不动的仓位 30s 后必然标红**、净值每个刷新周期的后 5 秒
// 变黄。满屏假红会很快被当成噪声忽略, 于是真断流的时候反而看不出来 —— 告警的价值就是这么废掉的。
// 变更驱动读数只显示年龄、不上色; "数据流还活不活着"交给顶部心跳与流式读数去回答。

const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const num = (v, d = 2) => v === null || v === undefined ? '' : Number(v).toFixed(d);

function fmtAge(ms) {
  return ms < 1000 ? ms + 'ms' : ms < 60000 ? (ms / 1000).toFixed(1) + 's' : Math.floor(ms / 60000) + 'm';
}

/** 流式读数的年龄: 按阈值上色 (年龄 = 陈旧度)。 */
function ageCell(ms) {
  if (ms === null || ms === undefined) return '<span class="none">—</span>';
  const cls = ms > DEAD_MS ? 'dead' : ms > WARN_MS ? 'old' : '';
  return '<span class="age ' + cls + '">' + fmtAge(ms) + '</span>';
}

/** 变更驱动读数的年龄: 只显示, 不上色 (年龄 = 距上次变化多久, 不动是常态)。 */
function sinceCell(ms) {
  if (ms === null || ms === undefined) return '<span class="none">—</span>';
  return '<span class="age">' + fmtAge(ms) + '</span>';
}

// 没有读数时显示 —— 而不是 0。这两者在交易上完全不是一回事。
const dash = '<span class="none">—</span>';

function reading(r, fmt) { return r ? fmt(r.value) + ' ' + ageCell(r.ageMs) : dash; }

function sideCls(v) { return v > 0 ? 'long' : v < 0 ? 'short' : 'none'; }

function symbolsTable(rows) {
  if (!rows.length) return '<div class="empty">还没有任何标的的数据。行情源连上了吗？</div>';
  let h = '<table><tr><th>交易所</th><th>标的</th><th class="num">买一</th><th class="num">卖一</th>'
        + '<th class="num">中价</th><th class="num">价差</th><th class="num">标记价</th>'
        + '<th class="num">资金费</th><th>账户</th></tr>';
  for (const r of rows) {
    const b = r.bbo;
    h += '<tr>'
      + '<td>' + esc(r.exchange) + '</td><td>' + esc(r.symbol) + '</td>'
      + '<td class="num">' + (b ? num(b.value.bid) : dash) + '</td>'
      + '<td class="num">' + (b ? num(b.value.ask) : dash) + '</td>'
      + '<td class="num">' + (b ? num(b.value.mid) + ' ' + ageCell(b.ageMs) : dash) + '</td>'
      + '<td class="num">' + (b ? num(b.value.spread, 4) : dash) + '</td>'
      + '<td class="num">' + reading(r.markPrice, v => num(v)) + '</td>'
      + '<td class="num">' + (r.funding ? (r.funding.value.rate * 100).toFixed(4) + '%' : dash) + '</td>'
      + '<td>' + accountsCell(r.accounts) + '</td>'
      + '</tr>';
  }
  return h + '</table>';
}

function accountsCell(accs) {
  if (!accs || !accs.length) return dash;
  return accs.map(a => {
    // 仓位是变更驱动的: 柜台只在成交入账与启动对齐时推。一个不动的仓位年龄会一直涨,
    // 那是正常的, 不该染成红色。
    const pos = a.position
      ? '<span class="' + sideCls(a.position.value) + '">' + num(a.position.value, 4) + '</span> ' + sinceCell(a.position.ageMs)
      : dash;
    let s = '<div><span class="pill">' + esc(a.account) + '</span>仓位 ' + pos;
    if (a.pendingOrders.length) {
      s += '<div class="sub">挂单 ' + a.pendingOrders.map(o =>
        esc(o.side) + ' ' + num(o.quantity, 4) + '@' + num(o.price) +
        (o.reduceOnly ? ' <span class="pill">RO</span>' : '') +
        ' <span class="sub">' + esc(o.status) + '</span>').join(' · ') + '</div>';
    }
    if (a.lastFill) {
      s += '<div class="sub">最近成交 ' + esc(a.lastFill.side) + ' ' + num(a.lastFill.size, 4)
         + '@' + num(a.lastFill.price) + ' ' + sinceCell(a.lastFill.ageMs) + '</div>';
    }
    return s + '</div>';
  }).join('');
}

function accountsTable(rows) {
  // 不写成"柜台连上了吗？": 纯监控进程 (如 crossspread) 本来就不装柜台, 那句反问会把
  // 一个正常状态说成故障。只陈述事实, 两种可能都列出来。
  if (!rows.length) return '<div class="empty">还没有任何账户读数 —— 本进程可能没装柜台（纯监控），也可能柜台还没连上。</div>';
  let h = '<table><tr><th>账户</th><th>交易所</th><th class="num">净值</th><th>余额</th></tr>';
  for (const r of rows) {
    // walletKnown=false 时, "某币不在下面的表里"不代表余额是 0 —— 那份全量只来自启动对齐的
    // 一次 REST 钱包查询, 没到之前不能把缺失读成 0。
    const bal = r.balances.length
      ? r.balances.map(b => '<span class="pill">' + esc(b.currency) + ' ' + num(b.amount, 4) + '</span>').join('')
      : dash;
    const note = r.walletKnown ? '' : ' <span class="warn sub">(未收到全量钱包快照, 未列出 ≠ 0)</span>';
    h += '<tr><td>' + esc(r.account) + '</td><td>' + esc(r.exchange) + '</td>'
      + '<td class="num">' + (r.equity ? num(r.equity.value) + ' ' + sinceCell(r.equity.ageMs) : dash) + '</td>'
      + '<td>' + bal + note + '</td></tr>';
  }
  return h + '</table>';
}

async function tick() {
  try {
    const res = await fetch('/api/board', { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const d = await res.json();
    document.getElementById('status').textContent = '已连接';
    document.getElementById('status').className = 'meta';
    document.getElementById('events').textContent = '事件 ' + d.eventsApplied;
    // eventsApplied === 0 不是"无事发生", 是看板压根没接上总线。两者必须分得开。
    const hb = document.getElementById('heartbeat');
    if (d.eventsApplied === 0) {
      hb.textContent = '总线上一条事件都没来过';
      hb.className = 'meta bad';
    } else if (d.lastEventAgeMs !== null && d.lastEventAgeMs !== undefined) {
      const s = (d.lastEventAgeMs / 1000).toFixed(1);
      hb.textContent = '最后一条事件 ' + s + 's 前';
      hb.className = d.lastEventAgeMs > DEAD_MS ? 'meta bad' : d.lastEventAgeMs > WARN_MS ? 'meta warn' : 'meta';
    }
    document.getElementById('symbols').innerHTML = symbolsTable(d.symbols);
    document.getElementById('accounts').innerHTML = accountsTable(d.accounts);
  } catch (e) {
    const st = document.getElementById('status');
    st.textContent = '连接失败: ' + e.message;
    st.className = 'meta bad';
    // 页面上的数字**留在原地不清空**, 但顶上的状态是红的 —— 清空会让人以为"仓位没了",
    // 而事实只是"看板取不到数了"。
  }
}

tick();
setInterval(tick, 1000);
</script>
</body>
</html>
"""
