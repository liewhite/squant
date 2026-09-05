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
  td.asset { font-weight:600; }
  .venue { display:inline-block; margin-right:18px; }
  .venue .ex { color:var(--dim); font-size:11px; display:block; }
  .venue .px { font-variant-numeric:tabular-nums; margin-right:5px; }
  .venue .bidask { color:var(--dim); font-size:11px; display:block; font-variant-numeric:tabular-nums; }
  .edge { font-size:15px; font-variant-numeric:tabular-nums; }
  .accts { margin-top:4px; font-weight:400; }
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
  <span class="meta" id="venues"></span>
</header>
<section>
  <h2>资产（同一资产的各家交易所并排）</h2>
  <div id="symbols"></div>
</section>
<section>
  <h2>账户</h2>
  <div id="accounts"></div>
</section>

<script>
// **"没更新"不等于"数据坏了"。**
//
// 股票永续在美股闭市时段几乎不成交, 报价几十秒不动是常态, 而那个价仍是当前真实盘口。
// 连接真断了是另一回事: WsLoop 有 5 分钟空闲看门狗, 那时进程直接死, 看板也就没了 ——
// 所以**进程还活着就说明连接是通的**, 报价不动只能说明市场安静。
// 从前这里按绝对年龄给每条报价染色 (5s 黄 / 30s 红), 于是非交易时段满屏黄红, 而什么都没坏。
//
// 真正说明问题的是**不对称**:
//   - 同一个资产上, 一家还在报而另一家落后很多 -> 那家可能卡住了, 且此时两家的价不可比;
//   - 一整家交易所的心跳停了 (下面的 venueHeartbeat) -> 那家的流卡住了。
const SKEW_WARN_MS = 5000;    // 组内报价时刻差: 超过它, 两家的价已经不可比
const VENUE_DEAD_MS = 60000;  // 一整家所这么久没有任何事件 -> 它的流很可能卡住了

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

/** 年龄: 只显示, 不按绝对阈值上色。
 *
 * 无论行情还是仓位, 年龄都是"距上次更新多久", 而不动本身不是故障 —— 见文件头的说明。
 */
function sinceCell(ms) {
  if (ms === null || ms === undefined) return '<span class="none">—</span>';
  return '<span class="age">' + fmtAge(ms) + '</span>';
}

/** 组内落后的那一家: 它比同组最新的那条旧了多少。落后太多就标出来 —— 那既可能是它卡住了,
 *  也意味着此刻它的价与别家不可比。 */
function laggingCell(ms, freshest) {
  if (ms === null || ms === undefined) return '<span class="none">—</span>';
  const behind = ms - freshest;
  const cls = behind > SKEW_WARN_MS ? 'old' : '';
  return '<span class="age ' + cls + '">' + fmtAge(ms) + '</span>';
}

// 没有读数时显示 —— 而不是 0。这两者在交易上完全不是一回事。
const dash = '<span class="none">—</span>';

function reading(r, fmt) { return r ? fmt(r.value) + ' ' + sinceCell(r.ageMs) : dash; }

function sideCls(v) { return v > 0 ? 'long' : v < 0 ? 'short' : 'none'; }

/** 跨所极差 = 各所中价的 (max − min) / min，单位 bp。
 *
 * **判据是同时性，不是新鲜度。** 三家一起安静时报价都"旧"却都仍是当前真实盘口，那个价差是
 * 真的（非交易时段正是这样）；真正不可比的是**一边现价配另一边几十秒前的价** —— 差出来的
 * 里面掺着这几十秒里对方走过的路。这与 crossspread 的 `maxQuoteAgeMs` 是同一条理由：
 * "一边的陈旧报价配上另一边的实时报价，会把对方的正常波动算成价差异动"。
 *
 * 所以照算不误，但把时刻差 (skew) 一并显示出来；skew 大到不可比时标黄，让人自己判断。
 *
 * 这只是把页面上已经并排显示的几个数做一次减法，**不是策略的异动判断**：后者要看这一对
 * 自己的中枢与 z 值（见 crossspread 的 SpreadDislocations），持续存在的价差是结构性的，
 * 照着它开仓等来的不是回归。别把这一列当信号。
 */
function crossVenueBps(venues) {
  const bids = venues.filter(v => v.bbo).map(v => v.bbo.value.bid);
  const asks = venues.filter(v => v.bbo).map(v => v.bbo.value.ask);
  if (bids.length < 2) return null;
  // **可执行**的边: 卖在最高的买一、买在最低的卖一。用中价算会系统性高估两边价差的均值 ——
  // 而那一截恰好落在决策边界上 (见 ArbPlan)。可能是负数, 那说明此刻吃不动, 如实显示。
  const lo = Math.min(...asks);
  return lo > 0 ? (Math.max(...bids) - lo) / lo * 10000 : null;
}

function assetsTable(rows) {
  if (!rows.length) return '<div class="empty">还没有任何标的的数据。行情源连上了吗？</div>';
  let h = '<table><tr><th>标的</th><th>各交易所价格</th><th class="num">最大价差</th></tr>';
  for (const r of rows) {
    const ages = r.venues.filter(v => v.bbo).map(v => v.bbo.ageMs);
    const freshest = ages.length ? Math.min(...ages) : 0;

    // 第二列: 各家并排。每家一小块 —— 交易所 / 中价 / 年龄, 以及它自己的买卖一。
    const prices = r.venues.map(v => {
      const b = v.bbo;
      if (!b) return '<span class="venue"><span class="ex">' + esc(v.exchange) + '</span>' + dash + '</span>';
      return '<span class="venue"><span class="ex">' + esc(v.exchange) + '</span>'
        + '<span class="px">' + num(b.value.mid) + '</span>'
        + laggingCell(b.ageMs, freshest)
        + '<span class="bidask">' + num(b.value.bid) + ' / ' + num(b.value.ask) + '</span>'
        + '</span>';
    }).join('');

    // 第三列: 最大价差。**可执行**的那个 —— 见 crossVenueBps。
    const edge = crossVenueBps(r.venues);
    const skew = r.quoteSkewMs;
    const skewNote = skew === null || skew === undefined ? ''
      : '<div class="sub' + (skew > SKEW_WARN_MS ? ' warn' : '') + '">skew ' + fmtAge(skew) + '</div>';
    const spread = edge === null
      ? dash
      : '<span class="edge">' + edge.toFixed(1) + 'bp</span>' + skewNote;

    // 账户信息挂在标的名下面 —— 它不是这三列的主角, 但没地方放会丢掉
    const accounts = accountsCell(r.venues.flatMap(v => v.accounts.map(a => [v, a])));

    h += '<tr>'
      + '<td class="asset">' + esc(r.asset) + accounts + '</td>'
      + '<td>' + prices + '</td>'
      + '<td class="num">' + spread + '</td>'
      + '</tr>';
  }
  return h + '</table>';
}

function accountsCell(pairs) {
  if (!pairs.length) return '';
  return '<div class="accts">' + pairs.map(([v, a]) => {
    const pos = a.position
      // 仓位是变更驱动的: 不动的仓位年龄会一直涨, 那是正常的, 不该染色。
      ? '<span class="' + sideCls(a.position.value) + '">' + num(a.position.value, 4) + '</span> ' + sinceCell(a.position.ageMs)
      : dash;
    let s = '<div class="sub"><span class="pill">' + esc(a.account) + '@' + esc(v.exchange) + '</span>' + pos;
    if (a.pendingOrders.length) {
      s += ' 挂单 ' + a.pendingOrders.map(o =>
        esc(o.side) + ' ' + num(o.quantity, 4) + '@' + num(o.price) + (o.reduceOnly ? '(RO)' : '')).join(' · ');
    }
    return s + '</div>';
  }).join('') + '</div>';
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

/** 把一份视图渲染上去。 */
function render(d) {
  document.getElementById('events').textContent = '事件 ' + d.eventsApplied;
  const hb = document.getElementById('heartbeat');
  // eventsApplied === 0 不是"无事发生", 是看板压根没接上总线。两者必须分得开。
  if (d.eventsApplied === 0) {
    hb.textContent = '总线上一条事件都没来过';
    hb.className = 'meta bad';
  } else if (d.lastEventAgeMs !== null && d.lastEventAgeMs !== undefined) {
    hb.textContent = '最后一条事件 ' + (d.lastEventAgeMs / 1000).toFixed(1) + 's 前';
    hb.className = d.lastEventAgeMs > VENUE_DEAD_MS ? 'meta bad' : 'meta';
  }
  // 各家交易所的心跳 —— "这一家的流还活着吗"唯一靠得住的读数。某个标的不报价可能只是
  // 没人交易; 一整家所都没有任何事件, 才说明那家卡住了。
  document.getElementById('venues').innerHTML = (d.venues || []).map(v =>
    '<span class="pill' + (v.ageMs > VENUE_DEAD_MS ? ' bad' : '') + '">' + esc(v.exchange) + ' ' + fmtAge(v.ageMs) + '</span>'
  ).join('');
  document.getElementById('symbols').innerHTML = assetsTable(d.assets);
  document.getElementById('accounts').innerHTML = accountsTable(d.accounts);
}

// **推送, 不是轮询。** 服务端有新快照才发 (并强制静默一小段合并) —— 行情安静的时段
// (美股闭市) 一个字节都不发, 只留心跳。EventSource 自带断线重连。
let last = null;
const es = new EventSource('/api/stream');
const status = document.getElementById('status');

es.onopen = () => { status.textContent = '已连接 (实时推送)'; status.className = 'meta'; };

es.onmessage = ev => {
  try {
    const d = JSON.parse(ev.data);
    d.receivedAt = Date.now();
    last = d;
    render(d);
  } catch (e) {
    status.textContent = '渲染失败: ' + e.message;
    status.className = 'meta bad';
  }
};

es.onerror = () => {
  // **页面上的数字留在原地不清空**, 只把顶上标红: 清空会让人以为"仓位没了",
  // 而事实只是"看板取不到数了"。EventSource 会自己重连, 连上后 onopen 会把状态改回去。
  status.textContent = '连接断开, 重连中…';
  status.className = 'meta bad';
};

// 年龄要自己走 —— 两次推送之间页面上的"3s 前"不该冻住。只重画年龄那几处代价太大,
// 所以整份重画: 数据在本地, 一秒一次的 DOM 重建对几百行是可接受的。
setInterval(() => {
  if (!last) return;
  // 本地推进时钟: 服务端给的 ageMs 是生成那一刻的, 这里补上从那时到现在的差。
  const drift = Date.now() - last.receivedAt;
  if (drift > 900) render(agedBy(last, drift));
}, 1000);

/** 把一份视图里所有 ageMs 往前推 `ms` —— 纯函数, 不改原对象。 */
function agedBy(d, ms) {
  const bump = r => r ? { ...r, ageMs: r.ageMs + ms } : r;
  return {
    ...d,
    lastEventAgeMs: d.lastEventAgeMs === null || d.lastEventAgeMs === undefined ? d.lastEventAgeMs : d.lastEventAgeMs + ms,
    venues: (d.venues || []).map(v => ({ ...v, ageMs: v.ageMs + ms })),
    assets: (d.assets || []).map(a => ({
      ...a,
      venues: a.venues.map(v => ({
        ...v,
        bbo: bump(v.bbo), markPrice: bump(v.markPrice), funding: bump(v.funding), lastTradePrice: bump(v.lastTradePrice),
        accounts: v.accounts.map(x => ({
          ...x,
          position: bump(x.position),
          pendingOrders: x.pendingOrders.map(o => ({ ...o, ageMs: o.ageMs + ms })),
          lastFill: x.lastFill ? { ...x.lastFill, ageMs: x.lastFill.ageMs + ms } : x.lastFill,
        })),
      })),
    })),
    accounts: (d.accounts || []).map(a => ({
      ...a,
      equity: bump(a.equity),
      balances: a.balances.map(b => ({ ...b, ageMs: b.ageMs + ms })),
    })),
  };
}

</script>
</body>
</html>
"""
