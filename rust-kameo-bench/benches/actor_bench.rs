//! kameo Actor 基准：对照 Scala ox 那组，比较"凡事走 mailbox"与"直接操作可变状态"。
//!
//! 四个对照组（均为对一个 u64 计数器 +1）：
//!   - baseline_atomic : AtomicU64.fetch_add(SeqCst) —— 无锁 CAS，理论上限
//!   - baseline_mutex  : std::sync::Mutex 加锁 +1     —— 传统锁（≈ Scala synchronized）
//!   - actor_ask       : actor_ref.ask(Inc).await     —— 经 mailbox 往返（含 reply oneshot）
//!   - actor_tell      : actor_ref.tell(Inc).await    —— 经 mailbox 单向（bounded 背压下=消费速率）
//!
//! mailbox 容量设为 1024，与 Scala ox 基准对齐。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};

use kameo::actor::Spawn;
use kameo::mailbox;
use kameo::message::{Context, Message};
use kameo::Actor;

/// 被 actor 保护的领域对象：纯可变计数器。
#[derive(Actor)]
struct Counter {
    count: u64,
}

/// 唯一的消息：自增。
struct Inc;

impl Message<Inc> for Counter {
    type Reply = u64;

    async fn handle(&mut self, _msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.count += 1;
        self.count
    }
}

fn bench(c: &mut Criterion) {
    // 多线程 runtime：让 actor 的消费任务与发送方跑在不同 worker 上，贴近真实并发。
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // === baseline 1: 无锁 CAS ===
    let atomic = AtomicU64::new(0);
    c.bench_function("baseline_atomic", |b| {
        b.iter(|| atomic.fetch_add(1, Ordering::SeqCst))
    });

    // === baseline 2: std Mutex ===
    let mutex = Mutex::new(0u64);
    c.bench_function("baseline_mutex", |b| {
        b.iter(|| {
            let mut g = mutex.lock().unwrap();
            *g += 1;
            *g
        })
    });

    // === actor：bounded(1024) mailbox，与 Scala 对齐 ===
    let actor_ref = rt.block_on(async {
        Counter::spawn_with_mailbox(Counter { count: 0 }, mailbox::bounded(1024))
    });

    // ask：每次往返一条消息 + 等待 reply
    c.bench_function("actor_ask", |b| {
        let r = actor_ref.clone();
        b.to_async(&rt).iter(|| {
            let r = r.clone();
            async move { r.ask(Inc).await.unwrap() }
        })
    });

    // tell：单向投递，bounded mailbox 满则背压 => 稳态速率 = 消费速率
    c.bench_function("actor_tell", |b| {
        let r = actor_ref.clone();
        b.to_async(&rt).iter(|| {
            let r = r.clone();
            async move { r.tell(Inc).await.unwrap() }
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
