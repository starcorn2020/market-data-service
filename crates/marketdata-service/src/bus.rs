//! Per-FIGI broadcast fan-out + 订阅者 fan-in mpsc。
//!
//! # 拓扑
//!
//! ```text
//! ingest --publish(book)--> DashMap<Figi, broadcast::Sender>
//!                                            │
//!                          每订阅者 N 个 fan-in task
//!                          (broadcast::Receiver --> mpsc::Sender)
//!                                            │
//!                                            ▼
//!                                  Subscription { mpsc::Receiver, dropped }
//! ```
//!
//! # 不变量
//!
//! - **I1**：[`Bus::publish`] **永不阻塞 ingest**。它只调用 `broadcast::Sender::send`,
//!   后者在 buffer 满时**覆盖最旧**而非阻塞；在无订阅者时返回 `SendError`，被忽略。
//! - **I2**：每个 fan-in task 是独立的 tokio task，慢/卡的订阅者只会让自己的
//!   `mpsc::try_send` 失败累进 `dropped_total`，**不影响其他订阅者也不回压 ingest**。
//! - 跨主机 lag：`dropped_total` 是 `Arc<AtomicU64>`，Phase 2 grpc handler 在
//!   wire 阶段也写入同一个计数器，每笔 `BookUpdate` 带上 `Load(Relaxed)` 的当前值。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use marketdata_types::{BookMessage, Figi};
use tokio::sync::{broadcast, mpsc};

// ---------------------------------------------------------------------------
// Bus
// ---------------------------------------------------------------------------

/// Per-FIGI fan-out 总线。
pub struct Bus {
    senders: DashMap<Figi, broadcast::Sender<BookMessage>>,
    channel_capacity: usize,
}

impl Bus {
    /// 构造一个空总线。
    ///
    /// `channel_capacity` 是每个 FIGI 的 broadcast ring buffer 大小，
    /// 满了 broadcast 自动丢最旧 → 订阅者下次 recv 拿到 `Lagged(n)`。
    pub fn new(channel_capacity: usize) -> Self {
        assert!(channel_capacity > 0, "channel_capacity must be > 0");
        Self {
            senders: DashMap::new(),
            channel_capacity,
        }
    }

    /// **Ingest hot path**。无订阅者时静默丢弃（无 entry → noop；有 entry 但
    /// 0 receivers → `SendError` 忽略）。
    ///
    /// 永不阻塞、永不分配（DashMap shard read lock 是极轻的 RwLock）。
    #[inline]
    pub fn publish(&self, book: BookMessage) {
        if let Some(tx) = self.senders.get(&book.figi) {
            // SendError 唯一可能原因：所有 receiver 同时 drop。忽略。
            let _ = tx.send(book);
        }
    }

    /// 订阅一组 FIGI；必须在 tokio runtime 上下文调用（内部 `tokio::spawn`）。
    ///
    /// 返回的 [`Subscription`] 暴露一个统一的 `mpsc::Receiver<BookMessage>` 与
    /// `dropped_total` 共享计数器。N 个 fan-in task 并行从 N 个 broadcast::Receiver
    /// 读，再 `try_send` 到同一个 mpsc。
    ///
    /// `figis` 为空集合时返回一个立即 close 的 subscription（caller 应在外层
    /// 校验过空集合）。
    pub fn subscribe(&self, figis: &[Figi], queue_size: usize) -> Subscription {
        let (tx, rx) = mpsc::channel::<BookMessage>(queue_size);
        let dropped = Arc::new(AtomicU64::new(0));

        for figi in figis {
            // 第一次订阅该 FIGI 时才创建 broadcast channel；ingest 的 publish 走
            // get-only 路径，零创建分配。
            let sender = self
                .senders
                .entry(*figi)
                .or_insert_with(|| broadcast::channel(self.channel_capacity).0)
                .clone();
            let bc_rx = sender.subscribe();

            let tx_c = tx.clone();
            let dropped_c = dropped.clone();
            tokio::spawn(fan_in_one(bc_rx, tx_c, dropped_c));
        }

        // tx 在此处 drop —— 若 figis 为空则 mpsc 立即关闭，订阅者收 None。
        // 若 figis 非空，N 个 task 持有 tx clone，mpsc 仍存活。
        drop(tx);

        Subscription { rx, dropped }
    }
}

/// 一个 broadcast::Receiver → mpsc::Sender 的 fan-in worker。
///
/// 终止条件：
/// - mpsc 端被 client 主动 drop → `TrySendError::Closed` → return
/// - broadcast 端无 sender → `RecvError::Closed` → return
async fn fan_in_one(
    mut bc_rx: broadcast::Receiver<BookMessage>,
    tx: mpsc::Sender<BookMessage>,
    dropped: Arc<AtomicU64>,
) {
    loop {
        match bc_rx.recv().await {
            Ok(book) => match tx.try_send(book) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // 订阅者 mpsc 满 → 丢这笔 + 累进 dropped_total（I2）。
                    dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // 订阅者已断；fan-in task 退出。
                    return;
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // broadcast ring buffer 覆盖了 n 笔（ingest 发得比 fan-in 快）。
                // 不当作错误，记进 dropped_total 继续。
                dropped.fetch_add(n, Ordering::Relaxed);
            }
            Err(broadcast::error::RecvError::Closed) => {
                // bus 自身被 drop（service 关闭）。退出。
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// 单个订阅者对 [`Bus`] 的视图。
///
/// Phase 1 暂时未被使用（不启动 gRPC，没有调用 `subscribe`），但骨架完整
/// 实现是为了 Phase 2 接 tonic handler 时 0 重构。
pub struct Subscription {
    rx: mpsc::Receiver<BookMessage>,
    dropped: Arc<AtomicU64>,
}

impl Subscription {
    /// 等待下一笔 update。`None` 表示订阅已彻底结束（bus 关闭 + buffer 排空）。
    pub async fn next(&mut self) -> Option<BookMessage> {
        self.rx.recv().await
    }

    /// Server 端累积丢失数（fan-in 阶段 + gRPC 阶段共用）。
    ///
    /// **累积值**而非 delta —— GUIDELINE §4.3.3：client 重连时自动对齐，
    /// 不依赖有序传递。
    ///
    /// 目前的调用方主要是 Phase 3 测试；wire 端走 [`Self::dropped_counter`]
    /// 直接拿原子计数器（避免每次 publish 都过一次方法调用）。
    #[allow(dead_code)]
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 暴露内部共享计数器；Phase 2 gRPC handler 在 wire `try_send` 失败时
    /// 也 `fetch_add` 同一个 `AtomicU64`，让 client 看到的 `dropped_total`
    /// 涵盖"任何原因没送达"。
    pub fn dropped_counter(&self) -> Arc<AtomicU64> {
        self.dropped.clone()
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn figi(s: &str) -> Figi {
        s.parse().unwrap()
    }

    fn book(seq: u64, f: Figi) -> BookMessage {
        let mut m = BookMessage::default();
        m.figi = f;
        m.gateway_seq = seq;
        m
    }

    #[tokio::test]
    async fn publish_without_subscribers_is_noop() {
        let bus = Bus::new(16);
        // 不应 panic，不应阻塞。
        bus.publish(book(1, figi("BBG000000001")));
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers() {
        let bus = Arc::new(Bus::new(16));
        let f = figi("BBG000000001");
        let mut sub = bus.subscribe(&[f], 16);

        // tokio::spawn 给 fan-in task 一点调度时间。
        tokio::task::yield_now().await;
        bus.publish(book(7, f));

        let got = tokio::time::timeout(std::time::Duration::from_secs(1), sub.next())
            .await
            .expect("timeout")
            .expect("subscription closed");
        assert_eq!(got.gateway_seq, 7);
    }

    #[tokio::test]
    async fn empty_figis_yields_closed_subscription() {
        let bus = Bus::new(16);
        let mut sub = bus.subscribe(&[], 16);
        // 无 fan-in task → mpsc 立即关闭 → next() 返 None。
        assert!(sub.next().await.is_none());
    }

    /// **T1-unit（DEV_PROCESS §5.1）**：守 I2 "慢/斷的訂閱者不影響其他訂閱者"。
    ///
    /// 在 Bus 层（不走 gRPC）直接证明：两个 subscription 订阅同一个 FIGI，
    /// 一路 tight loop 收，一路收一笔 sleep 一笔；publisher 用 5ms/笔的稳定
    /// 速率发 N 笔；最后 fast 收完全部、`dropped=0`，slow 大部分丢、`dropped>0`。
    ///
    /// 这条 unit 测试与 `tests/grpc_slow_consumer.rs` 的 E2E 版互补：unit 版
    /// 给出"Bus 逻辑本身正确"的确定性证据；E2E 版证明 wire 路径也守得住。
    ///
    /// 关键设计：publisher 走"sleep 间隔"而非 tight loop。tight loop 会瞬间
    /// 填爆 broadcast ring（capacity=8）→ 连 fast 也 lag → 失去测试意义。
    /// 5ms/笔的速率远低于 fast 的处理速度，但远高于 slow 的 50ms/笔，
    /// 自然把"快/慢"区分出来。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_consumer_isolation() {
        use std::time::Duration;
        let bus = Arc::new(Bus::new(8));
        let f = figi("BBG000000001");

        let mut fast = bus.subscribe(&[f], 256);
        let mut slow = bus.subscribe(&[f], 4); // 小 queue 容易满

        // 等 fan-in tasks 把 broadcast::Receiver 注册并跑到 recv().await。
        tokio::time::sleep(Duration::from_millis(80)).await;

        const TOTAL: u64 = 30;
        let pub_bus = bus.clone();
        let publisher = tokio::spawn(async move {
            for seq in 1..=TOTAL {
                pub_bus.publish(book(seq, f));
                // 10ms/笔 节奏 → fast 必能吃完，slow（50ms/笔）必跟不上。
                // Windows tokio 计时器抖动下 10ms 是相对安全的间隔。
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // Fast：deadline 给到 publisher 结束后 + 500ms drain 窗口。
        let fast_task = tokio::spawn(async move {
            let mut got = 0u64;
            let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
            while tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(200), fast.next()).await {
                    Ok(Some(_)) => got += 1,
                    _ => break, // timeout = publisher 已结束 + 队列排空
                }
            }
            (got, fast.dropped_total())
        });

        // Slow：每收一笔 sleep 50ms（5x 慢于 publisher 节奏）。
        let slow_task = tokio::spawn(async move {
            let mut got = 0u64;
            let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
            while tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(200), slow.next()).await {
                    Ok(Some(_)) => {
                        got += 1;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    _ => break,
                }
            }
            (got, slow.dropped_total())
        });

        publisher.await.unwrap();
        let (fast_got, fast_dropped) = fast_task.await.unwrap();
        let (slow_got, slow_dropped) = slow_task.await.unwrap();

        eprintln!(
            "[test] fast: got={fast_got} dropped={fast_dropped} | \
             slow: got={slow_got} dropped={slow_dropped}"
        );

        // ★ I2 关键断言。
        // 容忍 fast 个位数损失（初始 broadcast 注册 race + 调度噪声）。
        // 真正要测的是"slow 不影响 fast" —— fast 没被慢路反压拖累。
        assert!(
            fast_got >= TOTAL - 3,
            "fast 应收到几乎全部（≥{} of {TOTAL}），实际 {fast_got}",
            TOTAL - 3
        );
        assert!(
            fast_dropped <= 3,
            "I2: 快消费者应几乎 0 损失（容忍 ≤3 调度噪声），实际 {fast_dropped}"
        );
        assert!(
            slow_dropped > 0,
            "I2: 慢消费者必须有 dropped（否则压力不够、测试无效）, 实际 {slow_dropped}"
        );
        assert!(
            slow_got < fast_got,
            "慢路收到应严格少于快路：slow={slow_got} fast={fast_got}"
        );
    }

    /// **T6（DEV_PROCESS §5.1 衍生）**：守 GUIDELINE §4.3.3 "累积值而非 delta"。
    ///
    /// `dropped_counter` 暴露 `Arc<AtomicU64>`，本测试验证两次连续观察之间
    /// 该值**只增不减**——客户端用差分算 lag 时只能依赖累积值。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropped_total_is_cumulative_not_delta() {
        use std::time::Duration;
        let bus = Arc::new(Bus::new(4));
        let f = figi("BBG000000001");

        let mut sub = bus.subscribe(&[f], 2); // 极小 queue，必满
        let counter = sub.dropped_counter();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // 不消费、tight loop 狂发 → broadcast 必 lag + mpsc 必 full → 必有 dropped。
        for seq in 1..=100u64 {
            bus.publish(book(seq, f));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let snap1 = counter.load(Ordering::Relaxed);
        assert!(snap1 > 0, "应有 dropped, 实际 {snap1}");

        // drain 一笔不会回退 counter。
        let _ = tokio::time::timeout(Duration::from_millis(50), sub.next()).await;
        let snap2 = counter.load(Ordering::Relaxed);
        assert!(
            snap2 >= snap1,
            "累积值绝不回退：snap1={snap1} snap2={snap2}"
        );

        // 再发一批，counter 继续涨。
        for seq in 101..=200u64 {
            bus.publish(book(seq, f));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let snap3 = counter.load(Ordering::Relaxed);
        assert!(
            snap3 > snap2,
            "再发一批后累积值应再涨：snap2={snap2} snap3={snap3}"
        );
    }
}
