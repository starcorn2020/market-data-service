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
    /// 0 receivers → `SendError` 忽略,见下面 publish 内注释列出的两条 0-receivers
    /// 路径）。
    ///
    /// 永不阻塞、永不分配（DashMap shard read lock 是极轻的 RwLock）。
    #[inline]
    pub fn publish(&self, book: BookMessage) {
        if let Some(tx) = self.senders.get(&book.figi) {
            // SendError 在两种情况发生,均忽略:
            //   ① 所有 receiver 同时 drop —— 订阅者退订后下一笔 publish 走到这里;
            //   ② B1 race window:subscribe 进行中,entry 已 insert 但
            //      `sender.subscribe()` 尚未执行的 ns 级窗口。Benign:
            //      - 窗口宽度 ~ 一次 Arc clone,真实负载下平均丢 0-1 笔;
            //      - 丢的这笔属"from-now 切点"语义边界,C3 测试没承诺精确切点;
            //      - client 标准用法是先 GetSnapshot 后 Subscribe,丢的内容已在
            //        snapshot 表(GUIDELINE「先 put 后 publish」顺序不变量);
            //      - 修此 race 需让 entry 创建 + receiver 注册成单个原子操作,
            //        会影响 publish hot path 的 DashMap shard 设计,不值得。
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
///
/// # Log 策略（Phase 4 切 tracing 时统一替换）
///
/// 只在 4 条**错误/状态变化**分支打 `eprintln!`，正常路径（每笔 Ok→Ok）零开销。
/// `TrySendError::Full` 持续满时会每笔都触发，用 `is_power_of_two` 做对数级
/// 自抑制，避免 stderr 刷屏。
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
                    // fetch_add 返回 +1 前的旧值，所以新值 = prev + 1。
                    let prev = dropped.fetch_add(1, Ordering::Relaxed);
                    let now = prev + 1;
                    eprintln!(
                        "[bus] subscriber mpsc full, dropped_total={now}"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // 订阅者已断；fan-in task 退出。
                    eprintln!(
                        "[bus] subscriber disconnected, fan-in task exiting \
                         (dropped_total={})",
                        dropped.load(Ordering::Relaxed),
                    );
                    return;
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // broadcast ring buffer 覆盖了 n 笔（ingest 发得比 fan-in 快）。
                // 不当作错误，记进 dropped_total 继续。
                let prev = dropped.fetch_add(n, Ordering::Relaxed);
                eprintln!(
                    "[bus] broadcast lagged: missed={n} dropped_total={}",
                    prev + n,
                );
            }
            Err(broadcast::error::RecvError::Closed) => {
                // bus 自身被 drop（service 关闭）。退出。
                eprintln!(
                    "[bus] broadcast closed, fan-in task exiting \
                     (dropped_total={})",
                    dropped.load(Ordering::Relaxed),
                );
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
    //! Bus / Subscription / fan_in_one 单元测试。
    //!
    //! # 组织方式
    //!
    //! 测试按"守的不变量"分区,reviewer 顺序读完即可建立完整 bus.rs 行为认知:
    //!
    //! | 分区 | 守的契约 | 测试 |
    //! |---|---|---|
    //! | 基础契约 | happy path / 边界 | `publish_without_subscribers_is_noop` / `subscribe_then_publish_delivers` / `empty_figis_yields_closed_subscription` |
    //! | fan-in 合流 | Subscription 的核心非平凡逻辑 | `multi_figi_fan_in_merges_streams` |
    //! | I2 隔离 | 慢/断的订阅者不影响其他 | `slow_consumer_isolation` / `disconnected_subscriber_does_not_stall_others` |
    //! | `dropped_total` 累积值 | GUIDELINE §4.3.3 累积值而非 delta | `dropped_total_is_cumulative_not_delta` |
    //! | 订阅语义 | from-now / sender 生命周期 | `messages_before_subscribe_are_not_replayed` / `senders_entry_persists_after_all_subscribers_dropped` |
    //!
    //! # 关键设计取舍
    //!
    //! - **publisher 走 `sleep` 间隔而非 tight loop**:tight loop 会瞬间填爆
    //!   broadcast ring(`cap=8`)→ 连 fast 也 lag → 失去"快/慢"对比意义。
    //!   I2 测试用 10ms/笔节奏稳定区分 fast(无 sleep)与 slow(50ms/笔)。
    //! - **deadline + `timeout(per-poll)` 而非 `sleep(constant)`**:CI 上 tokio
    //!   启动 + std::thread 启动 + condvar wakeup 抖动可达 ±50ms,固定 sleep 易
    //!   flake。主动 poll 是"达标即返回 + 兜底超时"的双赢。
    //! - **多 FIGI fan-in 顺序不保证**:N 个并发 fan_in_one task 抢 mpsc::Sender,
    //!   到达顺序与 tokio 调度相关。assert 用 sort + multiset 比对,不比对到达
    //!   顺序。
    //! - **B3 议题**(senders entry 不随订阅者退订自动清空)用
    //!   `senders_entry_persists_after_all_subscribers_dropped` 固化当前行为,
    //!   未来若决定改为收缩 entry,该测试即明确的回归告警点。详见 DEV_PROCESS §6.4。

    use super::*;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    fn figi(s: &str) -> Figi {
        s.parse().unwrap()
    }

    fn book(seq: u64, f: Figi) -> BookMessage {
        BookMessage {
            figi: f,
            gateway_seq: seq,
            ..Default::default()
        }
    }

    // -----------------------------------------------------------------------
    // 分区 1:基础契约(happy path / 边界)
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // 分区 2:fan-in 合流(Subscription 的核心非平凡逻辑)
    // -----------------------------------------------------------------------

    /// **C1**:`Subscription` 的核心职责是把 N 条 broadcast::Receiver 合并成
    /// 一条 mpsc::Receiver。订阅 `[a, b]`,a/b 各 publish 一笔,sub.next() 必须
    /// 能各收到一笔且 figi 正确。
    ///
    /// # 顺序为何不能 assert
    ///
    /// 两条 fan_in_one task 并发跑 → 各自从独立 broadcast::Receiver pull →
    /// 抢同一个 mpsc::Sender 的写入位。到达 mpsc 的顺序与 tokio 调度相关,
    /// 与 publish 顺序无关。assert 走 multiset 比对(sort 后 ==)。
    ///
    /// # 守的不变量
    ///
    /// - **fan-in 合流正确性**:N 条流的并集 = sub 收到的集合(不漏)。
    /// - **每条 broadcast 独立**:a 的 publish 不会让 b 的 fan_in_one 漏掉。
    /// - **`dropped_total = 0`**:无任何丢失,排除"靠丢覆盖问题"的假阳性。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_figi_fan_in_merges_streams() {
        let bus = Arc::new(Bus::new(16));
        let fa = figi("BBG000000001");
        let fb = figi("BBG000000002");

        let mut sub = bus.subscribe(&[fa, fb], 16);

        // 等 2 个 fan_in_one task 完成 broadcast::Sender::subscribe 并跑到
        // recv().await。这一步必要 —— subscribe 在 spawn 时刻才注册 receiver,
        // 若没等就 publish,broadcast::Sender 看不到 receiver,消息直接丢。
        tokio::time::sleep(Duration::from_millis(50)).await;

        bus.publish(book(11, fa));
        bus.publish(book(22, fb));
        bus.publish(book(33, fa));

        let mut got: Vec<(u64, Figi)> = Vec::new();
        for _ in 0..3 {
            let b = tokio::time::timeout(Duration::from_millis(500), sub.next())
                .await
                .expect("timeout: fan-in 合流 3 笔超时")
                .expect("subscription closed");
            got.push((b.gateway_seq, b.figi));
        }
        got.sort_by_key(|t| t.0);

        assert_eq!(
            got,
            vec![(11, fa), (22, fb), (33, fa)],
            "multiset 必须 = 三笔 publish 的集合(顺序不限)"
        );
        assert_eq!(
            sub.dropped_total(),
            0,
            "正常路径无任何丢失(排除靠丢遮盖问题的假阳性)"
        );
    }

    // -----------------------------------------------------------------------
    // 分区 3:I2 隔离(慢/断的订阅者不影响其他)
    // -----------------------------------------------------------------------

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

    /// **C2**:守 I2 "**斷**的訂閱者不影響其他訂閱者"。
    ///
    /// `slow_consumer_isolation` 验证"慢"的隔离;本测试单独验证"断"的隔离 ——
    /// 即一个订阅者被主动 `drop` 后,另一个订阅者仍正常收到全部更新且
    /// `dropped_total = 0`。这是 DEV_PROCESS §5.4 「未做但已记录」的项,Phase 4
    /// 补齐。
    ///
    /// # 断订阅者的生命周期
    ///
    /// 1. `drop(_to_drop)` → `Subscription` drop → 内部 `mpsc::Receiver` drop。
    /// 2. 该订阅者对应的 `fan_in_one` task **仍在 `broadcast::Receiver::recv().await`**
    ///    上挂着,**直到下一笔 publish 唤醒它** → `try_send` 返 `Closed` → task return。
    /// 3. 之后该订阅者对应的 `broadcast::Sender` 仍持有(senders 表不缩,B3 议题),
    ///    但其 receiver_count 减 1,后续 publish 走该 sender 仍 OK,只是少推一份。
    ///
    /// # 断言重点
    ///
    /// - `survivor` 收到全部 3 笔(不漏)。
    /// - `survivor.dropped_total() == 0`(不被另一条退订路径污染)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disconnected_subscriber_does_not_stall_others() {
        let bus = Arc::new(Bus::new(16));
        let f = figi("BBG000000001");

        let mut survivor = bus.subscribe(&[f], 16);
        let to_drop = bus.subscribe(&[f], 16);

        // 等两个 fan_in_one task 都跑到 recv().await。
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 主动断掉一个订阅者。注意:此刻被断者的 fan_in_one task 还在 await,
        // 直到下一笔 publish 才会真正退出 —— 本测试只关心"是否影响 survivor"。
        drop(to_drop);

        bus.publish(book(1, f));
        bus.publish(book(2, f));
        bus.publish(book(3, f));

        let mut got = Vec::new();
        for _ in 0..3 {
            let b = tokio::time::timeout(Duration::from_millis(500), survivor.next())
                .await
                .expect("timeout: survivor 漏收")
                .expect("survivor subscription closed");
            got.push(b.gateway_seq);
        }

        assert_eq!(got, vec![1, 2, 3], "survivor 必须按序收到全部三笔");
        assert_eq!(
            survivor.dropped_total(),
            0,
            "I2:断开 sibling 订阅者不应让 survivor 出现任何丢失,实际 {}",
            survivor.dropped_total()
        );
    }

    // -----------------------------------------------------------------------
    // 分区 4:dropped_total 累积值(GUIDELINE §4.3.3)
    // -----------------------------------------------------------------------

    /// **T6（DEV_PROCESS §5.1 衍生）**：守 GUIDELINE §4.3.3 "累积值而非 delta"。
    ///
    /// `dropped_counter` 暴露 `Arc<AtomicU64>`，本测试验证两次连续观察之间
    /// 该值**只增不减**——客户端用差分算 lag 时只能依赖累积值。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropped_total_is_cumulative_not_delta() {
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

    // -----------------------------------------------------------------------
    // 分区 5:订阅语义(from-now / sender 生命周期)
    // -----------------------------------------------------------------------

    /// **C3**:固化 GUIDELINE §3.2 / DEV_PROCESS §3.2 拍板的 **from-now 语义** ——
    /// `Bus::publish` 走 get-only 路径,无订阅者时根本不创 broadcast::Sender,
    /// 所以订阅前 publish 的消息既不入 ring buffer 也不持久化。
    ///
    /// # 设计取舍
    ///
    /// 这是显式的"无 history / 无 replay"决策(详见待问清单 Q11):
    /// - 优点:零状态 → ingest hot path 无 history buffer → 守 I1。
    /// - 取舍:client 想要订阅起点的最新一笔,必须在 subscribe 前 / 后自己
    ///   先调用 `GetSnapshot`(README §3 的 R/R API)。
    ///
    /// # 守的契约
    ///
    /// - 订阅前 publish 的 seq=1, 2 **不**会出现在 sub.next() 中。
    /// - 订阅后 publish 的 seq=99 **必**收到。
    /// - 第二次 `next()` 必须 timeout(没有任何 replay)。
    #[tokio::test]
    async fn messages_before_subscribe_are_not_replayed() {
        let bus = Arc::new(Bus::new(16));
        let f = figi("BBG000000001");

        // 无订阅者时 publish → senders.get(&f) = None → 直接 return,
        // 既不创 broadcast::Sender 也不缓存消息。
        bus.publish(book(1, f));
        bus.publish(book(2, f));

        let mut sub = bus.subscribe(&[f], 16);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 订阅后才 publish 的这笔必须能收到。
        bus.publish(book(99, f));

        let got = tokio::time::timeout(Duration::from_millis(500), sub.next())
            .await
            .expect("timeout: 订阅后的 publish 应能收到")
            .expect("subscription closed");
        assert_eq!(
            got.gateway_seq, 99,
            "from-now 语义:首笔必须是订阅后 publish 的 seq=99(订阅前的 1, 2 不应补发)"
        );

        // 第二次 next 应当 timeout —— 没有 seq=1, 2 的回放。
        let extra = tokio::time::timeout(Duration::from_millis(100), sub.next()).await;
        assert!(
            extra.is_err(),
            "from-now 语义:订阅前的 publish 不应进入流,实际拿到 {extra:?}"
        );
    }

    /// **C4 / B3 议题固化**:**senders 表 entry 不随订阅者退订自动清空**。
    ///
    /// 当前实现:`Bus::subscribe` 在 entry 缺失时 `or_insert_with` 创 sender,
    /// 但**没有任何路径**在订阅者全退订后从 `senders` 移除 entry。长跑下若
    /// 持续有 unique FIGI 进出,`senders` 大小**单调递增**,内存不回收。
    ///
    /// # 为什么把当前行为固化成测试
    ///
    /// 1. **明确告警点**:未来若有人为 B3 加 entry 收缩(例如 `Bus::publish` 检测
    ///    `tx.receiver_count() == 0` 时移除 entry),本测试 fail,reviewer 立即
    ///    知道行为变更,有意识地走文档更新流程。
    /// 2. **交付素材**:Phase 4 README 在「Future work」或「设计取舍」段引用,
    ///    展示"已知问题 + 量化边界 + 故意留待 trade-off"的工程成熟度。
    ///
    /// # 边界与缓解
    ///
    /// - 量纲:每 entry 是 `(Figi 12B + broadcast::Sender, 约几十 B)` × 数百万
    ///   unique FIGI 才有意义。真实交易所 FIGI 数 < 100k,3 天 deliverable 不
    ///   构成瓶颈。
    /// - 缓解候选(若未来真撞墙):
    ///   - 周期性 GC task 扫 `senders` 把 `receiver_count == 0` 的 entry 移除。
    ///   - 在 `fan_in_one` 退出最后一个 receiver 时主动 `senders.remove(&figi)`,
    ///     但需要回带 figi 参数 → 改 fan_in_one 签名。
    ///
    /// 详见待问清单 B3 / DEV_PROCESS §6.4。
    #[tokio::test]
    async fn senders_entry_persists_after_all_subscribers_dropped() {
        let bus = Bus::new(16);
        let f = figi("BBG000000001");

        assert_eq!(bus.senders.len(), 0, "起始无 entry");

        {
            let _sub = bus.subscribe(&[f], 16);
            assert_eq!(
                bus.senders.len(),
                1,
                "subscribe 触发 or_insert_with → entry=1"
            );
        }
        // _sub 离开作用域 → mpsc::Receiver drop → 但 fan_in_one task 仍在
        // broadcast::recv().await 上挂着。

        // 触发一笔 publish 让 fan_in_one 走到 try_send(Closed) 分支并退出。
        bus.publish(book(1, f));
        tokio::time::sleep(Duration::from_millis(100)).await;

        // ★ B3 行为固化:即使 fan_in_one 已退出、broadcast 已无 receiver,
        // senders 中的 entry **不会**被自动清除。
        assert_eq!(
            bus.senders.len(),
            1,
            "B3:订阅者全退订后 entry 不缩 → 长跑下 unique FIGI 数 = senders 大小"
        );

        // 再 publish 一笔验证 noop(get-only 路径,SendError 被 `let _ =` 吞掉)。
        bus.publish(book(2, f));
        assert_eq!(bus.senders.len(), 1, "publish 不创新 entry 也不删旧 entry");
    }
}
