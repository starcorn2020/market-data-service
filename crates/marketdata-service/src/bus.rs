//! Per-FIGI broadcast fan-out + per-subscriber fan-in mpsc.
//!
//! # Topology
//!
//! ```text
//! ingest --publish(book)--> DashMap<Figi, broadcast::Sender>
//!                                            │
//!                          N fan-in tasks per subscriber
//!                          (broadcast::Receiver --> mpsc::Sender)
//!                                            │
//!                                            ▼
//!                                  Subscription { mpsc::Receiver, dropped }
//! ```
//!
//! # Invariants
//!
//! - **Ingest never blocks**: `publish` only calls
//!   `broadcast::Sender::send`. When the ring buffer is full it overwrites
//!   the oldest entry instead of blocking; when there are no subscribers the
//!   `SendError` is ignored.
//! - **Subscribers are isolated from each other**: each subscriber's fan-in
//!   is an independent tokio task. Slow / stuck subscribers only cause
//!   their own `mpsc::try_send` to fail and increment their own
//!   `dropped_total`; ingest is not back-pressured and sibling subscribers
//!   are unaffected.
//! - **`dropped_total` is shared across stages**: `Arc<AtomicU64>`, written
//!   by both the fan-in stage and the gRPC wire stage. Each `BookUpdate`
//!   the client receives carries the cumulative count of "messages not
//!   delivered for any reason".

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use marketdata_types::{BookMessage, Figi};
use tokio::sync::{broadcast, mpsc};

// ---------------------------------------------------------------------------
// Bus
// ---------------------------------------------------------------------------

/// Per-FIGI fan-out bus.
pub struct Bus {
    senders: DashMap<Figi, broadcast::Sender<BookMessage>>,
    channel_capacity: usize,
}

impl Bus {
    /// Construct an empty bus.
    ///
    /// `channel_capacity` is the per-FIGI broadcast ring buffer size; when
    /// it fills, broadcast automatically drops the oldest entry, and the
    /// subscriber's next `recv` returns `Lagged(n)`.
    pub fn new(channel_capacity: usize) -> Self {
        assert!(channel_capacity > 0, "channel_capacity must be > 0");
        Self {
            senders: DashMap::new(),
            channel_capacity,
        }
    }

    /// Ingest hot path. Never blocks, never allocates (a DashMap shard read
    /// lock is an extremely light `RwLock`). Silently discards when there
    /// are no subscribers.
    #[inline]
    pub fn publish(&self, book: BookMessage) {
        if let Some(tx) = self.senders.get(&book.figi) {
            // `SendError` only appears when there are 0 receivers. Both
            // sources are ignored:
            //   ① all subscribers have unsubscribed;
            //   ② a subscribe is in progress — the entry has been inserted
            //      but `sender.subscribe()` has not yet run (a ns-scale
            //      race window). The window is about one Arc clone wide;
            //      under realistic load this loses 0–1 messages on average.
            //      The standard client pattern is GetSnapshots followed by
            //      Subscribe, so the lost content is already in the
            //      snapshot table. Fixing this race would require making
            //      entry creation + receiver registration atomic, which
            //      breaks the hot path's lock-free design — not worth it.
            let _ = tx.send(book);
        }
    }

    /// Subscribe to a set of FIGIs; must be called within a tokio runtime
    /// context (uses `tokio::spawn` internally).
    ///
    /// The returned [`Subscription`] exposes a unified
    /// `mpsc::Receiver<BookMessage>` and a shared `dropped_total` counter.
    /// N fan-in tasks read from N broadcast::Receivers in parallel and
    /// `try_send` to the same mpsc.
    ///
    /// When `figis` is empty, returns a subscription that closes
    /// immediately (the caller should have already validated the empty case).
    pub fn subscribe(&self, figis: &[Figi], queue_size: usize) -> Subscription {
        let (tx, rx) = mpsc::channel::<BookMessage>(queue_size);
        let dropped = Arc::new(AtomicU64::new(0));

        for figi in figis {
            // Create the broadcast channel only on the first subscription
            // to that FIGI; ingest's `publish` takes the get-only path,
            // with zero allocation for new entries.
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

        // tx is dropped here — if figis is empty the mpsc closes
        // immediately and the subscriber sees None. If figis is non-empty,
        // the N tasks hold clones of tx and the mpsc stays alive.
        drop(tx);

        Subscription { rx, dropped }
    }
}

/// A `broadcast::Receiver → mpsc::Sender` fan-in worker.
///
/// On the happy path each message is zero-alloc, zero-log. Termination
/// conditions:
///
/// - Subscriber drops `Subscription` → `TrySendError::Closed` → return.
/// - Bus itself is dropped (global shutdown) → `RecvError::Closed` → return.
///
/// `TrySendError::Full` and `RecvError::Lagged(n)` are not treated as
/// errors: they only increment `dropped_total` and the loop continues.
/// The two have distinct semantics: `Full` means the subscriber's
/// downstream mpsc is full (slow consumer), while `Lagged` means the
/// subscriber's upstream broadcast ring was overwritten (ingest is faster
/// than fan-in).
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
                    let now = dropped.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "[bus] subscriber mpsc full, dropped_total={now}"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Subscriber is gone; this fan-in task exits.
                    eprintln!(
                        "[bus] subscriber disconnected, fan-in task exiting \
                         (dropped_total={})",
                        dropped.load(Ordering::Relaxed),
                    );
                    return;
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                let now = dropped.fetch_add(n, Ordering::Relaxed) + n;
                eprintln!("[bus] broadcast lagged: missed={n} dropped_total={now}");
            }
            Err(broadcast::error::RecvError::Closed) => {
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

/// Client-facing subscription handle: a single mpsc stream plus the shared
/// `dropped_total` counter.
pub struct Subscription {
    rx: mpsc::Receiver<BookMessage>,
    dropped: Arc<AtomicU64>,
}

impl Subscription {
    /// Await the next update. `None` means the subscription has ended for
    /// good (bus closed and buffer drained).
    pub async fn next(&mut self) -> Option<BookMessage> {
        self.rx.recv().await
    }

    /// Expose the internal shared counter. When `try_send` on the wire
    /// fails, the gRPC handler `fetch_add`s the same `AtomicU64`, so the
    /// `dropped_total` the client sees covers "messages not delivered for
    /// any reason".
    ///
    /// This is a **cumulative value**, not a delta — the client can diff
    /// consecutive values to compute lag, without relying on ordered
    /// delivery and without requiring the server to track "the value last
    /// reported to this client".
    pub fn dropped_counter(&self) -> Arc<AtomicU64> {
        self.dropped.clone()
    }
}

#[cfg(test)]
impl Subscription {
    /// Test-only helper that reads the cumulative value directly.
    /// Production code goes through `dropped_counter()` to obtain the Arc.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    //! Unit tests for Bus / Subscription / fan_in_one, organized by the
    //! contracts they guard. Details and design trade-offs for each
    //! contract live on the corresponding test's doc; this module-level
    //! doc only lists the sections.
    //!
    //! 1. Basic contract (happy path / edges)
    //! 2. fan-in merge
    //! 3. Subscriber isolation (slow / disconnected)
    //! 4. `dropped_total` cumulative semantics
    //! 5. Subscribe semantics (from-now / sender entry lifecycle)

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
    // Section 1: basic contract (happy path / edges)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn publish_without_subscribers_is_noop() {
        let bus = Bus::new(16);
        // Should not panic, should not block.
        bus.publish(book(1, figi("BBG000000001")));
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers() {
        let bus = Arc::new(Bus::new(16));
        let f = figi("BBG000000001");
        let mut sub = bus.subscribe(&[f], 16);

        // Give the tokio::spawn'd fan-in task a moment to be scheduled.
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
        // No fan-in tasks → mpsc closes immediately → next() returns None.
        assert!(sub.next().await.is_none());
    }

    // -----------------------------------------------------------------------
    // Section 2: fan-in merge (the core non-trivial behavior of Subscription)
    // -----------------------------------------------------------------------

    /// The core responsibility of `Subscription`: merge N
    /// `broadcast::Receiver`s into a single `mpsc::Receiver`. Subscribe to
    /// `[a, b]`, publish one message to each; `sub.next()` must return one
    /// message per FIGI with the correct figi field.
    ///
    /// # Why we cannot assert on order
    ///
    /// The two `fan_in_one` tasks run concurrently, each pulling from its
    /// own `broadcast::Receiver` and racing to write to the same
    /// `mpsc::Sender`. Arrival order depends on tokio scheduling, not on
    /// publish order. The assertion compares as a multiset (sort then ==).
    ///
    /// # Invariants guarded
    ///
    /// - **fan-in merge correctness**: the union of N streams = the set
    ///   the subscriber receives (no drops).
    /// - **Per-broadcast independence**: a publish to `a` does not cause
    ///   `b`'s `fan_in_one` to miss anything.
    /// - **`dropped_total = 0`**: zero loss on the happy path (rules out
    ///   the false positive of "loss masking the bug").
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_figi_fan_in_merges_streams() {
        let bus = Arc::new(Bus::new(16));
        let fa = figi("BBG000000001");
        let fb = figi("BBG000000002");

        let mut sub = bus.subscribe(&[fa, fb], 16);

        // Wait for both `fan_in_one` tasks to complete
        // `broadcast::Sender::subscribe` and reach `recv().await`. This
        // step is required — subscribe registers the receiver only at
        // spawn time; if we publish before that, the broadcast::Sender
        // sees no receiver and the messages are dropped.
        tokio::time::sleep(Duration::from_millis(50)).await;

        bus.publish(book(11, fa));
        bus.publish(book(22, fb));
        bus.publish(book(33, fa));

        let mut got: Vec<(u64, Figi)> = Vec::new();
        for _ in 0..3 {
            let b = tokio::time::timeout(Duration::from_millis(500), sub.next())
                .await
                .expect("timeout: fan-in merge of 3 messages timed out")
                .expect("subscription closed");
            got.push((b.gateway_seq, b.figi));
        }
        got.sort_by_key(|t| t.0);

        assert_eq!(
            got,
            vec![(11, fa), (22, fb), (33, fa)],
            "multiset must equal the set of three publishes (order irrelevant)"
        );
        assert_eq!(
            sub.dropped_total(),
            0,
            "happy path must have zero loss (rules out false positive of loss masking the bug)"
        );
    }

    // -----------------------------------------------------------------------
    // Section 3: isolation (slow / disconnected subscribers do not affect others)
    // -----------------------------------------------------------------------

    /// Guards "a slow subscriber does not affect a fast subscriber". Two
    /// subscriptions on the same FIGI: `fast` reads in a tight loop, `slow`
    /// sleeps 50ms after every receive. The publisher sends N messages at a
    /// steady 10ms/message rate. Expected: `fast` receives nearly all
    /// messages with `dropped≈0`; `slow` drops most messages with
    /// `dropped>0`.
    ///
    /// Complementary to the E2E version in
    /// `tests/grpc_slow_consumer.rs`: this test proves the Bus logic is
    /// correct in isolation; the E2E version proves the wire path holds
    /// up too.
    ///
    /// # Key design choice: the publisher sleeps between messages instead of using a tight loop
    ///
    /// A tight loop would saturate the broadcast ring (`cap=8`)
    /// instantaneously → even fast would lag → the fast/slow contrast is
    /// lost. 10ms/message is far below fast's processing speed but well
    /// above slow's 50ms/message, naturally separating "fast" from "slow".
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_consumer_isolation() {
        let bus = Arc::new(Bus::new(8));
        let f = figi("BBG000000001");

        let mut fast = bus.subscribe(&[f], 256);
        let mut slow = bus.subscribe(&[f], 4); // small queue, easy to fill

        // Wait for the fan-in tasks to register their broadcast::Receivers
        // and reach `recv().await`.
        tokio::time::sleep(Duration::from_millis(80)).await;

        const TOTAL: u64 = 30;
        let pub_bus = bus.clone();
        let publisher = tokio::spawn(async move {
            for seq in 1..=TOTAL {
                pub_bus.publish(book(seq, f));
                // 10ms/message → fast keeps up easily, slow (50ms/message)
                // cannot. Under Windows tokio timer jitter, 10ms is a
                // relatively safe interval.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // Fast: deadline is publisher-end + a 500ms drain window.
        let fast_task = tokio::spawn(async move {
            let mut got = 0u64;
            let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
            while tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(200), fast.next()).await {
                    Ok(Some(_)) => got += 1,
                    _ => break, // timeout = publisher finished and queue drained
                }
            }
            (got, fast.dropped_total())
        });

        // Slow: sleeps 50ms after every receive (5x slower than the publish rate).
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

        // Tolerate a handful of fast losses (initial broadcast registration
        // race + scheduling noise). What we are really testing is that
        // "slow does not affect fast" — fast is not back-pressured by the
        // slow path.
        assert!(
            fast_got >= TOTAL - 3,
            "fast should receive almost all (≥{} of {TOTAL}), actual {fast_got}",
            TOTAL - 3
        );
        assert!(
            fast_dropped <= 3,
            "fast consumer should have near-zero loss (tolerate ≤3 scheduling noise), actual {fast_dropped}"
        );
        assert!(
            slow_dropped > 0,
            "slow consumer must report dropped > 0 (otherwise pressure is insufficient and the test is invalid), actual {slow_dropped}"
        );
        assert!(
            slow_got < fast_got,
            "slow path receives must be strictly fewer than fast path: slow={slow_got} fast={fast_got}"
        );
    }

    /// `slow_consumer_isolation` verifies "slow" isolation; this test
    /// independently verifies "disconnected" isolation — after one
    /// subscriber is actively `drop`ped, the other subscriber still
    /// receives all updates with `dropped_total = 0`.
    ///
    /// # Lifecycle of a disconnected subscriber
    ///
    /// 1. `drop(_to_drop)` → `Subscription` is dropped → its internal
    ///    `mpsc::Receiver` is dropped.
    /// 2. The corresponding `fan_in_one` task is still parked on
    ///    `broadcast::Receiver::recv().await` until the next publish
    ///    wakes it → `try_send` returns `Closed` → the task returns.
    /// 3. The `broadcast::Sender` for that subscriber is still held by
    ///    the senders table (the entry does not shrink — see
    ///    `senders_entry_persists_after_all_subscribers_dropped`), but
    ///    `receiver_count` decreases by 1. Subsequent publishes are
    ///    still fine; they just push one fewer copy.
    ///
    /// # Assertion focus
    ///
    /// - `survivor` receives all 3 messages (no drops).
    /// - `survivor.dropped_total() == 0` (not polluted by the other
    ///   subscriber's disconnect).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disconnected_subscriber_does_not_stall_others() {
        let bus = Arc::new(Bus::new(16));
        let f = figi("BBG000000001");

        let mut survivor = bus.subscribe(&[f], 16);
        let to_drop = bus.subscribe(&[f], 16);

        // Wait for both fan_in_one tasks to reach `recv().await`.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Actively disconnect one subscriber. Note: the disconnected
        // subscriber's fan_in_one task is still awaiting, and only really
        // exits after the next publish — this test only cares about
        // "whether survivor is affected".
        drop(to_drop);

        bus.publish(book(1, f));
        bus.publish(book(2, f));
        bus.publish(book(3, f));

        let mut got = Vec::new();
        for _ in 0..3 {
            let b = tokio::time::timeout(Duration::from_millis(500), survivor.next())
                .await
                .expect("timeout: survivor missed a message")
                .expect("survivor subscription closed");
            got.push(b.gateway_seq);
        }

        assert_eq!(got, vec![1, 2, 3], "survivor must receive all three messages in order");
        assert_eq!(
            survivor.dropped_total(),
            0,
            "disconnecting a sibling subscriber must not cause any loss for survivor, actual {}",
            survivor.dropped_total()
        );
    }

    // -----------------------------------------------------------------------
    // Section 4: dropped_total cumulative semantics
    // -----------------------------------------------------------------------

    /// `dropped_counter` exposes an `Arc<AtomicU64>`. This test verifies
    /// that between two consecutive observations the value is
    /// **monotonically non-decreasing** — the client can only compute lag
    /// by diffing cumulative values.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropped_total_is_cumulative_not_delta() {
        let bus = Arc::new(Bus::new(4));
        let f = figi("BBG000000001");

        let mut sub = bus.subscribe(&[f], 2); // tiny queue, guaranteed to fill
        let counter = sub.dropped_counter();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Do not consume; tight-loop publish → broadcast must lag + mpsc
        // must fill → drops are guaranteed.
        for seq in 1..=100u64 {
            bus.publish(book(seq, f));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let snap1 = counter.load(Ordering::Relaxed);
        assert!(snap1 > 0, "expected dropped > 0, actual {snap1}");

        // Draining one message must not roll back the counter.
        let _ = tokio::time::timeout(Duration::from_millis(50), sub.next()).await;
        let snap2 = counter.load(Ordering::Relaxed);
        assert!(
            snap2 >= snap1,
            "cumulative value must never decrease: snap1={snap1} snap2={snap2}"
        );

        // Publish another batch; the counter must keep growing.
        for seq in 101..=200u64 {
            bus.publish(book(seq, f));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let snap3 = counter.load(Ordering::Relaxed);
        assert!(
            snap3 > snap2,
            "cumulative value must grow after another batch: snap2={snap2} snap3={snap3}"
        );
    }

    // -----------------------------------------------------------------------
    // Section 5: subscribe semantics (from-now / sender lifecycle)
    // -----------------------------------------------------------------------

    /// Pins down "from-now semantics" — `Bus::publish` takes a get-only
    /// path; with no subscribers no `broadcast::Sender` is ever created,
    /// and messages published before subscription enter neither the ring
    /// buffer nor any persistent store.
    ///
    /// # Design trade-off
    ///
    /// This is an explicit "no history / no replay" decision:
    ///
    /// - Pro: the ingest hot path carries zero history state, preserving
    ///   the "never blocks" invariant.
    /// - Trade-off: a client wanting the latest message at subscribe time
    ///   must call `GetSnapshots` (R/R API) before/after subscribing.
    ///
    /// # Contract guarded
    ///
    /// - `seq=1, 2` published before subscribing **must not** appear in
    ///   `sub.next()`.
    /// - `seq=99` published after subscribing **must** be received.
    /// - A second `next()` must time out (no replay whatsoever).
    #[tokio::test]
    async fn messages_before_subscribe_are_not_replayed() {
        let bus = Arc::new(Bus::new(16));
        let f = figi("BBG000000001");

        // With no subscribers, publish → senders.get(&f) = None → return
        // immediately; no broadcast::Sender is created and no message is
        // buffered.
        bus.publish(book(1, f));
        bus.publish(book(2, f));

        let mut sub = bus.subscribe(&[f], 16);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The message published after subscribing must be received.
        bus.publish(book(99, f));

        let got = tokio::time::timeout(Duration::from_millis(500), sub.next())
            .await
            .expect("timeout: a publish after subscribing should have been received")
            .expect("subscription closed");
        assert_eq!(
            got.gateway_seq, 99,
            "from-now semantics: first message must be the seq=99 published after subscribing (the pre-subscribe 1, 2 must not be replayed)"
        );

        // The second next must time out — no replay of seq=1, 2.
        let extra = tokio::time::timeout(Duration::from_millis(100), sub.next()).await;
        assert!(
            extra.is_err(),
            "from-now semantics: publishes before subscribing must not enter the stream, actual {extra:?}"
        );
    }

    /// Pins down the known behavior: **the senders table entry is not
    /// automatically cleared when subscribers unsubscribe**.
    ///
    /// `Bus::subscribe` calls `or_insert_with` to create a sender when the
    /// entry is missing, but **no code path** removes the entry from
    /// `senders` after the last subscriber leaves. Under long-running load
    /// with a continuous flow of unique FIGIs, `senders` size grows
    /// **monotonically**, and memory is not reclaimed.
    ///
    /// # Why pin the current behavior as a test
    ///
    /// 1. **Explicit alarm point**: if anyone later adds entry shrink
    ///    logic (e.g. removing the entry inside `publish` when
    ///    `tx.receiver_count() == 0`), this test fails and the reviewer
    ///    immediately knows the behavior has changed and consciously
    ///    walks through the doc-update process.
    /// 2. Surfaces a known boundary proactively rather than leaving the
    ///    reviewer to discover it.
    ///
    /// # Boundary and mitigation
    ///
    /// - **Scale**: each entry ≈ `Figi (12B) + broadcast::Sender (~tens
    ///   of bytes)`; the issue only matters at millions of unique FIGIs.
    ///   Real exchange FIGI counts are < 100k, so this is not a
    ///   bottleneck for take-home scenarios.
    /// - **Mitigation candidates** (if this ever becomes a real concern):
    ///   - A periodic GC task scans `senders` and removes entries with
    ///     `receiver_count == 0`.
    ///   - When the last receiver leaves `fan_in_one`, actively call
    ///     `senders.remove(&figi)`, but that requires passing `figi`
    ///     through and changing the `fan_in_one` signature.
    #[tokio::test]
    async fn senders_entry_persists_after_all_subscribers_dropped() {
        let bus = Bus::new(16);
        let f = figi("BBG000000001");

        assert_eq!(bus.senders.len(), 0, "initially no entries");

        {
            let _sub = bus.subscribe(&[f], 16);
            assert_eq!(
                bus.senders.len(),
                1,
                "subscribe triggers or_insert_with → entry=1"
            );
        }
        // _sub goes out of scope → mpsc::Receiver is dropped — but the
        // fan_in_one task is still parked on broadcast::recv().await.

        // Trigger a publish so fan_in_one reaches the try_send(Closed)
        // branch and exits.
        bus.publish(book(1, f));
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Even though fan_in_one has exited and the broadcast has no
        // receivers, the entry in senders is **not** automatically
        // removed — this is the current trade-off, pinned by this test.
        assert_eq!(
            bus.senders.len(),
            1,
            "after all subscribers leave the entry does not shrink → over the long run, unique FIGI count = senders size"
        );

        // Publish once more to verify the noop (get-only path; the
        // SendError is swallowed by `let _ =`).
        bus.publish(book(2, f));
        assert_eq!(bus.senders.len(), 1, "publish neither creates a new entry nor removes the old one");
    }
}
