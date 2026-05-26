//! Phase 3 测试专用的可控 [`Upstream`] 实现。
//!
//! # 与 feed-sim 的对比
//!
//! | 维度 | `FeedSimUpstream` | `MockUpstream` |
//! |---|---|---|
//! | 数据生成方式 | 内部背景线程按 `rate_hz` 节奏生成 | 测试通过 [`MockHandle::push`] 显式注入 |
//! | 速率控制 | env / `SubscriberConfig::rate_hz` | 测试代码全权决定 |
//! | 终止信号 | `max_messages` cap 或 drop | [`MockHandle::close`] |
//! | 决定性 | 同 seed 可重现 | 100% 决定性（无 RNG / 无背景线程） |
//! | `wait` 响应延迟 | poll 间隔 | condvar 唤醒（push / close 即时 wake） |
//!
//! # 为什么不复用 `feed-sim`
//!
//! `feed-sim` 是黑盒上游（README "What's provided"），无法注入"恰好 seq=1,2,5"
//! 这种 gap 序列；也无法在测试期间精确控制 ingest 看到的消息数。Mock 让测试
//! 既快又精确。
//!
//! # 为什么 publish API 用 [`MockHandle`] 而非 `&MockUpstream`
//!
//! `MockUpstream` 被 `move` 进 ingest 线程后，测试持有的是 [`MockHandle`]
//! 别名（内部 `Arc<Inner>` clone）。这样 ingest / 测试两边各拿自己的句柄，
//! 互不干扰；`MockUpstream::new()` 构造时同时返回两端。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use marketdata_types::BookMessage;

use super::Upstream;

// ---------------------------------------------------------------------------
// Shared inner state
// ---------------------------------------------------------------------------

struct Inner {
    /// 待消费消息队列；MockHandle::push 入队，Upstream::receive 出队。
    queue: Mutex<VecDeque<BookMessage>>,
    /// `Upstream::wait` 阻塞在此；push / close 时被 notify。
    cv: Condvar,
    /// `MockHandle::close()` 设为 true，标志"上游永久结束"。
    closed: AtomicBool,
    /// 累计 push 数（成功入队的，对应 Upstream::total_generated）。
    total: AtomicU64,
}

// ---------------------------------------------------------------------------
// MockUpstream
// ---------------------------------------------------------------------------

/// 测试用 [`Upstream`] 实作。配套 [`MockHandle`] 控制数据流。
///
/// 典型用法：
///
/// ```ignore
/// let (upstream, handle) = MockUpstream::new();
/// let service = Service::new_with_upstream(cfg, upstream)?;
/// handle.push(make_book(figi, 1));
/// handle.push(make_book(figi, 2));
/// handle.close();      // 让 ingest 自然 EOF
/// ```
pub struct MockUpstream {
    inner: Arc<Inner>,
}

impl MockUpstream {
    /// 构造一对 (`MockUpstream`, `MockHandle`)：前者 move 进 service / ingest,
    /// 后者由测试持有以控制数据流。
    pub fn new() -> (Self, MockHandle) {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            closed: AtomicBool::new(false),
            total: AtomicU64::new(0),
        });
        (
            Self {
                inner: inner.clone(),
            },
            MockHandle { inner },
        )
    }
}

impl Upstream for MockUpstream {
    fn receive(&self) -> anyhow::Result<Option<BookMessage>> {
        Ok(self.inner.queue.lock().unwrap().pop_front())
    }

    /// 阻塞最多 `duration`；提前唤醒条件：push / close。
    ///
    /// 返回 `Err(())` 仅当 closed **且** queue 排空 —— 与 feed-sim 的
    /// "唯一合法的结束信号" 语义一致（GUIDELINE §3.2）。
    fn wait(&self, duration: Duration) -> Result<(), ()> {
        let inner = &*self.inner;
        let guard = inner.queue.lock().unwrap();

        if !guard.is_empty() {
            return Ok(());
        }
        if inner.closed.load(Ordering::Acquire) {
            return Err(());
        }

        // queue 空 + 未 close → 在 condvar 上等待 push / close 或超时。
        let (guard, _timeout) = inner.cv.wait_timeout(guard, duration).unwrap();

        if guard.is_empty() && inner.closed.load(Ordering::Acquire) {
            Err(())
        } else {
            // 任一情况都让外层回去 try receive：要么有数据，要么 spurious wakeup。
            Ok(())
        }
    }

    fn total_generated(&self) -> u64 {
        self.inner.total.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// MockHandle
// ---------------------------------------------------------------------------

/// 测试侧句柄：往 [`MockUpstream`] 推消息、关闭上游。
///
/// `Clone` 可分发给多个生产者任务（典型场景：subscribe 期间另起 tokio task
/// 持续 push）。
#[derive(Clone)]
pub struct MockHandle {
    inner: Arc<Inner>,
}

impl MockHandle {
    /// 入队一笔。每次唤醒一个等待中的 `wait` 调用。
    pub fn push(&self, book: BookMessage) {
        self.inner.queue.lock().unwrap().push_back(book);
        self.inner.total.fetch_add(1, Ordering::Relaxed);
        self.inner.cv.notify_one();
    }

    /// 标志上游永久结束。一旦 queue 排空，下一个 `wait` 返回 `Err(())`。
    ///
    /// 唤醒所有 wait（队列可能仍有数据，让 ingest 把剩余 drain 干净）。
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.cv.notify_all();
    }

    /// 累计 push 笔数。
    #[allow(dead_code)]
    pub fn total_pushed(&self) -> u64 {
        self.inner.total.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Test helpers (production-visible, intentional)
// ---------------------------------------------------------------------------

/// 构造一个最小有效 [`BookMessage`]，供测试快速生成数据流。
///
/// `figi` 长度 > 12 字符会截断（`Figi::from_str` 的语义）。
pub fn make_book(figi: &str, gateway_seq: u64) -> BookMessage {
    let mut m = BookMessage::default();
    m.figi = figi.parse().expect("Figi::from_str is Infallible");
    m.gateway_seq = gateway_seq;
    m
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn push_then_receive_in_fifo_order() {
        let (up, h) = MockUpstream::new();
        h.push(make_book("BBG000000001", 1));
        h.push(make_book("BBG000000001", 2));

        assert_eq!(up.receive().unwrap().unwrap().gateway_seq, 1);
        assert_eq!(up.receive().unwrap().unwrap().gateway_seq, 2);
        assert!(up.receive().unwrap().is_none());
    }

    #[test]
    fn wait_returns_err_after_close_and_drain() {
        let (up, h) = MockUpstream::new();
        h.push(make_book("BBG000000001", 1));
        h.close();

        // 有数据 → Ok
        assert!(up.wait(Duration::from_millis(50)).is_ok());
        let _ = up.receive().unwrap();
        // 排空 + closed → Err
        assert!(up.wait(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn wait_wakes_up_on_push() {
        let (up, h) = MockUpstream::new();
        let start = Instant::now();
        let pusher = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            h.push(make_book("BBG000000001", 1));
        });

        // 即使 wait 给了 1s 超时，push 后 20ms 内就应该被唤醒。
        let res = up.wait(Duration::from_secs(1));
        let elapsed = start.elapsed();
        assert!(res.is_ok());
        assert!(
            elapsed < Duration::from_millis(200),
            "wait should wake on push, took {elapsed:?}"
        );
        pusher.join().unwrap();
    }

    #[test]
    fn total_generated_tracks_pushes() {
        let (up, h) = MockUpstream::new();
        for seq in 1..=5 {
            h.push(make_book("BBG000000001", seq));
        }
        assert_eq!(up.total_generated(), 5);
        assert_eq!(h.total_pushed(), 5);
    }
}
