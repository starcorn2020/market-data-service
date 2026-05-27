use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use marketdata_types::BookMessage;

use crate::BoxError;

use super::Upstream;

// ---------------------------------------------------------------------------
// Shared inner state
// ---------------------------------------------------------------------------

struct Inner {
    /// Pending-message queue; MockHandle::push enqueues, Upstream::receive dequeues.
    queue: Mutex<VecDeque<BookMessage>>,
    /// `Upstream::wait` blocks here; push / close notifies.
    cv: Condvar,
    /// Set to true by `MockHandle::close()`, signaling "upstream has ended permanently".
    closed: AtomicBool,
    /// Cumulative successful pushes (matches Upstream::total_generated).
    total: AtomicU64,
}

// ---------------------------------------------------------------------------
// MockUpstream
// ---------------------------------------------------------------------------

/// Test-only [`Upstream`] implementation. Paired with [`MockHandle`] for
/// data-flow control.
///
/// Typical usage:
///
/// ```ignore
/// let (upstream, handle) = MockUpstream::new();
/// let service = Service::new_with_upstream(cfg, upstream)?;
/// handle.push(make_book(figi, 1));
/// handle.push(make_book(figi, 2));
/// handle.close();      // let ingest reach natural EOF
/// ```
pub struct MockUpstream {
    inner: Arc<Inner>,
}

impl MockUpstream {
    /// Construct a (`MockUpstream`, `MockHandle`) pair: the former is
    /// moved into service / ingest, the latter is retained by the test
    /// to control the data flow.
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
    fn receive(&self) -> Result<Option<BookMessage>, BoxError> {
        Ok(self.inner.queue.lock().unwrap().pop_front())
    }

    /// Blocks for at most `duration`; wakes early on push / close.
    ///
    /// Returns `Err(())` only when closed **and** queue is drained —
    /// matches feed-sim's "the only legitimate end signal" semantics,
    /// so the ingest_loop EOF decision behaves identically on the mock
    /// path and the real upstream path.
    fn wait(&self, duration: Duration) -> Result<(), ()> {
        let inner = &*self.inner;
        let guard = inner.queue.lock().unwrap();

        if !guard.is_empty() {
            return Ok(());
        }
        if inner.closed.load(Ordering::Acquire) {
            return Err(());
        }

        // Queue is empty and not closed → wait on the condvar for
        // push / close or timeout.
        let (guard, _timeout) = inner.cv.wait_timeout(guard, duration).unwrap();

        if guard.is_empty() && inner.closed.load(Ordering::Acquire) {
            Err(())
        } else {
            // Either case sends the caller back to try a receive: either
            // there is data, or this was a spurious wakeup.
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

/// Test-side handle: push messages into [`MockUpstream`] and close the upstream.
///
/// `Clone`able for distribution to multiple producer tasks (typical
/// scenario: a separate tokio task that keeps pushing during a
/// subscribe).
#[derive(Clone)]
pub struct MockHandle {
    inner: Arc<Inner>,
}

impl MockHandle {
    /// Enqueue one message. Each call wakes one waiting `wait` call.
    pub fn push(&self, book: BookMessage) {
        self.inner.queue.lock().unwrap().push_back(book);
        self.inner.total.fetch_add(1, Ordering::Relaxed);
        self.inner.cv.notify_one();
    }

    /// Signal that the upstream has ended permanently. Once the queue
    /// drains, the next `wait` returns `Err(())`.
    ///
    /// Wakes all waits (the queue may still have data, so ingest can
    /// drain the remainder).
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.cv.notify_all();
    }
}

#[cfg(test)]
impl MockHandle {
    /// Test-only helper: cumulative push count, used to cross-check
    /// against `Upstream::total_generated()`.
    pub(crate) fn total_pushed(&self) -> u64 {
        self.inner.total.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Test helpers (production-visible, intentional)
// ---------------------------------------------------------------------------

/// Build a minimally valid [`BookMessage`] for tests to quickly produce a
/// data flow.
///
/// `figi` longer than 12 characters is truncated (per `Figi::from_str`
/// semantics).
pub fn make_book(figi: &str, gateway_seq: u64) -> BookMessage {
    BookMessage {
        figi: figi.parse().expect("Figi::from_str is Infallible"),
        gateway_seq,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    //! Correctness tests for `MockUpstream` itself ("tests for the test").
    //!
    //! Contracts guarded (1:1 with the implementation semantics of the
    //! [`super::Upstream`] trait):
    //!
    //! | Test | Contract |
    //! |---|---|
    //! | `push_then_receive_in_fifo_order` | `receive` is FIFO and returns `Ok(None)` after drain |
    //! | `wait_returns_err_after_close_and_drain` | Matches feed-sim: only returns `Err(())` when **closed and queue is empty** — the only legitimate EOF signal |
    //! | `wait_wakes_up_on_push` | condvar wakeup works: woken within ≤200ms of push, **not** poll-sleep in disguise |
    //! | `total_generated_tracks_pushes` | Cumulative count equals push count (matches `Upstream::total_generated`) |

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

        // Has data → Ok
        assert!(up.wait(Duration::from_millis(50)).is_ok());
        let _ = up.receive().unwrap();
        // Drained + closed → Err
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

        // Even though wait has a 1s timeout, push should wake it within ~20ms.
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
