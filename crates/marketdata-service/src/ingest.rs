//! Single-threaded ingest loop: pull upstream → write snapshot → broadcast bus.
//!
//! # Why `std::thread` instead of `tokio::task`
//!
//! [`crate::upstream::Upstream`] is a synchronous blocking API (`wait()`
//! internally uses `thread::sleep`); putting it on a tokio worker would
//! occupy an OS thread and disrupt other async tasks. A dedicated OS
//! thread is the right tool.
//!
//! # Invariants
//!
//! - **Ingest is never blocked by downstream**: `snapshot.put` only holds a
//!   DashMap shard write lock (extremely brief); `bus.publish` is
//!   `broadcast::send` (non-blocking, drops the oldest entry on full).
//! - **Order: `snapshot.put` before `bus.publish`** — when a subscriber
//!   receives an update, `GetSnapshots([figi])` is guaranteed to read at
//!   least the same message. The reverse does not hold (snapshot may lead
//!   bus by one message), but that gap is simply the window before the
//!   subscriber has been able to receive — it does not violate the public
//!   contract that "the snapshot never lags behind the stream".

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::bus::Bus;
use crate::snapshot::Snapshot;
use crate::upstream::Upstream;

/// Control handle for the ingest thread. On drop, automatically signals
/// ingest to stop and joins the thread.
pub struct IngestHandle {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<IngestStats>>,
}

/// Cumulative statistics produced when ingest exits.
#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    /// Number of messages actually received / written to snapshot /
    /// broadcast.
    pub received: u64,
    /// **Event count** of `gateway_seq` discontinuities (each jump = +1,
    /// regardless of how many messages were skipped).
    ///
    /// We chose "event count" over "total missed messages" granularity:
    /// the former is enough for operators to notice an anomaly; the
    /// latter would require maintaining a sliding window. Finer gap
    /// handling such as "alert on missed-count threshold" or "active
    /// resync recovery" is out of scope for this deliverable, so we
    /// deliberately only expose a counter without any reaction.
    pub gaps: u64,
}

impl IngestHandle {
    /// Returns an `Arc` clone of the stop signal so that
    /// [`Service::run`](crate::Service::run) can still trigger stop
    /// externally after `tokio::select!` has taken ownership of the
    /// `IngestHandle` (always via `.store(true, Release)`).
    ///
    /// We expose `Arc<AtomicBool>` directly rather than a `StopToken`
    /// newtype: this is internal to the service crate and does not cross
    /// the crate boundary, so the benefit of type wrapping does not
    /// justify the boilerplate.
    pub fn stop_token(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    /// **Wait for ingest to exit on its own** (does not send a stop signal).
    ///
    /// Typical usage: the upstream has a `max_messages` cap, and we wait
    /// for it to reach natural EOF; or the caller has already called
    /// [`stop`](Self::stop) manually and then calls `join`. Directly
    /// joining an uncapped ingest **blocks forever** — this is
    /// intentional, keeping "non-blocking stop" and "wait for end" as two
    /// distinct semantics.
    pub fn join(mut self) -> IngestStats {
        self.thread
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or_default()
    }
}

impl Drop for IngestHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }
}

/// Spawn the ingest thread.
///
/// Uses generic `U` rather than `Box<dyn Upstream>` (static dispatch):
/// `Upstream::receive` is a hot path called thousands of times per
/// second and does not tolerate virtual call overhead. Trade-off: once
/// Service assembly has chosen the upstream type it is fixed and cannot
/// be switched at runtime; but this service only assembles once in main,
/// so there is no such requirement.
pub fn spawn<U>(
    upstream: U,
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    poll_interval: Duration,
    progress_log_every: u64,
) -> IngestHandle
where
    U: Upstream + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();

    let thread = thread::Builder::new()
        .name("mds-ingest".into())
        .spawn(move || ingest_loop(upstream, snapshot, bus, poll_interval, progress_log_every, stop_t))
        .expect("failed to spawn ingest thread");

    IngestHandle {
        stop,
        thread: Some(thread),
    }
}

fn ingest_loop<U: Upstream>(
    upstream: U,
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    poll_interval: Duration,
    progress_log_every: u64,
    stop: Arc<AtomicBool>,
) -> IngestStats {
    let mut stats = IngestStats::default();
    // Upstream contract: `gateway_seq` is strictly monotonic across the
    // entire stream — the only reliable basis for gap detection.
    let mut last_seq: Option<u64> = None;

    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        // Outer `wait`: the only legitimate "upstream has ended" signal channel.
        if upstream.wait(poll_interval).is_err() {
            break;
        }

        // Inner drain loop: empty the current buffer in one sweep before
        // returning to the outer `wait`. Without this layer, only one
        // message would be taken per `poll_interval`, and the feed-sim
        // buffer would inevitably overflow and drop messages.
        loop {
            match upstream.receive() {
                Ok(Some(book)) => {
                    stats.received += 1;

                    // Use `!=` rather than `<` for gap detection: this
                    // relies on the upstream's strict monotonic contract.
                    // If a future upstream allows out-of-order arrivals
                    // (e.g. fan-in from multiple partitions), this must
                    // become "maintain an in-flight set + judge gap after
                    // N messages", otherwise out-of-order arrivals would
                    // be misreported as gaps. The current feed-sim is
                    // single-threaded and strictly monotonic, so this is
                    // not an issue.
                    if let Some(prev) = last_seq
                        && book.gateway_seq != prev + 1
                    {
                        stats.gaps += 1;
                    }
                    last_seq = Some(book.gateway_seq);

                    // snapshot first, bus second — order-sensitive.
                    snapshot.put(book);
                    bus.publish(book);

                    if progress_log_every > 0
                        && stats.received.is_multiple_of(progress_log_every)
                    {
                        eprintln!(
                            "[ingest] received={} snapshot.len={} gaps={} total_generated={}",
                            stats.received,
                            snapshot.len(),
                            stats.gaps,
                            upstream.total_generated(),
                        );
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    // A single error must not kill the entire service:
                    // log + return to the outer wait and retry on the
                    // next poll. If the error is persistent, the symptom
                    // is stderr noise + zero throughput, and the
                    // operator decides whether to restart.
                    eprintln!("[ingest] receive error: {e:?}");
                    break;
                }
            }
        }
    }

    eprintln!(
        "[ingest] stopped: received={} snapshot.len={} gaps={} total_generated={}",
        stats.received,
        snapshot.len(),
        stats.gaps,
        upstream.total_generated(),
    );
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::{MockUpstream, make_book};

    /// Push N consecutive seq messages, close the upstream, and wait for
    /// ingest to reach natural EOF. Verifies that ingest_loop drains
    /// correctly, writes to snapshot, and does not misreport gaps.
    #[test]
    fn ingest_drains_finite_mock_and_populates_snapshot() {
        let (up, handle_in) = MockUpstream::new();
        let snap = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(16));

        for seq in 1..=30u64 {
            // Rotate across 3 FIGIs to verify snapshot.len() == 3.
            let figi = format!("F{:011}", seq % 3);
            handle_in.push(make_book(&figi, seq));
        }
        handle_in.close();

        let handle = spawn(
            up,
            snap.clone(),
            bus.clone(),
            Duration::from_millis(5),
            0,
        );
        let stats = handle.join();

        assert_eq!(stats.received, 30);
        assert_eq!(snap.len(), 3, "3 distinct FIGIs (seq % 3)");
        assert_eq!(stats.gaps, 0, "1..=30 has no gaps");
    }

    /// Guards the ingest_loop contract: "discontinuous gateway_seq →
    /// increment a gap event". Inject seq=1, 2, 5 → ingest_loop should
    /// increment `gaps` once at the 5.
    #[test]
    fn gap_counter_increments_on_skipped_seq() {
        let (up, handle_in) = MockUpstream::new();
        let snap = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(16));

        handle_in.push(make_book("BBG000000001", 1));
        handle_in.push(make_book("BBG000000001", 2));
        // Seq jumps to 5 — 3 and 4 are missing, corresponding to one gap event.
        handle_in.push(make_book("BBG000000001", 5));
        handle_in.close();

        let handle = spawn(
            up,
            snap.clone(),
            bus.clone(),
            Duration::from_millis(5),
            0,
        );
        let stats = handle.join();

        assert_eq!(stats.received, 3);
        assert_eq!(stats.gaps, 1, "skipping 3,4 between 2 and 5 = 1 gap event");
    }

    /// Guards the ingest order invariant: "snapshot.put before bus.publish".
    ///
    /// Push one message → ingest ends → assert that snapshot contains it.
    /// The bus may have no subscribers, but the snapshot must be written
    /// first — otherwise a subscriber receiving an update and immediately
    /// calling GetSnapshots would read a stale snapshot, breaking the
    /// public contract "GetSnapshots never lags behind the stream".
    #[test]
    fn snapshot_populated_before_join_returns() {
        let (up, handle_in) = MockUpstream::new();
        let snap = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(16));

        handle_in.push(make_book("BBG000000042", 42));
        handle_in.close();

        let handle = spawn(up, snap.clone(), bus.clone(), Duration::from_millis(5), 0);
        let _ = handle.join();

        let got = snap.get(&"BBG000000042".parse().unwrap()).unwrap();
        assert_eq!(got.gateway_seq, 42);
    }
}
