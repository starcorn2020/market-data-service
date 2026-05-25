use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use marketdata_types::BookMessage;

use crate::config::{Pacing, SubscriberConfig};
use crate::error::{FeedSimError, Result};
use crate::generator::Generator;
use crate::sample::FeedSample;

/// Drop-in stand-in for an iceoryx2 subscriber.
///
/// On construction, spawns a background thread that paces a deterministic
/// stream of `BookMessage` values into a bounded internal buffer. The
/// public API is intentionally identical to what you'd write against a real
/// iceoryx2 subscriber so swapping later is mechanical:
///
/// - [`receive`](Self::receive) is non-blocking and returns `Ok(Some)` /
///   `Ok(None)`.
/// - [`wait`](Self::wait) sleeps and signals shutdown via `Err(())`. It is
///   the canonical end-of-stream signal — `receive()` itself never
///   reports it.
/// - The returned [`FeedSample`] derefs to `&BookMessage`.
///
/// Backpressure: when the internal buffer is full the simulator drops the
/// in-flight message rather than blocking the generator — same behavior
/// as a real subscriber that has fallen behind its publisher.
pub struct FeedSubscriber {
    rx: Receiver<BookMessage>,
    // Held purely to keep the channel "connected" for the lifetime of the
    // subscriber. Without this, `try_recv` would start returning
    // `Disconnected` as soon as the generator thread exits and drops its
    // own sender, racing with `wait()` (which is supposed to be the single
    // shutdown signal) and panicking any caller that uses `receive()?` or
    // `receive().unwrap()`.
    _keep_alive_tx: SyncSender<BookMessage>,
    stop: Arc<AtomicBool>,
    bg_running: Arc<AtomicBool>,
    sent: Arc<AtomicU64>,
    received: AtomicU64,
    handle: Option<JoinHandle<()>>,
}

impl FeedSubscriber {
    /// Starts the simulator. Spawns one background generator thread.
    pub fn new(config: SubscriberConfig) -> Result<Self> {
        config.validate()?;

        let (tx, rx) = sync_channel::<BookMessage>(config.buffer_size);
        let tx_for_generator = tx.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let bg_running = Arc::new(AtomicBool::new(true));
        let sent = Arc::new(AtomicU64::new(0));

        let stop_t = stop.clone();
        let bg_running_t = bg_running.clone();
        let sent_t = sent.clone();

        let handle = thread::Builder::new()
            .name("feed-sim-generator".into())
            .spawn(move || {
                run_generator(config, tx_for_generator, stop_t, sent_t);
                bg_running_t.store(false, Ordering::Release);
            })
            .map_err(|e| FeedSimError::Spawn(e.to_string()))?;

        Ok(Self {
            rx,
            _keep_alive_tx: tx,
            stop,
            bg_running,
            sent,
            received: AtomicU64::new(0),
            handle: Some(handle),
        })
    }

    /// Returns the next sample, or `Ok(None)` if the buffer is currently
    /// empty (which also covers "upstream finished and buffer drained" —
    /// use [`wait`](Self::wait) to detect shutdown).
    pub fn receive(&self) -> Result<Option<FeedSample>> {
        match self.rx.try_recv() {
            Ok(msg) => {
                self.received.fetch_add(1, Ordering::Release);
                Ok(Some(FeedSample::new(msg)))
            }
            Err(TryRecvError::Empty) => Ok(None),
            // Unreachable while `_keep_alive_tx` is held, but kept defensive
            // so a stray future change can't reintroduce the panic.
            Err(TryRecvError::Disconnected) => Ok(None),
        }
    }

    /// Sleeps for `duration`, then returns `Ok(())` if the subscriber should
    /// keep polling, or `Err(())` if the upstream is gone and there's
    /// nothing left to drain. This mirrors the shutdown signal from the
    /// production subscriber's `wait`.
    pub fn wait(&self, duration: Duration) -> std::result::Result<(), ()> {
        thread::sleep(duration);
        let upstream_done = !self.bg_running.load(Ordering::Acquire);
        let drained =
            self.received.load(Ordering::Acquire) >= self.sent.load(Ordering::Acquire);
        if upstream_done && drained {
            Err(())
        } else {
            Ok(())
        }
    }

    /// Returns the total number of messages successfully buffered by the
    /// generator since the simulator started. Differs from "messages
    /// received by the consumer" by the number of in-flight + dropped
    /// samples — useful for sanity checks in tests.
    pub fn total_generated(&self) -> u64 {
        self.sent.load(Ordering::Acquire)
    }
}

impl Drop for FeedSubscriber {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn run_generator(
    config: SubscriberConfig,
    tx: SyncSender<BookMessage>,
    stop: Arc<AtomicBool>,
    sent: Arc<AtomicU64>,
) {
    let max = config.max_messages;
    let mut pacer = Pacer::new(&config);
    let mut generator = Generator::new(config);
    let mut emitted: u64 = 0;

    while !stop.load(Ordering::Relaxed) {
        if let Some(cap) = max
            && emitted >= cap
        {
            break;
        }
        pacer.next_tick(&stop);
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let msg = generator.next();
        match tx.try_send(msg) {
            Ok(()) => {
                sent.fetch_add(1, Ordering::Release);
                emitted += 1;
            }
            Err(TrySendError::Full(_)) => {
                // Subscriber is too slow — drop the message. Mirrors what a
                // real subscriber does on buffer overflow.
            }
            Err(TrySendError::Disconnected(_)) => break,
        }
    }
    // SyncSender drops here; channel transitions to Disconnected once empty.
}

/// Inter-arrival pacer.
struct Pacer {
    pacing: Pacing,
    rate_hz: u32,
    /// Time origin of the next steady-rate slot (or burst boundary).
    next_slot: Instant,
    /// Messages remaining in the current burst (Bursty mode only).
    burst_remaining: u32,
}

impl Pacer {
    fn new(config: &SubscriberConfig) -> Self {
        Self {
            pacing: config.pacing,
            rate_hz: config.rate_hz,
            next_slot: Instant::now(),
            burst_remaining: 0,
        }
    }

    /// Sleeps as needed before the next message should be emitted. Returns
    /// early (without sleeping) if `stop` becomes true mid-sleep.
    fn next_tick(&mut self, stop: &AtomicBool) {
        match self.pacing {
            Pacing::Steady => {
                let interval = Duration::from_nanos(1_000_000_000 / self.rate_hz as u64);
                let target = self.next_slot;
                sleep_until_or_stop(target, stop);
                self.next_slot = target + interval;
                let now = Instant::now();
                // If we fell badly behind (e.g. consumer paused us), don't
                // try to "catch up" — restart the pacing window from now.
                if now > self.next_slot + Duration::from_secs(1) {
                    self.next_slot = now + interval;
                }
            }
            Pacing::Bursty { burst_size } => {
                if self.burst_remaining > 0 {
                    self.burst_remaining -= 1;
                    return;
                }
                // Compute when the next burst starts so the long-run rate
                // matches `rate_hz`.
                let burst_period_ns =
                    1_000_000_000_u64 * burst_size as u64 / self.rate_hz as u64;
                let target = self.next_slot;
                sleep_until_or_stop(target, stop);
                self.next_slot = target + Duration::from_nanos(burst_period_ns);
                self.burst_remaining = burst_size.saturating_sub(1);
                let now = Instant::now();
                if now > self.next_slot + Duration::from_secs(1) {
                    self.next_slot = now + Duration::from_nanos(burst_period_ns);
                }
            }
        }
    }
}

/// Sleeps until `target`, waking early if `stop` becomes true. Sleeps in
/// short slices so shutdown remains responsive.
fn sleep_until_or_stop(target: Instant, stop: &AtomicBool) {
    let max_slice = Duration::from_millis(50);
    loop {
        let now = Instant::now();
        if now >= target {
            return;
        }
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let remaining = target - now;
        thread::sleep(remaining.min(max_slice));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(rate_hz: u32, max: Option<u64>) -> SubscriberConfig {
        SubscriberConfig {
            instruments: 4,
            rate_hz,
            pacing: Pacing::Steady,
            depth: 3,
            max_messages: max,
            seed: 99,
            start_seq: 1,
            buffer_size: 64,
        }
    }

    #[test]
    fn produces_messages() {
        let sub = FeedSubscriber::new(cfg(10_000, Some(50))).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut count = 0;
        while Instant::now() < deadline {
            if sub.wait(Duration::from_millis(10)).is_err() {
                break;
            }
            while let Some(s) = sub.receive().unwrap() {
                let _ = s.gateway_seq;
                count += 1;
            }
        }
        // Final drain after wait signals done.
        while let Ok(Some(_)) = sub.receive() {
            count += 1;
        }
        assert_eq!(count, 50);
    }

    #[test]
    fn wait_signals_done_after_max_messages() {
        let sub = FeedSubscriber::new(cfg(50_000, Some(20))).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut got_done = false;
        let mut count = 0;
        while Instant::now() < deadline {
            match sub.wait(Duration::from_millis(20)) {
                Ok(()) => {
                    while let Some(_s) = sub.receive().unwrap() {
                        count += 1;
                    }
                }
                Err(()) => {
                    got_done = true;
                    break;
                }
            }
        }
        assert!(got_done, "wait should have signalled done");
        assert_eq!(count, 20);
    }

    #[test]
    fn drop_stops_generator_quickly() {
        // Run forever, then drop and ensure the thread joins promptly.
        let sub = FeedSubscriber::new(cfg(1_000_000, None)).unwrap();
        thread::sleep(Duration::from_millis(50));
        let started_drop = Instant::now();
        drop(sub);
        assert!(
            started_drop.elapsed() < Duration::from_millis(500),
            "drop took too long: {:?}",
            started_drop.elapsed()
        );
    }

    #[test]
    fn slow_consumer_does_not_block_generator() {
        // Tiny buffer, fast generator, no draining → generator must drop
        // and keep running, not hang.
        let mut c = cfg(100_000, Some(10_000));
        c.buffer_size = 4;
        let sub = FeedSubscriber::new(c).unwrap();
        thread::sleep(Duration::from_millis(200));
        let total = sub.total_generated();
        // We almost certainly buffered < 10_000 (we'd need more time at
        // 100k/s) but generator should have made progress, capped by
        // pacing or the cap.
        assert!(total > 0, "generator made no progress");
    }
}
