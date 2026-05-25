//! Deterministic message generator.
//!
//! Given a fixed [`SubscriberConfig`], the same seed produces the same sequence
//! of [`BookMessage`] payloads (modulo `gateway_ts`, which uses wall-clock).
//! Prices oscillate per-instrument so consumers see continuous, plausible
//! book updates rather than constant snapshots.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use marketdata_types::{
    BookFlags, BookLevel, BookMessage, ConditionFlags, ExchangeHeader, Figi, MAX_BOOK_DEPTH,
};

use crate::config::SubscriberConfig;
use crate::rng::Rng;

/// `ENTER | REVISE | CANCEL | TRADING` — normal trading state.
const NORMAL_TRADING: ConditionFlags = ConditionFlags::from_bits_truncate(
    ConditionFlags::ENTER.bits()
        | ConditionFlags::REVISE.bits()
        | ConditionFlags::CANCEL.bits()
        | ConditionFlags::TRADING.bits(),
);

pub(crate) struct Generator {
    config: SubscriberConfig,
    rng: Rng,
    start: Instant,
    figis: Vec<Figi>,
    base_prices: Vec<f64>,
    per_figi_packet_seq: Vec<u32>,
    per_figi_exchange_seq: Vec<u32>,
    next_index: usize,
    next_seq: u64,
}

impl Generator {
    pub fn new(config: SubscriberConfig) -> Self {
        let n = config.instruments as usize;
        let figis: Vec<Figi> = (0..n)
            .map(|i| {
                // BBG + 9 digits = 12 ASCII chars exactly.
                let s = format!("BBG{:09}", i + 1);
                s.parse().expect("FromStr for Figi is infallible")
            })
            .collect();
        // Spread base prices across [100.0, 1100.0) so each instrument is
        // visually distinguishable in logs without overflowing realistic
        // ranges for futures.
        let base_prices: Vec<f64> = (0..n)
            .map(|i| 100.0 + (i as f64) * (1000.0 / n as f64))
            .collect();
        Self {
            next_seq: config.start_seq,
            rng: Rng::new(config.seed),
            start: Instant::now(),
            per_figi_packet_seq: vec![1; n],
            per_figi_exchange_seq: vec![1; n],
            next_index: 0,
            figis,
            base_prices,
            config,
        }
    }

    /// Generates the next [`BookMessage`].
    pub fn next(&mut self) -> BookMessage {
        let i = self.next_index;
        self.next_index = (self.next_index + 1) % self.figis.len();

        let elapsed = self.start.elapsed().as_secs_f64();
        let base = self.base_prices[i];
        // Per-instrument oscillation frequency so they don't all move in lockstep.
        let freq = 0.1 + (i as f64) * 0.0137;
        let mid = base + (elapsed * freq).sin() * base * 0.001;
        let spread = base * 0.0001;
        let tick = spread / 4.0;

        let depth = self.config.depth.min(MAX_BOOK_DEPTH as u8) as usize;
        let mut bids = [BookLevel::default(); MAX_BOOK_DEPTH];
        let mut asks = [BookLevel::default(); MAX_BOOK_DEPTH];
        for k in 0..depth {
            bids[k] = BookLevel {
                price: mid - spread / 2.0 - (k as f64) * tick,
                qty: 50.0 + self.rng.next_in_range(200) as f32,
                orders: 1 + self.rng.next_in_range(10) as u16,
            };
            asks[k] = BookLevel {
                price: mid + spread / 2.0 + (k as f64) * tick,
                qty: 50.0 + self.rng.next_in_range(200) as f32,
                orders: 1 + self.rng.next_in_range(10) as u16,
            };
        }

        let gateway_seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        let packet_seq = self.per_figi_packet_seq[i];
        self.per_figi_packet_seq[i] = packet_seq.wrapping_add(1);
        let exchange_seq = self.per_figi_exchange_seq[i];
        self.per_figi_exchange_seq[i] = exchange_seq.wrapping_add(1);

        BookMessage {
            gateway_seq,
            gateway_ts: now_ns,
            packet_seq,
            header: ExchangeHeader {
                time_ns: now_ns,
                seq: exchange_seq,
            },
            contract_id: 1_000_000 + i as u32,
            figi: self.figis[i],
            book_flags: BookFlags::empty(),
            condition_flags: NORMAL_TRADING,
            bid_count: depth as u8,
            ask_count: depth as u8,
            bids,
            asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Pacing;

    fn cfg(seed: u64) -> SubscriberConfig {
        SubscriberConfig {
            instruments: 4,
            rate_hz: 1_000,
            pacing: Pacing::Steady,
            depth: 3,
            max_messages: None,
            seed,
            start_seq: 1,
            buffer_size: 16,
        }
    }

    #[test]
    fn deterministic_payloads_for_same_seed() {
        let mut a = Generator::new(cfg(42));
        let mut b = Generator::new(cfg(42));
        for _ in 0..16 {
            let ma = a.next();
            let mb = b.next();
            // gateway_ts uses wall-clock; everything else should match.
            assert_eq!(ma.gateway_seq, mb.gateway_seq);
            assert_eq!(ma.figi, mb.figi);
            assert_eq!(ma.contract_id, mb.contract_id);
            assert_eq!(ma.packet_seq, mb.packet_seq);
            assert_eq!(ma.bid_count, mb.bid_count);
            assert_eq!(ma.ask_count, mb.ask_count);
            for k in 0..ma.bid_count as usize {
                assert_eq!(ma.bids[k].qty, mb.bids[k].qty);
                assert_eq!(ma.bids[k].orders, mb.bids[k].orders);
            }
        }
    }

    #[test]
    fn round_robins_through_instruments() {
        let mut g = Generator::new(cfg(1));
        let figis: Vec<_> = (0..8).map(|_| g.next().figi).collect();
        assert_eq!(figis[0], figis[4]);
        assert_eq!(figis[1], figis[5]);
        assert_ne!(figis[0], figis[1]);
    }

    #[test]
    fn monotonic_gateway_seq() {
        let mut g = Generator::new(cfg(1));
        let seqs: Vec<_> = (0..8).map(|_| g.next().gateway_seq).collect();
        for w in seqs.windows(2) {
            assert_eq!(w[1], w[0] + 1);
        }
    }
}
