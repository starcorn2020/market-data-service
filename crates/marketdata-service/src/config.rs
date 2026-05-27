//! Service-level configuration.
//!
//! # Design notes
//!
//! - [`UpstreamConfig`] is a plain struct **owned by this crate**; it does
//!   **not** directly reuse `feed_sim::SubscriberConfig`. This seals the
//!   feed-sim type boundary inside the `upstream::feed_sim` module: when
//!   we swap to a real iceoryx2 in the future, only the fields of this
//!   struct need to be redefined; [`ServiceConfig`]'s public shape and
//!   the wire schema change by zero.
//!
//! - The mapping from [`UpstreamConfig`] → `SubscriberConfig` is a `From`
//!   impl used **only inside the service crate** (`upstream::feed_sim`),
//!   never crossing the crate boundary.
//!
//! - Environment variable naming: upstream fields directly reuse the
//!   `SIM_*` names already used by feed-sim (so a reviewer running
//!   feed-sim's own demo is not confused); fields introduced by this
//!   crate use the `MDS_` prefix.

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use feed_sim::{Pacing, SubscriberConfig};

use crate::BoxError;

// ---------------------------------------------------------------------------
// UpstreamConfig
// ---------------------------------------------------------------------------

/// Upstream feed configuration (a service-crate-owned type; **does not
/// leak `feed_sim::*`**).
#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    /// Number of simulated FIGIs.
    pub instruments: u32,

    /// Aggregate target rate across all FIGIs (msg/s).
    pub rate_hz: u32,

    /// Book depth per message (1..=10).
    pub depth: u8,

    /// Cumulative message cap. `None` = unlimited.
    pub max_messages: Option<u64>,

    /// Deterministic RNG seed; fixing it makes the stream reproducible.
    pub seed: u64,

    /// Starting value of `gateway_seq`.
    pub start_seq: u64,

    /// Internal upstream buffer capacity; when full the oldest entries
    /// are dropped (slow-consumer semantics).
    pub buffer_size: usize,

    /// `None` => steady pacing; `Some(n)` => bursty:n.
    pub burst_size: Option<u32>,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            instruments: 100,
            rate_hz: 1_000,
            depth: 5,
            max_messages: None,
            seed: 0xDEAD_BEEF_CAFE_F00D,
            start_seq: 1,
            buffer_size: 1024,
            burst_size: None,
        }
    }
}

// For service-crate internal use only (`upstream::feed_sim::FeedSimUpstream::new`).
// The single place in the entire crate that constructs `feed_sim::SubscriberConfig` —
// the sealing point that keeps the boundary type from leaking.
impl From<UpstreamConfig> for SubscriberConfig {
    fn from(c: UpstreamConfig) -> Self {
        SubscriberConfig {
            instruments: c.instruments,
            rate_hz: c.rate_hz,
            pacing: match c.burst_size {
                None => Pacing::Steady,
                Some(n) => Pacing::Bursty { burst_size: n },
            },
            depth: c.depth,
            max_messages: c.max_messages,
            seed: c.seed,
            start_seq: c.start_seq,
            buffer_size: c.buffer_size,
        }
    }
}

// ---------------------------------------------------------------------------
// ServiceConfig
// ---------------------------------------------------------------------------

/// Top-level startup configuration for the service.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Upstream feed configuration.
    pub upstream: UpstreamConfig,

    /// Poll interval for the ingest thread's `Upstream::wait`. Too long
    /// → shutdown signal is delayed; too short → busy-looping. 50ms is
    /// a conservative default.
    pub poll_interval: Duration,

    /// Per-FIGI `tokio::sync::broadcast` capacity. When full, broadcast
    /// automatically drops the oldest entry; the subscriber's next
    /// `recv` returns `Lagged(n)`, and the `Bus` fan-in task
    /// accumulates this into `dropped_total`.
    pub bus_channel_capacity: usize,

    /// Per-subscriber fan-in mpsc capacity. The gRPC handler reuses
    /// this channel for the wire stage as well; full = drop + increment
    /// `dropped_total`.
    pub subscriber_queue_size: usize,

    /// Ingest prints a progress line to stderr every N messages (0 =
    /// off). Demo output only.
    pub progress_log_every: u64,

    /// gRPC server listen address. Default `0.0.0.0:50051`, suitable for
    /// both same-host and cross-host use.
    ///
    /// **Do not** change this to `127.0.0.1:50051` — that breaks LAN
    /// clients and violates the assignment's "Works for clients on the
    /// same host and on a remote machine".
    pub listen_addr: SocketAddr,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            upstream: UpstreamConfig::default(),
            poll_interval: Duration::from_millis(50),
            bus_channel_capacity: 1024,
            subscriber_queue_size: 1024,
            progress_log_every: 100,
            // 0.0.0.0 rather than 127.0.0.1 — cross-host connections are
            // a hard assignment requirement.
            listen_addr: "0.0.0.0:50051".parse().expect("hardcoded addr valid"),
        }
    }
}

impl ServiceConfig {
    /// Load configuration from environment variables.
    ///
    /// | Env | Field | Default |
    /// |---|---|---|
    /// | `SIM_INSTRUMENTS` | `upstream.instruments` | 100 |
    /// | `SIM_RATE_HZ` | `upstream.rate_hz` | 1000 |
    /// | `SIM_DEPTH` | `upstream.depth` | 5 |
    /// | `SIM_MAX_MESSAGES` | `upstream.max_messages` | unlimited |
    /// | `SIM_SEED` | `upstream.seed` | fixed |
    /// | `SIM_START_SEQ` | `upstream.start_seq` | 1 |
    /// | `SIM_BUFFER_SIZE` | `upstream.buffer_size` | 1024 |
    /// | `SIM_PACING` | `upstream.burst_size` | steady |
    /// | `MDS_POLL_INTERVAL_MS` | `poll_interval` | 50 |
    /// | `MDS_BUS_CAPACITY` | `bus_channel_capacity` | 1024 |
    /// | `MDS_SUBSCRIBER_QUEUE` | `subscriber_queue_size` | 1024 |
    /// | `MDS_PROGRESS_EVERY` | `progress_log_every` | 100 |
    /// | `MDS_LISTEN` | `listen_addr` | `0.0.0.0:50051` |
    pub fn from_env() -> Result<Self, BoxError> {
        let mut cfg = Self::default();
        let u = &mut cfg.upstream;

        if let Some(v) = parse_env::<u32>("SIM_INSTRUMENTS")? {
            u.instruments = v;
        }
        if let Some(v) = parse_env::<u32>("SIM_RATE_HZ")? {
            u.rate_hz = v;
        }
        if let Some(v) = parse_env::<u8>("SIM_DEPTH")? {
            u.depth = v;
        }
        if let Some(v) = parse_env::<u64>("SIM_MAX_MESSAGES")? {
            u.max_messages = Some(v);
        }
        if let Some(v) = parse_env::<u64>("SIM_SEED")? {
            u.seed = v;
        }
        if let Some(v) = parse_env::<u64>("SIM_START_SEQ")? {
            u.start_seq = v;
        }
        if let Some(v) = parse_env::<usize>("SIM_BUFFER_SIZE")? {
            u.buffer_size = v;
        }
        if let Ok(s) = std::env::var("SIM_PACING") {
            u.burst_size = parse_pacing(&s)?;
        }

        if let Some(v) = parse_env::<u64>("MDS_POLL_INTERVAL_MS")? {
            cfg.poll_interval = Duration::from_millis(v);
        }
        if let Some(v) = parse_env::<usize>("MDS_BUS_CAPACITY")? {
            cfg.bus_channel_capacity = v;
        }
        if let Some(v) = parse_env::<usize>("MDS_SUBSCRIBER_QUEUE")? {
            cfg.subscriber_queue_size = v;
        }
        if let Some(v) = parse_env::<u64>("MDS_PROGRESS_EVERY")? {
            cfg.progress_log_every = v;
        }
        if let Ok(s) = std::env::var("MDS_LISTEN") {
            cfg.listen_addr = s
                .parse()
                .map_err(|e| -> BoxError { format!("invalid MDS_LISTEN={s:?}: {e}").into() })?;
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate this layer's invariants. feed-sim validates again on its
    /// own (fail-fast in two layers of defense).
    pub fn validate(&self) -> Result<(), BoxError> {
        if self.bus_channel_capacity == 0 {
            return Err("bus_channel_capacity must be > 0".into());
        }
        if self.subscriber_queue_size == 0 {
            return Err("subscriber_queue_size must be > 0".into());
        }
        if self.poll_interval.is_zero() {
            return Err("poll_interval must be > 0 (avoid busy-loop)".into());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn parse_env<T>(name: &str) -> Result<Option<T>, BoxError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(s) => s
            .parse::<T>()
            .map(Some)
            .map_err(|e| -> BoxError { format!("invalid {name}={s:?}: {e}").into() }),
    }
}

/// `steady` / `bursty:N` (compatible with feed-sim's SIM_PACING).
fn parse_pacing(s: &str) -> Result<Option<u32>, BoxError> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("steady") {
        return Ok(None);
    }
    if let Some(n) = s.strip_prefix("bursty:") {
        let burst_size = n.parse::<u32>().map_err(|e| -> BoxError {
            format!("invalid SIM_PACING bursty:N (got {n:?}): {e}").into()
        })?;
        return Ok(Some(burst_size));
    }
    Err(format!("SIM_PACING must be 'steady' or 'bursty:N' (got {s:?})").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        ServiceConfig::default().validate().unwrap();
    }

    #[test]
    fn rejects_zero_bus_capacity() {
        let cfg = ServiceConfig {
            bus_channel_capacity: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_poll_interval() {
        let cfg = ServiceConfig {
            poll_interval: Duration::ZERO,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    /// Symmetric coverage with `bus_channel_capacity` / `poll_interval`,
    /// avoiding the silent gap where "validate gained a check but no
    /// test guards it".
    #[test]
    fn rejects_zero_subscriber_queue_size() {
        let cfg = ServiceConfig {
            subscriber_queue_size: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn upstream_config_maps_to_subscriber_config() {
        let u = UpstreamConfig {
            burst_size: Some(32),
            ..Default::default()
        };
        let sc: SubscriberConfig = u.into();
        assert!(matches!(sc.pacing, Pacing::Bursty { burst_size: 32 }));
    }

    #[test]
    fn parses_pacing() {
        assert_eq!(parse_pacing("steady").unwrap(), None);
        assert_eq!(parse_pacing("bursty:16").unwrap(), Some(16));
        assert!(parse_pacing("garbage").is_err());
    }
}
