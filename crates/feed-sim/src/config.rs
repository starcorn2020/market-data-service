use std::str::FromStr;

use marketdata_types::MAX_BOOK_DEPTH;

use crate::error::{FeedSimError, Result};

/// Pacing pattern for the simulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pacing {
    /// Steady inter-arrival of `1/rate_hz` seconds.
    Steady,
    /// Emit `burst_size` messages back-to-back, then sleep so the long-run
    /// rate matches `rate_hz`.
    Bursty {
        /// Number of messages emitted in each burst.
        burst_size: u32,
    },
}

/// Configuration for [`FeedSubscriber`](crate::FeedSubscriber).
#[derive(Debug, Clone)]
pub struct SubscriberConfig {
    /// Number of distinct instruments (FIGIs) to simulate.
    pub instruments: u32,

    /// Target message rate across all instruments, in messages per second.
    pub rate_hz: u32,

    /// Pacing pattern.
    pub pacing: Pacing,

    /// Number of populated book levels per message (`1..=MAX_BOOK_DEPTH`).
    pub depth: u8,

    /// Cap on total messages generated. `None` means run forever.
    pub max_messages: Option<u64>,

    /// Seed for the deterministic message stream.
    pub seed: u64,

    /// Initial value for `gateway_seq`.
    pub start_seq: u64,

    /// Capacity of the internal bounded buffer between the generator thread
    /// and [`FeedSubscriber::receive`](crate::FeedSubscriber::receive). When
    /// full, the simulator drops the oldest in-flight message — mirroring
    /// the behavior of a real subscriber that has fallen behind its
    /// publisher's buffer.
    pub buffer_size: usize,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            instruments: 100,
            rate_hz: 1_000,
            pacing: Pacing::Steady,
            depth: 5,
            max_messages: None,
            seed: 0xDEAD_BEEF_CAFE_F00D,
            start_seq: 1,
            buffer_size: 1024,
        }
    }
}

impl SubscriberConfig {
    /// Loads configuration from environment variables, falling back to
    /// [`Default`]. Recognised variables:
    ///
    /// | Variable | Type | Default | Notes |
    /// |---|---|---|---|
    /// | `SIM_INSTRUMENTS` | `u32` | `100` | Number of distinct FIGIs. |
    /// | `SIM_RATE_HZ` | `u32` | `1000` | Total msgs/sec across all FIGIs. |
    /// | `SIM_DEPTH` | `u8` | `5` | Book levels per message (1..=10). |
    /// | `SIM_MAX_MESSAGES` | `u64` | _unset_ | Stop after N messages. |
    /// | `SIM_SEED` | `u64` | fixed | Deterministic stream seed. |
    /// | `SIM_START_SEQ` | `u64` | `1` | Initial `gateway_seq`. |
    /// | `SIM_BUFFER_SIZE` | `usize` | `1024` | Internal buffer capacity. |
    /// | `SIM_PACING` | string | `steady` | `steady` or `bursty:<N>`. |
    pub fn from_env() -> Result<Self> {
        let mut cfg = Self::default();
        if let Some(v) = parse_env::<u32>("SIM_INSTRUMENTS")? {
            cfg.instruments = v;
        }
        if let Some(v) = parse_env::<u32>("SIM_RATE_HZ")? {
            cfg.rate_hz = v;
        }
        if let Some(v) = parse_env::<u8>("SIM_DEPTH")? {
            cfg.depth = v;
        }
        if let Some(v) = parse_env::<u64>("SIM_MAX_MESSAGES")? {
            cfg.max_messages = Some(v);
        }
        if let Some(v) = parse_env::<u64>("SIM_SEED")? {
            cfg.seed = v;
        }
        if let Some(v) = parse_env::<u64>("SIM_START_SEQ")? {
            cfg.start_seq = v;
        }
        if let Some(v) = parse_env::<usize>("SIM_BUFFER_SIZE")? {
            cfg.buffer_size = v;
        }
        if let Ok(s) = std::env::var("SIM_PACING") {
            cfg.pacing = parse_pacing(&s)?;
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the configuration. Called automatically by
    /// [`FeedSubscriber::new`](crate::FeedSubscriber::new); also useful for
    /// fail-fast configs assembled in code.
    pub fn validate(&self) -> Result<()> {
        if self.instruments == 0 {
            return Err(FeedSimError::Config("instruments must be > 0".into()));
        }
        if self.rate_hz == 0 {
            return Err(FeedSimError::Config("rate_hz must be > 0".into()));
        }
        if self.depth == 0 || self.depth as usize > MAX_BOOK_DEPTH {
            return Err(FeedSimError::Config(format!(
                "depth must be in 1..={MAX_BOOK_DEPTH} (got {})",
                self.depth
            )));
        }
        if self.buffer_size == 0 {
            return Err(FeedSimError::Config("buffer_size must be > 0".into()));
        }
        if let Pacing::Bursty { burst_size } = self.pacing
            && burst_size == 0
        {
            return Err(FeedSimError::Config(
                "Pacing::Bursty.burst_size must be > 0".into(),
            ));
        }
        Ok(())
    }
}

fn parse_env<T>(name: &str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(s) => s
            .parse::<T>()
            .map(Some)
            .map_err(|e| FeedSimError::Config(format!("invalid {name}={s:?}: {e}"))),
    }
}

fn parse_pacing(s: &str) -> Result<Pacing> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("steady") {
        return Ok(Pacing::Steady);
    }
    if let Some(n) = s.strip_prefix("bursty:") {
        let burst_size = n.parse::<u32>().map_err(|e| {
            FeedSimError::Config(format!(
                "invalid SIM_PACING bursty:N — N must be a positive integer (got {n:?}): {e}"
            ))
        })?;
        return Ok(Pacing::Bursty { burst_size });
    }
    Err(FeedSimError::Config(format!(
        "SIM_PACING must be 'steady' or 'bursty:N' (got {s:?})"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        SubscriberConfig::default().validate().unwrap();
    }

    #[test]
    fn rejects_zero_rate() {
        let mut cfg = SubscriberConfig::default();
        cfg.rate_hz = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_depth() {
        let mut cfg = SubscriberConfig::default();
        cfg.depth = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_excessive_depth() {
        let mut cfg = SubscriberConfig::default();
        cfg.depth = (MAX_BOOK_DEPTH + 1) as u8;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn parses_pacing_steady() {
        assert_eq!(parse_pacing("steady").unwrap(), Pacing::Steady);
        assert_eq!(parse_pacing("STEADY").unwrap(), Pacing::Steady);
    }

    #[test]
    fn parses_pacing_bursty() {
        assert_eq!(
            parse_pacing("bursty:32").unwrap(),
            Pacing::Bursty { burst_size: 32 }
        );
    }

    #[test]
    fn rejects_bad_pacing() {
        assert!(parse_pacing("garbage").is_err());
        assert!(parse_pacing("bursty:abc").is_err());
    }
}
