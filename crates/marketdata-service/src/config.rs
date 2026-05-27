//! 服务级配置。
//!
//! # 设计要点
//!
//! - [`UpstreamConfig`] 是 service crate **自己拥有**的 plain struct，**不**直接复用
//!   `feed_sim::SubscriberConfig`。这是 GUIDELINE I4 不变量的核心抓手：未来把
//!   `feed-sim` 换成真实 iceoryx2 时，本 struct 的字段会跟着重定义，
//!   但 [`ServiceConfig`] 的对外 shape 保持不动，调用方与 wire schema 0 改动。
//!
//! - [`UpstreamConfig`] 到 `SubscriberConfig` 的映射是 `From` impl，
//!   **仅 service crate 内部**（`upstream::feed_sim`）使用，不出 crate 边界。
//!
//! - 环境变量命名：Phase 1 直接读 feed-sim 既有的 `SIM_*`（reviewer 跑 README
//!   时不困惑）；未来若抽 `marketdata-protocol` 子 crate，统一改 `MDS_UPSTREAM_*`。

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use feed_sim::{Pacing, SubscriberConfig};

use crate::BoxError;

// ---------------------------------------------------------------------------
// UpstreamConfig
// ---------------------------------------------------------------------------

/// 上游 feed 配置（service crate 自有型别，**不洩漏 `feed_sim::*`**）。
#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    /// 模拟 FIGI 数量。
    pub instruments: u32,

    /// 跨所有 FIGI 的总目标速率 (msg/s)。
    pub rate_hz: u32,

    /// 每条 message 的盘口檔位数 (1..=10)。
    pub depth: u8,

    /// 累计 message 上限。`None` = 无限。
    pub max_messages: Option<u64>,

    /// 决定性 RNG 种子；固定后流可重现。
    pub seed: u64,

    /// `gateway_seq` 起始值。
    pub start_seq: u64,

    /// 上游内部 buffer 容量；满了会丢最旧（GUIDELINE §3.4 slow consumer 语义）。
    pub buffer_size: usize,

    /// `None` => steady pacing；`Some(n)` => bursty:n。
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

// 仅供 service crate 内部使用（`upstream::feed_sim::FeedSimUpstream::new`）。
// 这条 From 是 I4 的"密封点"——所有 `feed_sim::SubscriberConfig` 的构造都
// 收敛到这一处。
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

/// 整个 service 的启动配置。
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// 上游 feed 配置。
    pub upstream: UpstreamConfig,

    /// Ingest 线程对 `Upstream::wait` 的 poll 间隔。
    /// 太长 → 关闭信号延迟；太短 → 空转。50ms 是 GUIDELINE §3.3 示范值。
    pub poll_interval: Duration,

    /// 每个 FIGI 的 `tokio::sync::broadcast` 容量。
    /// 满了 broadcast 自动丢最旧，订阅者下次 `recv` 会拿到 `Lagged(n)`，
    /// 由 `Bus` 的 fan-in task 累进 `dropped_total`。
    pub bus_channel_capacity: usize,

    /// 每个订阅者 fan-in mpsc 的容量（Phase 2 gRPC handler 会用到）。
    pub subscriber_queue_size: usize,

    /// Ingest 每 N 笔在 stderr 打一次进度（0 = 关）。Phase 1 demo 用。
    pub progress_log_every: u64,

    /// gRPC server 监听地址。默认 `0.0.0.0:50051`（同主机 + 跨主机两用，README §5）。
    ///
    /// **不要**改成 `127.0.0.1:50051`：那样 LAN client 连不通，违反 README §5
    /// "Works for clients on the same host and on a remote machine"。
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
            // 0.0.0.0 而非 127.0.0.1 —— 跨主机支持是 README §5 硬要求。
            listen_addr: "0.0.0.0:50051".parse().expect("hardcoded addr valid"),
        }
    }
}

impl ServiceConfig {
    /// 从环境变量加载。
    ///
    /// | Env | 字段 | 默认 |
    /// |---|---|---|
    /// | `SIM_INSTRUMENTS` | `upstream.instruments` | 100 |
    /// | `SIM_RATE_HZ` | `upstream.rate_hz` | 1000 |
    /// | `SIM_DEPTH` | `upstream.depth` | 5 |
    /// | `SIM_MAX_MESSAGES` | `upstream.max_messages` | 无限 |
    /// | `SIM_SEED` | `upstream.seed` | 固定 |
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

    /// 校验本层不变量。feed-sim 自己也会再校验一次（fail-fast 两道防线）。
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

/// `steady` / `bursty:N`（与 feed-sim 的 SIM_PACING 兼容）。
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

    /// `validate()` 的第三条 check(subscriber_queue_size > 0)与
    /// bus_channel_capacity / poll_interval 对称覆盖,避免「漏一个 check 没测试
    /// 守护」的 silent gap。
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
