//! Shared helpers for integration tests.
//!
//! Each `tests/*.rs` file is compiled by cargo as an independent test
//! binary; this file is pulled in by each test via `mod common;` to avoid
//! boilerplate duplication.
//!
//! # Design principles
//!
//! - **Fully deterministic**: the upstream is `MockUpstream`; the test
//!   code controls exactly when each message enters ingest, avoiding the
//!   nondeterministic timing of feed-sim's background thread.
//! - **Dynamic port**: `listen_addr = 127.0.0.1:0`; the OS assigns a
//!   port, so tests run in parallel without conflicts.
//! - **Explicit shutdown**: `RunningService::shutdown()` must be awaited,
//!   otherwise the ingest `std::thread` may not be reaped until after
//!   the runtime is dropped, triggering a cleanup race.

#![allow(dead_code)] // Different test files use different helpers; suppress warnings on unused ones.

use std::time::Duration;

use marketdata_service::pb::market_data_client::MarketDataClient;
use marketdata_service::{
    BoxError, MockHandle, MockUpstream, RunningService, Service, ServiceConfig, UpstreamConfig,
};
use tonic::transport::Channel;

/// Default test config: listen on `127.0.0.1:0`, low-latency poll, small
/// capacities to make boundary cases easy to trigger.
///
/// # Capacity choice (64 / 32)
///
/// Much smaller than the production default (1024 / 1024).
/// `grpc_basic.rs` is a low-rate scenario (a handful of pushes) and does
/// not hit capacity boundaries → equivalent to defaults; **the small
/// capacity is kept** so future wire-level boundary tests do not need a
/// new config. `grpc_slow_consumer.rs` uses a more aggressive custom
/// config (overrides `bus_channel_capacity` / `subscriber_queue_size`).
pub fn test_config() -> ServiceConfig {
    ServiceConfig {
        upstream: UpstreamConfig::default(), // Unused (MockUpstream goes through new_with_upstream).
        poll_interval: Duration::from_millis(5),
        bus_channel_capacity: 64,
        subscriber_queue_size: 32,
        progress_log_every: 0, // Disable progress logs during tests.
        listen_addr: "127.0.0.1:0".parse().expect("hardcoded addr valid"),
    }
}

/// Start a test Service driven by a [`MockUpstream`].
///
/// Returns `(running, mock_handle)`:
/// - `running.addr()` is the actual listen address (OS-assigned).
/// - `mock_handle.push(book)` injects a message;
///   `mock_handle.close()` lets ingest reach natural EOF.
/// - The test **must** call `running.shutdown().await` before exiting to
///   prevent thread leaks.
pub async fn spawn_service(
    cfg: ServiceConfig,
) -> Result<(RunningService, MockHandle), BoxError> {
    let (upstream, handle) = MockUpstream::new();
    let service = Service::new_with_upstream(cfg, upstream)?;
    let running = service.start().await?;
    Ok((running, handle))
}

/// Start with the default config (suitable for most tests).
pub async fn spawn_default_service() -> Result<(RunningService, MockHandle), BoxError> {
    spawn_service(test_config()).await
}

/// Build a gRPC client targeting the server's actual address.
///
/// `tonic::transport::Channel::from_shared` accepts a `String` URL; we
/// assemble `http://127.0.0.1:<port>` here.
pub async fn make_client(
    addr: std::net::SocketAddr,
) -> Result<MarketDataClient<Channel>, BoxError> {
    let url = format!("http://{addr}");
    let channel = Channel::from_shared(url)?
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await?;
    Ok(MarketDataClient::new(channel))
}

/// Wait until ingest has drained the messages pushed via `mock_handle`
/// into the snapshot table.
///
/// Refuses `sleep`-based magic — actively polls `running.snapshot_len()`
/// until it reaches the target, otherwise panics on timeout (so CI fails
/// explicitly rather than silently flaking).
pub async fn wait_for_snapshot_len(
    running: &RunningService,
    target: usize,
    timeout: Duration,
) -> Result<(), BoxError> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if running.snapshot_len() >= target {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(format!(
        "snapshot did not reach len={target} within {timeout:?} (actual={})",
        running.snapshot_len()
    )
    .into())
}
