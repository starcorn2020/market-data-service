//! 集成测试共享 helper。
//!
//! 每个 `tests/*.rs` 文件被 cargo 当作独立的测试二进制;本文件通过
//! `mod common;` 被各测试包含, 避免重复样板。
//!
//! # 设计原则
//!
//! - **完全确定性**:上游用 `MockUpstream`, 由测试代码控制每一笔何时进入
//!   ingest, 避免 feed-sim 背景线程带来的不确定时序。
//! - **动态端口**:`listen_addr = 127.0.0.1:0`, OS 分配端口, 测试并行无冲突。
//! - **显式 shutdown**:`RunningService::shutdown()` 必须 await, 否则 ingest
//!   `std::thread` 可能在 runtime drop 之后才回收, 引发 cleanup race。

#![allow(dead_code)] // 不同测试文件用不同 helper，未使用的不报警。

use std::time::Duration;

use marketdata_service::pb::market_data_client::MarketDataClient;
use marketdata_service::{
    BoxError, MockHandle, MockUpstream, RunningService, Service, ServiceConfig, UpstreamConfig,
};
use tonic::transport::Channel;

/// 默认测试 config:listen `127.0.0.1:0`、低延迟 poll、小容量便于触发边界。
///
/// # 容量选择 (64 / 32)
///
/// 远小于 production default (1024 / 1024)。`grpc_basic.rs` 都是低速场景
/// (几笔 push), 不会触发 capacity 边界 → 与 default 等价;**保留小容量**
/// 是为未来添加 wire-level 边界测试时不需要再换 config。
/// `grpc_slow_consumer.rs` 走自定义更激进的配置 (overrides `bus_channel_capacity`
/// / `subscriber_queue_size`)。
pub fn test_config() -> ServiceConfig {
    ServiceConfig {
        upstream: UpstreamConfig::default(), // 不会用到（MockUpstream 走 new_with_upstream）
        poll_interval: Duration::from_millis(5),
        bus_channel_capacity: 64,
        subscriber_queue_size: 32,
        progress_log_every: 0, // 测试期间禁用进度 log
        listen_addr: "127.0.0.1:0".parse().expect("hardcoded addr valid"),
    }
}

/// 启动一个用 [`MockUpstream`] 驱动的测试 Service。
///
/// 返回 `(running, mock_handle)`：
/// - `running.addr()` 是实际监听地址（OS 分配）。
/// - `mock_handle.push(book)` 注入消息；`mock_handle.close()` 让 ingest 自然结束。
/// - 测试结束前**必须** `running.shutdown().await` 防止线程泄漏。
pub async fn spawn_service(
    cfg: ServiceConfig,
) -> Result<(RunningService, MockHandle), BoxError> {
    let (upstream, handle) = MockUpstream::new();
    let service = Service::new_with_upstream(cfg, upstream)?;
    let running = service.start().await?;
    Ok((running, handle))
}

/// 启动 + 用默认 config（适合大部分测试）。
pub async fn spawn_default_service() -> Result<(RunningService, MockHandle), BoxError> {
    spawn_service(test_config()).await
}

/// 用 server 的实际地址构造一个 gRPC client。
///
/// `tonic::transport::Channel::from_shared` 接受 `String` URL，
/// 这里组装 `http://127.0.0.1:<port>`。
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

/// 等到 ingest 把 `mock_handle.push` 进去的数据 drain 到 snapshot 表。
///
/// 拒绝 `sleep` 黑魔法 —— 主动轮询 `running.snapshot_len()` 直到达到目标，
/// 或超时 panic（让 CI 失败显式而非沉默 flake）。
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
