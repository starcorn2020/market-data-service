//! # `marketdata-service`
//!
//! Take-home assignment 的核心 crate：接住 [`feed_sim`] 噴出的 `BookMessage` 流，
//! 对外同时提供 request/response（取最新快照）与 pub/sub（推播即时更新）两种 API。
//!
//! 详细设计与不变量见专案根目录的 `AI_DEV_GUILDELINE.md`。
//!
//! ## Phase 1 范围
//!
//! 本 crate 目前**仅实现 ingest → snapshot → bus 的内部路径**；gRPC server 与
//! sample client 在 Phase 2 引入。Phase 1 binary 跑起来后可观察 ingest 进度输出，
//! 验证骨架通畅。
//!
//! ## 对外 API 形状
//!
//! ```ignore
//! let cfg = ServiceConfig::from_env()?;
//! let service = Service::new(cfg)?;     // 假设身处 tokio runtime 上下文
//! service.run().await?;                  // ingest 线程跑到上游排空为止
//! ```

#![warn(missing_docs)]

mod bus;
mod config;
mod grpc;
mod ingest;
mod snapshot;
mod upstream;

pub use config::{ServiceConfig, UpstreamConfig};
pub use grpc::pb;
pub use upstream::{MockHandle, MockUpstream, Upstream, make_book};

// ---------------------------------------------------------------------------
// 错误别名
// ---------------------------------------------------------------------------

/// Service 层统一错误类型。
///
/// `Box<dyn Error + Send + Sync + 'static>` 是 `Send + Sync`，可跨 `tokio::spawn`
/// 边界；同时 `?` 自动从任何实现 `Error + Send + Sync + 'static` 的具体错误转换，
/// 替代 `anyhow::Error` 的 ergonomic 又不引入额外依赖。
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::bus::Bus;
use crate::grpc::{MarketDataServer, MarketDataService};
use crate::ingest::IngestHandle;
use crate::snapshot::Snapshot;
use crate::upstream::FeedSimUpstream;

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// 服务总体生命周期 holder。
///
/// 持有 ingest 线程句柄 + 共享状态（snapshot 表、fan-out bus）+ gRPC 监听地址。
///
/// # D6: runtime 主导权外推
///
/// `Service::new` **假设身处 tokio runtime 上下文**（构造 `Bus` 内部不需要 runtime，
/// 但留出 runtime 给 `tonic::transport::Server::serve` 与 `tokio::spawn`）。
/// Service crate 本身不写 `#[tokio::main]`，runtime 配置权在 `main.rs` 或调用方。
pub struct Service {
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    ingest: Option<IngestHandle>,
    listen_addr: SocketAddr,
    subscriber_queue_size: usize,
}

impl Service {
    /// 构造服务：启动 ingest 线程，绑定 snapshot 表与 bus。
    ///
    /// 注意此函数会**立即启动 feed-sim 背景执行緒**（透过 [`FeedSimUpstream::new`]），
    /// 因此 [`Service::run`] 之前已经在累积数据；gRPC server 上线时 snapshot 表
    /// 通常已经热身完毕（消除 client 第一笔必然 NotYet 的窘境）。
    pub fn new(cfg: ServiceConfig) -> Result<Self, BoxError> {
        let upstream = FeedSimUpstream::new(cfg.upstream.clone())?;
        Self::new_with_upstream(cfg, upstream)
    }

    /// 测试 / 自定义上游入口：注入任意 [`Upstream`] 实作。
    ///
    /// 与 [`Service::new`] 的区别：不构造 `FeedSimUpstream`，让调用方控制
    /// 上游的速率与终止时机。Phase 3 集成测试用 [`MockUpstream`] 走这条路径,
    /// 避免真 feed-sim 背景执行緒的不确定性。
    ///
    /// 走泛型 `<U: Upstream + 'static>` 而非 `Box<dyn Upstream>`：见
    /// `upstream/mod.rs` 文档对静态分派的解释（D3 决策）。
    pub fn new_with_upstream<U: Upstream + 'static>(
        cfg: ServiceConfig,
        upstream: U,
    ) -> Result<Self, BoxError> {
        cfg.validate()?;

        let snapshot = Arc::new(Snapshot::new());
        let bus = Arc::new(Bus::new(cfg.bus_channel_capacity));

        let ingest = ingest::spawn(
            upstream,
            snapshot.clone(),
            bus.clone(),
            cfg.poll_interval,
            cfg.progress_log_every,
        );

        Ok(Self {
            snapshot,
            bus,
            ingest: Some(ingest),
            listen_addr: cfg.listen_addr,
            subscriber_queue_size: cfg.subscriber_queue_size,
        })
    }

    /// 同时跑 ingest 与 tonic gRPC server，三路合流退出条件：
    ///
    /// 1. `Ctrl-C` → stop ingest + tonic `serve` 因 `shutdown_signal` 退出
    /// 2. tonic `serve` 自身报错（端口冲突等）→ stop ingest 后向上抛
    /// 3. Ingest 自然 EOF（上游 `SIM_MAX_MESSAGES` 跑完）→ 触发 graceful shutdown
    pub async fn run(mut self) -> Result<(), BoxError> {
        let ingest_handle = self
            .ingest
            .take()
            .ok_or_else(|| -> BoxError { "Service::run called twice".into() })?;

        // gRPC service：注入共享 snapshot / bus。
        let svc = MarketDataService::new(
            self.snapshot.clone(),
            self.bus.clone(),
            self.subscriber_queue_size,
        );
        let addr = self.listen_addr;
        eprintln!("[server] listening on {addr}");

        // Ingest join 必须放进 blocking pool —— ingest 是 std::thread，
        // 其 JoinHandle::join 是同步阻塞调用（参考 GUIDELINE §7.1）。
        // 通过 `flume`-less 拆法：clone stop signal 给上面用，spawn_blocking 等结束。
        let stop_token = ingest_handle.stop_token();
        let ingest_join = tokio::task::spawn_blocking(move || ingest_handle.join());

        // tonic serve 直接 await；ctrl_c 用 `tokio::signal::ctrl_c`。
        let serve_fut = tonic::transport::Server::builder()
            .add_service(MarketDataServer::new(svc))
            .serve_with_shutdown(addr, async {
                // 三选一关闭：ctrl_c / ingest 自己 EOF / 上层 stop_token 被 set。
                // 这里只需任一触发就够 —— 真正等 ingest stats 是 `ingest_join`。
                let _ = tokio::signal::ctrl_c().await;
            });

        tokio::select! {
            // 路径 1：ingest 自然 EOF（finite stream / max_messages 跑完）。
            //         主动断 gRPC server，让 serve_fut 在下一个 yield 退出。
            join_res = ingest_join => {
                let stats = join_res
                    .map_err(|e| -> BoxError { format!("ingest join task panicked: {e}").into() })?;
                eprintln!(
                    "[service] ingest finished: received={} gaps={}",
                    stats.received, stats.gaps
                );
                // 这里不强制等 serve 退出 —— ctrl_c 信号才会让它走 graceful。
                // Phase 2 deliverable 重点是 demo 跑通；EOF 后 process 退出即可。
                Ok(())
            }
            // 路径 2 + 3：tonic serve 退出（ctrl_c 触发 graceful shutdown 或自身错误）。
            //              停掉 ingest，避免线程泄漏。
            serve_res = serve_fut => {
                stop_token.store(true, std::sync::atomic::Ordering::Release);
                serve_res.map_err(|e| -> BoxError { format!("tonic serve failed: {e}").into() })?;
                eprintln!("[server] shut down gracefully");
                Ok(())
            }
        }
    }

    /// 当前 snapshot 表中已知的 FIGI 数（demo / 测试用）。
    pub fn snapshot_len(&self) -> usize {
        self.snapshot.len()
    }

    /// 集成测试用启动入口：bind 一个 TcpListener、后台 spawn tonic server，
    /// 立即返回 [`RunningService`] 句柄。
    ///
    /// 与 [`Service::run`] 的核心区别：
    /// - `run`：阻塞 await，走 ctrl_c / 自然 EOF 退出。生产 binary 路径。
    /// - `start`：后台 spawn，立即返回 `addr` 与 shutdown handle。集成测试路径。
    ///
    /// 支持 `listen_addr = 127.0.0.1:0`（OS 分配动态端口），用 [`RunningService::addr`]
    /// 拿实际监听端口。集成测试用此避免端口冲突。
    pub async fn start(mut self) -> Result<RunningService, BoxError> {
        let listen_addr = self.listen_addr;
        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .map_err(|e| -> BoxError { format!("bind tcp listener on {listen_addr}: {e}").into() })?;
        let local_addr = listener.local_addr()?;

        let ingest = self
            .ingest
            .take()
            .ok_or_else(|| -> BoxError { "Service::start called twice".into() })?;
        let stop_token = ingest.stop_token();
        let snapshot = self.snapshot.clone();

        let svc = MarketDataService::new(
            self.snapshot.clone(),
            self.bus.clone(),
            self.subscriber_queue_size,
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let join = tokio::spawn(async move {
            let serve_res = tonic::transport::Server::builder()
                .add_service(MarketDataServer::new(svc))
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await;

            // 让 ingest 也停下来，避免线程泄漏（IngestHandle::drop 会 stop+join）。
            stop_token.store(true, Ordering::Release);
            drop(ingest);

            serve_res.map_err(|e| -> BoxError { format!("tonic serve failed: {e}").into() })
        });

        Ok(RunningService {
            local_addr,
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
            snapshot,
        })
    }
}

// ---------------------------------------------------------------------------
// RunningService
// ---------------------------------------------------------------------------

/// 测试用：由 [`Service::start`] 返回的运行中服务句柄。
///
/// 持有 server task 的 join handle 与 graceful shutdown 信号。Drop 时
/// 自动发送 shutdown 信号防止 server task 泄漏，但**强烈建议**显式
/// `.shutdown().await` 以等待 task 真正结束（否则 ingest std::thread
/// 可能在 runtime drop 之后才被回收，引发 cleanup race）。
pub struct RunningService {
    local_addr: SocketAddr,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<(), BoxError>>>,
    snapshot: Arc<Snapshot>,
}

impl RunningService {
    /// 实际监听的地址（含 OS 分配的动态端口）。
    pub fn addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 当前 snapshot 表已知 FIGI 数。集成测试断言 ingest 仍在跑用。
    pub fn snapshot_len(&self) -> usize {
        self.snapshot.len()
    }

    /// 发送 shutdown 信号并 await server task 结束。
    pub async fn shutdown(mut self) -> Result<(), BoxError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            join.await
                .map_err(|e| -> BoxError { format!("server task panicked: {e}").into() })?
        } else {
            Ok(())
        }
    }
}

impl Drop for RunningService {
    fn drop(&mut self) {
        // 防御性：测试遗漏 shutdown 时也不要让 server task / ingest thread 泄漏。
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // 同步 drop 没法 await join；runtime 关停时会 cancel 该 task。
    }
}
