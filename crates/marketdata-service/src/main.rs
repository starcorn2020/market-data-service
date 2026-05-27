//! Phase 1 二进位 entrypoint。
//!
//! 跑法：
//!
//! ```sh
//! # 最快的 smoke test：跑 1000 笔 / 10 个 FIGI / 进度每 100 笔打一行
//! SIM_MAX_MESSAGES=1000 SIM_INSTRUMENTS=10 MDS_PROGRESS_EVERY=100 \
//!     cargo run -p marketdata-service
//! ```
//!
//! 通过条件：
//! - stderr 看到 `[ingest] received=... snapshot.len=10 gaps=0 ...` 持续打出
//! - 最后看到 `[ingest] stopped: received=1000 ...`
//! - 进程退出码 0

use marketdata_service::{BoxError, Service, ServiceConfig};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = ServiceConfig::from_env()?;
    eprintln!("[main] config: {cfg:#?}");

    let service = Service::new(cfg)?;
    service.run().await
}
