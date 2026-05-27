//! Server binary entry point.
//!
//! Quick run:
//!
//! ```sh
//! # Fastest smoke test: 1000 messages / 10 FIGIs / progress log every 100
//! SIM_MAX_MESSAGES=1000 SIM_INSTRUMENTS=10 MDS_PROGRESS_EVERY=100 \
//!     cargo run -p marketdata-service
//! ```
//!
//! Pass criteria:
//!
//! - stderr shows `[ingest] received=... snapshot.len=10 gaps=0 ...` printed continuously
//! - finally `[ingest] stopped: received=1000 ...` appears
//! - process exit code is 0
//!
//! The sample client is a separate binary; see the top-of-file doc in
//! `src/bin/client.rs`.

use marketdata_service::{BoxError, Service, ServiceConfig};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = ServiceConfig::from_env()?;
    eprintln!("[main] config: {cfg:#?}");

    let service = Service::new(cfg)?;
    service.run().await
}
