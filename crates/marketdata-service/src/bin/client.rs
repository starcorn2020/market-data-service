//! Sample gRPC client (README §6).
//!
//! Demos both APIs end-to-end:
//!
//! 1. `GetSnapshot(figi)` → prints `Found(...)` or `NotYet`.
//! 2. `Subscribe([figi…])` → counts updates for N seconds, prints final
//!    `dropped_total` so reviewer can see GUIDELINE §4.3.3 lag mechanism live.
//!
//! Env vars:
//!
//! | Env | Default | Purpose |
//! |---|---|---|
//! | `MDS_CLIENT_TARGET` | `http://127.0.0.1:50051` | Server endpoint (use LAN IP for remote demo) |
//! | `MDS_CLIENT_FIGI`   | `BBG000000001`           | FIGI to query / subscribe |
//! | `MDS_CLIENT_FIGIS`  | (same as `MDS_CLIENT_FIGI`) | Comma-separated FIGI list for Subscribe |
//! | `MDS_CLIENT_SECS`   | `3`                      | Subscribe duration |
//!
//! 跨主机 demo:
//!
//! ```sh
//! # Server (host A)
//! MDS_LISTEN=0.0.0.0:50051 SIM_INSTRUMENTS=10 cargo run -p marketdata-service
//!
//! # Client (host B on same LAN)
//! MDS_CLIENT_TARGET=http://<host-a-ip>:50051 cargo run --bin client
//! ```

use std::time::{Duration, Instant};

use marketdata_service::BoxError;
use marketdata_service::pb::{
    GetSnapshotRequest, SubscribeRequest, market_data_client::MarketDataClient,
    snapshot_response::Result as SnapResult,
};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let target = std::env::var("MDS_CLIENT_TARGET")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let figi = std::env::var("MDS_CLIENT_FIGI").unwrap_or_else(|_| "BBG000000001".to_string());
    let figis_csv = std::env::var("MDS_CLIENT_FIGIS").unwrap_or_else(|_| figi.clone());
    let secs: u64 = std::env::var("MDS_CLIENT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    eprintln!("[client] connecting to {target}");
    let mut client = MarketDataClient::connect(target.clone()).await?;

    // ---- demo 1: unary GetSnapshot ----
    let resp = client
        .get_snapshot(GetSnapshotRequest {
            figi: figi.clone(),
        })
        .await?
        .into_inner();
    match resp.result {
        Some(SnapResult::Found(book)) => {
            eprintln!(
                "[client] GetSnapshot({figi}) -> Found(seq={}, bids={}, asks={})",
                book.gateway_seq,
                book.bids.len(),
                book.asks.len(),
            );
        }
        Some(SnapResult::NotYet(_)) | None => {
            eprintln!("[client] GetSnapshot({figi}) -> NotYet");
        }
    }

    // ---- demo 2: server-streaming Subscribe ----
    let figis: Vec<String> = figis_csv
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    eprintln!("[client] Subscribe({figis:?}) for {secs}s ...");

    let mut stream = client
        .subscribe(SubscribeRequest {
            figis: figis.clone(),
        })
        .await?
        .into_inner();

    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut received: u64 = 0;
    let mut final_dropped: u64 = 0;

    // tonic streaming::next() 来自 futures_core::Stream，via prelude.
    use tokio_stream::StreamExt as _;
    loop {
        // 用 timeout 控总时长；stream 自身可能永远不结束（server 持续推流）。
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(upd))) => {
                received += 1;
                final_dropped = upd.dropped_total;
                // 每 50 笔打一行，避免 stdout 爆炸但仍能看到推流活着。
                if received.is_multiple_of(50) {
                    eprintln!(
                        "[client] recv #{received} dropped_total={final_dropped} \
                         (seq={} figi={})",
                        upd.book.as_ref().map(|b| b.gateway_seq).unwrap_or(0),
                        upd.book.as_ref().map(|b| b.figi.as_str()).unwrap_or("?"),
                    );
                }
            }
            Ok(Some(Err(status))) => {
                return Err(format!("stream error: {status:?}").into());
            }
            Ok(None) => {
                eprintln!("[client] stream closed by server");
                break;
            }
            Err(_) => break, // timeout reached
        }
    }

    eprintln!(
        "[client] subscribe finished: received={received} dropped_total={final_dropped}"
    );

    Ok(())
}
