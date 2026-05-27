//! Sample gRPC client — the "end-to-end demo of both APIs" required by the assignment.
//!
//! Demos both APIs end-to-end:
//!
//! 1. `GetSnapshots([figi…])` → prints one `Found(seq, bids, asks)` /
//!    `NotYet` line per requested FIGI.
//! 2. `Subscribe([figi…])` → receives the push stream for N seconds and on
//!    exit prints `received` and `dropped_total`, letting the reviewer
//!    directly observe whether the slow-consumer lag mechanism is engaged
//!    (note: if the client keeps up with the rate, `dropped_total` may stay
//!    at 0; to see a non-zero value, temporarily lower the server's
//!    `MDS_SUBSCRIBER_QUEUE` or raise `SIM_RATE_HZ`).
//!
//! # Verbose mode (highly recommended for the first run)
//!
//! The default output only shows summary lines (counts / seq / figi); it
//! **does not show the actual price / qty / orders inside bids/asks**.
//! Setting `MDS_CLIENT_VERBOSE` additionally dumps the full proto structure
//! in three places (pretty-printed with `{:#?}`):
//!
//! - **Each GetSnapshots hit**: the full `Book` for every `Found` entry,
//!   including all 5 `PriceLevel`s.
//! - **First Subscribe `BookUpdate`**: the actual shape of the wire payload
//!   (including `dropped_total`, the `book` field, the `PriceLevel.orders`
//!   list, etc.).
//! - **Last Subscribe `BookUpdate`**: lets you compare the first and last
//!   messages (seq advancement, level changes).
//!
//! This is the demo client's only entry point for directly seeing "what an
//! order book looks like on the wire"; it is almost always required when
//! onboarding a new reviewer, validating the schema, or chasing missing
//! proto fields. It is off by default purely to keep happy-path output
//! concise and avoid scrolling.
//!
//! # Env vars
//!
//! | Env | Default | Purpose |
//! |---|---|---|
//! | `MDS_CLIENT_TARGET`  | `http://127.0.0.1:50051`    | Server endpoint (for a LAN demo, use `http://<LAN-IP>:50051`) |
//! | `MDS_CLIENT_FIGI`    | `BBG000000001`              | Default FIGI (used when `MDS_CLIENT_FIGIS` is unset) |
//! | `MDS_CLIENT_FIGIS`   | (same as `MDS_CLIENT_FIGI`) | Comma-separated FIGI list passed to **both** GetSnapshots and Subscribe |
//! | `MDS_CLIENT_SECS`    | `3`                         | Subscribe duration (seconds) |
//! | `MDS_CLIENT_VERBOSE` | (unset)                     | **Set to any non-empty value** to enable verbose dump (see above). Note that `MDS_CLIENT_VERBOSE=0` also enables it (the code uses `var().is_ok()`). |
//!
//! # Usage (PowerShell)
//!
//! ```powershell
//! # Simplest: all defaults, summary output only.
//! cargo run -p marketdata-service --bin client --release
//!
//! # Recommended: enable verbose to see the full Book/BookUpdate structure.
//! $env:MDS_CLIENT_VERBOSE = "1"
//! cargo run -p marketdata-service --bin client --release
//!
//! # Multiple FIGIs + longer run for easier dropped_total observation.
//! $env:MDS_CLIENT_FIGIS = "BBG000000001,BBG000000002"
//! $env:MDS_CLIENT_SECS  = "10"
//! cargo run -p marketdata-service --bin client --release
//! ```


use std::time::{Duration, Instant};

use marketdata_service::BoxError;
use marketdata_service::pb::{
    BookUpdate, GetSnapshotsRequest, SubscribeRequest, market_data_client::MarketDataClient,
    snapshot_entry::Result as SnapResult,
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
    let verbose = std::env::var("MDS_CLIENT_VERBOSE").is_ok();

    // Used by both GetSnapshots and Subscribe — keeps the two demos
    // symmetric (matching the batch shape on the wire).
    let figis: Vec<String> = figis_csv
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    eprintln!("[client] connecting to {target}");
    let mut client = MarketDataClient::connect(target).await?;

    // ---- demo 1: unary GetSnapshots ----
    eprintln!("[client] GetSnapshots({figis:?}) ...");
    let resp = client
        .get_snapshots(GetSnapshotsRequest {
            figis: figis.clone(),
        })
        .await?
        .into_inner();
    for entry in resp.entries {
        let entry_figi = entry.figi;
        match entry.result {
            Some(SnapResult::Found(book)) => {
                eprintln!(
                    "[client] GetSnapshots({entry_figi}) -> Found(seq={}, bids={}, asks={})",
                    book.gateway_seq,
                    book.bids.len(),
                    book.asks.len(),
                );
                if verbose {
                    eprintln!(
                        "[client] GetSnapshots({entry_figi}) -> Found:\n{book:#?}"
                    );
                }
            }
            Some(SnapResult::NotYet(_)) | None => {
                eprintln!("[client] GetSnapshots({entry_figi}) -> NotYet");
            }
        }
    }

    // ---- demo 2: server-streaming Subscribe ----
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
    // Keep the most recent BookUpdate around so we can dump it in verbose
    // mode after the loop exits.
    let mut last_upd: Option<BookUpdate> = None;

    // tonic streaming::next() comes from futures_core::Stream, via prelude.
    use tokio_stream::StreamExt as _;
    loop {
        // Use a timeout to bound total duration; the stream itself may never
        // end (the server keeps pushing).
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(upd))) => {
                received += 1;
                final_dropped = upd.dropped_total;
                // Dump the first message once in verbose mode to inspect the
                // full wire-payload shape.
                if verbose && received == 1 {
                    eprintln!("[client] first BookUpdate:\n{upd:#?}");
                }
                // Print one summary line every 50 messages: avoids flooding
                // stdout while still showing that the stream is alive.
                if received.is_multiple_of(50) {
                    eprintln!(
                        "[client] recv #{received} dropped_total={final_dropped} \
                         (seq={} figi={})",
                        upd.book.as_ref().map(|b| b.gateway_seq).unwrap_or(0),
                        upd.book.as_ref().map(|b| b.figi.as_str()).unwrap_or("?"),
                    );
                }
                last_upd = Some(upd);
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

    // Dump the final BookUpdate in full (verbose only).
    if verbose {
        match &last_upd {
            Some(upd) => eprintln!("[client] final BookUpdate:\n{upd:#?}"),
            None => eprintln!("[client] final BookUpdate: <none received>"),
        }
    }

    eprintln!(
        "[client] subscribe finished: received={received} dropped_total={final_dropped}"
    );

    Ok(())
}
