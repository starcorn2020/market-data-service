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
//! | `MDS_CLIENT_TARGET`  | `http://127.0.0.1:50051` | Server endpoint (use LAN IP for remote demo) |
//! | `MDS_CLIENT_FIGI`    | `BBG000000001`           | FIGI to query / subscribe |
//! | `MDS_CLIENT_FIGIS`   | (same as `MDS_CLIENT_FIGI`) | Comma-separated FIGI list for Subscribe |
//! | `MDS_CLIENT_SECS`    | `3`                      | Subscribe duration |
//! | `MDS_CLIENT_VERBOSE` | (unset)                  | If set (任意非空值), pretty-print 完整 `Book` / `BookUpdate` proto 结构 —— GetSnapshot 结果 + Subscribe 第一笔。用于查看 bids/asks levels 的 price/qty/orders 实际形态。默认关闭,输出保持简洁。 |
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
//!
//! # 预期输出(reviewer 验收锚点)
//!
//! 跑默认参数(`SIM_INSTRUMENTS=10 SIM_RATE_HZ=1000` server + 3s subscribe)时,
//! stderr 大致如下:
//!
//! ```text
//! [client] connecting to http://127.0.0.1:50051
//! [client] GetSnapshot(BBG000000001) -> Found(seq=5921, bids=5, asks=5)
//! [client] Subscribe(["BBG000000001"]) for 3s ...
//! [client] recv #50 dropped_total=0 (seq=6010 figi=BBG000000001)
//! [client] recv #100 dropped_total=0 ...
//! [client] subscribe finished: received=178 dropped_total=0
//! ```
//!
//! 关键观察点(对应不变量验证):
//! - `Found(...)` 而非 `NotYet`:server 启动后 ingest 已经热身(GUIDELINE §4.2
//!   「先 put snapshot 后 publish」+ `Service::new` 立即 spawn ingest 顺序)。
//! - `dropped_total=0`:正常网速 / 单 client / 默认速率下不应有丢失。**若出现 >0
//!   而非递增**,wire 路径 mpsc 满或 broadcast lagged —— 调高 `MDS_BUS_CAPACITY` /
//!   `MDS_SUBSCRIBER_QUEUE`。
//! - `received` 取决于 server 速率与持续秒数(默认 1000 msg/s × N FIGI 平均 →
//!   ≈ `MDS_CLIENT_SECS × RATE_PER_FIGI`)。
//!
//! # Verbose 模式(`MDS_CLIENT_VERBOSE=1`)
//!
//! 想看 wire payload 的实际形态(每档 bid/ask 的 price/qty/orders / proto 字段排布):
//!
//! ```sh
//! MDS_CLIENT_VERBOSE=1 cargo run --bin client
//! ```
//!
//! 额外输出(节选):
//!
//! ```text
//! [client] GetSnapshot(BBG000000001) -> Found:
//! Book {
//!     figi: "BBG000000001",
//!     gateway_seq: 5411,
//!     gateway_ts: 1763890234567890123,
//!     bids: [
//!         Level { price: 100.05, qty: 1.5, orders: 3 },
//!         Level { price: 100.02, qty: 2.1, orders: 5 },
//!         ...
//!     ],
//!     asks: [ ... ],
//! }
//! [client] first BookUpdate:
//! BookUpdate { book: Some(Book { ... }), dropped_total: 0 }
//! ```
//!
//! Verbose 只 dump 一次性的 sample(GetSnapshot 结果 + Subscribe 第一笔),
//! 不影响后续 50 笔节流的简洁输出 —— 避免 stdout 爆炸但仍能让 reviewer 一眼
//! 看清资料形态。

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
    let verbose = std::env::var("MDS_CLIENT_VERBOSE").is_ok();

    eprintln!("[client] connecting to {target}");
    // `MarketDataClient::connect<D: TryInto<Endpoint>>` 直接接 owned `String`,
    // 无需 clone(target 之后不再使用)。
    let mut client = MarketDataClient::connect(target).await?;

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
            if verbose {
                eprintln!("[client] GetSnapshot({figi}) -> Found:\n{book:#?}");
            }
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
                // 第一笔 verbose dump 一次,看 wire payload 的完整形态。
                if verbose && received == 1 {
                    eprintln!("[client] first BookUpdate:\n{upd:#?}");
                }
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
