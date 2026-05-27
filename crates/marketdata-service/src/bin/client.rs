//! Sample gRPC client —— 题面要求的 "end-to-end 演示两条 API"。
//!
//! Demos both APIs end-to-end:
//!
//! 1. `GetSnapshot(figi)` → prints `Found(seq, bids, asks)` 或 `NotYet`。
//! 2. `Subscribe([figi…])` → 按时长收 N 秒推流, 结束时打印 `received` 与
//!    `dropped_total`, 让 reviewer 直接观察 slow-consumer lag 机制是否生效
//!    (注:若 client 跟得上节奏, `dropped_total` 可能始终为 0; 想看到非 0,
//!    可临时把 server 的 `MDS_SUBSCRIBER_QUEUE` 设小或 `SIM_RATE_HZ` 调高)。
//!
//! # Verbose 模式（强烈建议第一次跑就打开）
//!
//! 默认输出只有计数 / seq / figi 这种摘要行，**看不到 bids/asks 的实际
//! price / qty / orders**。打开 `MDS_CLIENT_VERBOSE` 后会额外 dump 三处
//! 完整 proto 结构（`{:#?}` pretty print）：
//!
//! - **GetSnapshot 命中时**：完整 `Book`，含全部 5 档 `PriceLevel`。
//! - **Subscribe 第一笔 `BookUpdate`**：看 wire payload 的真实形态
//!   （含 `dropped_total`、`book` 字段、`PriceLevel.orders` 列表等）。
//! - **Subscribe 最后一笔 `BookUpdate`**：让你对比首末两笔的差异
//!   （seq 推进、levels 变化）。
//!
//! 这是 demo client 唯一能直接看到「订单簿在 wire 上长什么样」的入口；
//! 调试新 reviewer / 验证 schema / 排查 proto 字段缺漏时几乎一定要开。
//! 默认关闭只是为了让 happy-path 输出保持精简、不刷屏。
//!
//! # Env vars
//!
//! | Env | Default | Purpose |
//! |---|---|---|
//! | `MDS_CLIENT_TARGET`  | `http://127.0.0.1:50051`    | Server endpoint（局域网 demo 换成 `http://<LAN-IP>:50051`） |
//! | `MDS_CLIENT_FIGI`    | `BBG000000001`              | FIGI to query / subscribe |
//! | `MDS_CLIENT_FIGIS`   | (same as `MDS_CLIENT_FIGI`) | Comma-separated FIGI list for Subscribe |
//! | `MDS_CLIENT_SECS`    | `3`                         | Subscribe duration (seconds) |
//! | `MDS_CLIENT_VERBOSE` | (unset)                     | **设为任意非空值**即开启 verbose dump（见上节）。注意 `MDS_CLIENT_VERBOSE=0` 也算开（用的是 `var().is_ok()`）。 |
//!
//! # Usage (PowerShell)
//!
//! ```powershell
//! # 最简：全部默认，仅摘要输出。
//! cargo run -p marketdata-service --bin client --release
//!
//! # 推荐：开 verbose，看完整 Book/BookUpdate 结构。
//! $env:MDS_CLIENT_VERBOSE = "1"
//! cargo run -p marketdata-service --bin client --release
//!
//! # 多 FIGI + 跑久一点，方便观察 dropped_total。
//! $env:MDS_CLIENT_FIGIS = "BBG000000001,BBG000000002"
//! $env:MDS_CLIENT_SECS  = "10"
//! cargo run -p marketdata-service --bin client --release
//! ```


use std::time::{Duration, Instant};

use marketdata_service::BoxError;
use marketdata_service::pb::{
    BookUpdate, GetSnapshotRequest, SubscribeRequest, market_data_client::MarketDataClient,
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
    // 保留最后一笔 BookUpdate，供 loop 结束后 verbose dump。
    let mut last_upd: Option<BookUpdate> = None;

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

    // 把最后一笔 BookUpdate 完整 dump 出来（verbose 才打）。
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
