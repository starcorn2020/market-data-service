//! gRPC service implementation.
//!
//! 把 [`crate::snapshot::Snapshot`] 与 [`crate::bus::Bus`] 包装成 tonic
//! `MarketData` service。
//!
//! # 设计要点
//!
//! - `GetSnapshot` 是 unary RPC：直接读 [`Snapshot::get`]；`None` → `NotYet`，
//!   `Some` → `Found(Book)`（README §3 的 "clearly-defined no data yet"）。
//! - `Subscribe` 是 server-streaming：照 GUIDELINE §4.3.4 模板，**严禁 `send().await`**。
//!   wire 阶段 `try_send` 与 fan-in 阶段共用同一个 `dropped` 计数器
//!   （`Subscription::dropped_counter`），所以 `BookUpdate.dropped_total` 是
//!   "fan-in 丢" + "wire 丢" 的累积值。
//! - `BookMessage ↔ proto::Book` 的转换是纯机械映射，集中放在本档底部。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use marketdata_types::{BookMessage, Figi};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::bus::Bus;
use crate::snapshot::Snapshot;

// ---------------------------------------------------------------------------
// Generated proto module
// ---------------------------------------------------------------------------

/// `tonic-build` 输出在 `$OUT_DIR/marketdata.v1.rs`，由 build.rs 触发；
/// 这里 inline pull-in，避免 IDE 找不到。
///
/// `#![allow(missing_docs)]`：proto 注释由 tonic-build 自动转 doc-comment，
/// 但 `derive(Message)` 的 boilerplate 字段不会带 docs；service crate 顶层
/// 用 `#![warn(missing_docs)]` 严格要求，对生成代码必须豁免。
pub mod pb {
    #![allow(missing_docs)]
    tonic::include_proto!("marketdata.v1");
}

pub use pb::market_data_server::{MarketData, MarketDataServer};
use pb::snapshot_response::Result as SnapshotResult;
use pb::{
    Book, BookUpdate, GetSnapshotRequest, Level, NotYet, SnapshotResponse, SubscribeRequest,
};

// ---------------------------------------------------------------------------
// Service impl
// ---------------------------------------------------------------------------

/// `tonic::Server::add_service(MarketDataServer::new(MarketDataService { ... }))` 用的实体。
pub struct MarketDataService {
    snapshot: Arc<Snapshot>,
    bus: Arc<Bus>,
    /// Wire 端 mpsc 的容量。这是 GUIDELINE §4.2 "唯一陷阱" 的关键参数：
    /// tonic server-streaming 默认会把背压传回 producer，必须在此处 try_send
    /// 切断。容量影响 client 短暂 lag 时的容忍度。
    subscriber_queue_size: usize,
}

impl MarketDataService {
    pub fn new(
        snapshot: Arc<Snapshot>,
        bus: Arc<Bus>,
        subscriber_queue_size: usize,
    ) -> Self {
        Self {
            snapshot,
            bus,
            subscriber_queue_size,
        }
    }
}

#[tonic::async_trait]
impl MarketData for MarketDataService {
    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let figi_str = request.into_inner().figi;
        let figi: Figi = figi_str
            .parse()
            .map_err(|_| Status::invalid_argument("figi parse failed"))?;

        // README §3 的两种合法返回：Found / NotYet。
        let result = match self.snapshot.get(&figi) {
            Some(book) => SnapshotResult::Found(book_to_proto(&book)),
            None => SnapshotResult::NotYet(NotYet {}),
        };
        Ok(Response::new(SnapshotResponse {
            result: Some(result),
        }))
    }

    type SubscribeStream = ReceiverStream<Result<BookUpdate, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let figi_strs = request.into_inner().figis;
        if figi_strs.is_empty() {
            return Err(Status::invalid_argument("subscribe with empty figi list"));
        }

        // String → Figi 转换，单条解析失败即拒绝整个请求。
        let figis: Vec<Figi> = figi_strs
            .into_iter()
            .map(|s| {
                s.parse::<Figi>()
                    .map_err(|_| Status::invalid_argument(format!("invalid figi: {s}")))
            })
            .collect::<Result<_, _>>()?;

        // 在 bus 上开订阅（这一步内部 spawn N 个 fan-in task）。
        let mut sub = self.bus.subscribe(&figis, self.subscriber_queue_size);

        // 共享 dropped 计数：fan-in 阶段(bus.rs) 与 wire 阶段(本档下方 spawn)
        // 都 fetch_add 到这同一个 AtomicU64。Client 看到的累积值涵盖"全链路丢"。
        let dropped = sub.dropped_counter();

        // Wire 端 mpsc：tonic 的 ReceiverStream 即从此 Receiver pull。
        // 容量与 fan-in 端独立 —— fan-in 满会丢；wire 满也会丢；二者共同进 dropped。
        let (out_tx, out_rx) =
            mpsc::channel::<Result<BookUpdate, Status>>(self.subscriber_queue_size);

        tokio::spawn(async move {
            while let Some(book) = sub.next().await {
                let upd = BookUpdate {
                    book: Some(book_to_proto(&book)),
                    dropped_total: dropped.load(Ordering::Relaxed),
                };
                // ★ 必须 try_send，不能 await（GUIDELINE §4.2 "唯一陷阱"）。
                // 任何 await 都会让慢 client 反压到 fan-in，再回压 ingest，违反 I1。
                match out_tx.try_send(Ok(upd)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TrySendError::Closed(_)) => {
                        // Client 已断；fan-in task 也会因 mpsc closed 自然退出。
                        break;
                    }
                }
            }
            // 显式 drop 让 ReceiverStream 在 client 端表现为 stream end。
            drop(out_tx);
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}

// ---------------------------------------------------------------------------
// Conversion: marketdata_types::BookMessage  <->  proto Book
// ---------------------------------------------------------------------------

/// 把 service 内部 `BookMessage` 映射到 wire `Book`。
///
/// 注意：
/// - `Figi` 是 `[u8; 12]` (NUL-padded)，调用 `as_str()` 即可自动 trim。
/// - 只输出 `bid_count` / `ask_count` 内的有效 levels（GUIDELINE §2.1 fact）。
///   `BookLevel` 是 `Copy`，但 proto `Level` 没有 `From<BookLevel>` —— 一行 lambda 转完。
fn book_to_proto(msg: &BookMessage) -> Book {
    Book {
        figi: msg.figi.as_str().to_string(),
        gateway_seq: msg.gateway_seq,
        gateway_ts: msg.gateway_ts,
        bids: msg.bids().iter().map(level_to_proto).collect(),
        asks: msg.asks().iter().map(level_to_proto).collect(),
    }
}

fn level_to_proto(level: &marketdata_types::BookLevel) -> Level {
    Level {
        price: level.price,
        qty: level.qty,
        orders: level.orders as u32,
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn figi(s: &str) -> Figi {
        s.parse().unwrap()
    }

    fn sample_book() -> BookMessage {
        let mut m = BookMessage::default();
        m.figi = figi("BBG000000123");
        m.gateway_seq = 42;
        m.gateway_ts = 1_700_000_000_000_000_000;
        m.bid_count = 2;
        m.ask_count = 1;
        m.bids[0] = marketdata_types::BookLevel {
            price: 100.5,
            qty: 1.0,
            orders: 3,
        };
        m.bids[1] = marketdata_types::BookLevel {
            price: 100.0,
            qty: 2.5,
            orders: 7,
        };
        m.asks[0] = marketdata_types::BookLevel {
            price: 101.0,
            qty: 0.5,
            orders: 1,
        };
        m
    }

    #[test]
    fn book_to_proto_maps_basic_fields() {
        let m = sample_book();
        let pb = book_to_proto(&m);

        assert_eq!(pb.figi, "BBG000000123");
        assert_eq!(pb.gateway_seq, 42);
        assert_eq!(pb.gateway_ts, 1_700_000_000_000_000_000);
        assert_eq!(pb.bids.len(), 2);
        assert_eq!(pb.asks.len(), 1);
        assert_eq!(pb.bids[0].price, 100.5);
        assert_eq!(pb.bids[0].qty, 1.0);
        assert_eq!(pb.bids[0].orders, 3);
        assert_eq!(pb.asks[0].price, 101.0);
    }

    #[test]
    fn book_to_proto_trims_to_active_levels_only() {
        // bid_count=2 但 bids 数组有 10 slot —— proto Book 必须只带 2 个。
        let m = sample_book();
        let pb = book_to_proto(&m);
        assert_eq!(pb.bids.len(), 2);
        // 后 8 个 BookLevel::default() 不应进 wire。
    }

    #[test]
    fn book_to_proto_empty_book_is_valid() {
        let m = BookMessage::default();
        let pb = book_to_proto(&m);
        assert_eq!(pb.figi, "");
        assert!(pb.bids.is_empty());
        assert!(pb.asks.is_empty());
    }
}
