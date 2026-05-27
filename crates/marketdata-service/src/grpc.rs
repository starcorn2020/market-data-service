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
        // Figi::from_str 是 Infallible(GUIDELINE §2.1)—— 长度 > 12 byte 会 silently
        // 截断而非报错。wire 层显式拒绝过长 figi,避免客户端送 "BBG_LONG_FIGI" 被切成
        // 前 12 byte 后大概率返 NotYet 的诡异 UX。
        if figi_str.len() > 12 {
            return Err(Status::invalid_argument(format!(
                "figi too long ({} bytes, max 12)",
                figi_str.len()
            )));
        }
        let figi: Figi = figi_str
            .parse()
            .expect("Figi::from_str is Infallible per GUIDELINE §2.1");

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

    // `tonic::Status` ~176 bytes,clippy `result_large_err` 提示 `Result<_, Status>`
    // 偏大。这是 tonic handler 的标准错误型别;改 `Box<Status>` 会破坏 tonic 的
    // wire 契约,且 figi parse 失败属于罕见路径,size 优化无意义。
    #[allow(clippy::result_large_err)]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let figi_strs = request.into_inner().figis;
        if figi_strs.is_empty() {
            return Err(Status::invalid_argument("subscribe with empty figi list"));
        }

        // String → Figi 转换。单条解析失败即拒绝整个请求(all-or-nothing)。
        // 同 get_snapshot:Figi::from_str 是 Infallible(GUIDELINE §2.1 silently 截断),
        // 显式拒绝过长入参,避免静默切断后的诡异 UX。
        let figis: Vec<Figi> = figi_strs
            .into_iter()
            .map(|s| -> Result<Figi, Status> {
                if s.len() > 12 {
                    return Err(Status::invalid_argument(format!(
                        "figi too long ({} bytes, max 12): {s}",
                        s.len()
                    )));
                }
                Ok(s.parse::<Figi>()
                    .expect("Figi::from_str is Infallible per GUIDELINE §2.1"))
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

        // wire-pump task:从 fan-in mpsc 拉一笔,搬到 wire mpsc 让 tonic 推给 client。
        //
        // 双臂 select!:
        //   - 主臂 `sub.next()`:有新 update 时正常推流;
        //   - 副臂 `out_tx.closed()`:client 断线时立刻退出,避免「该 figi 久无 publish
        //     时本 task 卡在 sub.next() 直到下一笔 publish 才走 Closed」的潜在 task
        //     泄漏(成本极小,但语义不干净)。
        // 双臂均 cancel-safe(`mpsc::Receiver::recv` / `mpsc::Sender::closed` tokio
        // 文档明示),loop 每次重新 register interest,无 starvation。
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    book = sub.next() => {
                        let Some(book) = book else { break };
                        let upd = BookUpdate {
                            book: Some(book_to_proto(&book)),
                            // dropped.load(Relaxed) 可能滞后 fan-in 端 fetch_add 一刹那
                            // —— 多个 fan_in_one task 并发 add,本 task 单 load。
                            // benign:dropped_total 是累积值,client 用 (curr - prev)
                            // 算 delta;此次 sample 漏掉的会出现在下一笔 BookUpdate,
                            // 最终累积值正确(GUIDELINE §4.3.3 累积值语义)。
                            dropped_total: dropped.load(Ordering::Relaxed),
                        };
                        // ★ 必须 try_send,不能 await(GUIDELINE §4.2 "唯一陷阱")。
                        // 任何 await 都会让慢 client 反压到 fan-in,再回压 ingest,违反 I1。
                        match out_tx.try_send(Ok(upd)) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                dropped.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(TrySendError::Closed(_)) => {
                                // 与副臂 out_tx.closed() 等价路径;client 已断,兜底退出。
                                break;
                            }
                        }
                    }
                    _ = out_tx.closed() => break,
                }
            }
            // 三条退出路径都到这里,drop 语义按路径不同:
            //   - 副臂 out_tx.closed() / 主臂 try_send Closed:out_tx 已 closed,
            //     drop 冗余但无害;
            //   - 主臂 sub.next() 返 None(fan-in mpsc 关闭,e.g. bus 被 drop):
            //     out_tx 仍 open,**必须** drop 让 client 看到 stream end 而非 hang。
            //
            // 关于 `Result<BookUpdate, Status>` 的 Status arm:
            // tonic server-streaming 约定 stream item 是 Result<T, Status>,即使我们
            // 只送 Ok,签名也无法省 Status arm。本档不主动发 mid-stream Err ——
            // client 断线由 tonic transport 层(HTTP/2 RST_STREAM)直接告知,无需
            // service 介入;未来若加 graceful shutdown 想推一笔 Status::Cancelled
            // 给所有订阅者,此处即注入点。
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
    //! grpc.rs 测试分层:
    //!
    //! - **本档(unit)**:只测**纯转换函数** `book_to_proto` —— `BookMessage` 是
    //!   `Copy`,转 proto 不涉及 IO / spawn / runtime,适合 deterministic 单测。
    //! - **`tests/grpc_basic.rs`(integration)**:覆盖 handler 在真 gRPC wire 上的
    //!   行为 —— `GetSnapshot` NotYet/Found 切换、`Subscribe` 推流、空 figi / too-long
    //!   figi 被拒绝。需要 tonic server + client,跑得稍慢但是「真路径」证据。
    //! - **`tests/grpc_slow_consumer.rs`(`#[ignore]` 手动)**:I2 wire 层压力测试
    //!   (详见 DEV_PROCESS §5.1)。
    //!
    //! 不在本档加 handler unit test 的理由:`MarketData::*` 是 `async fn` +
    //! `Request<T>`,直接 unit 测要么 mock 太多(失去信号),要么本质上重写
    //! integration 流程(重复成本)。让 integration 层守 handler 逻辑、unit 层守
    //! 纯函数,分工更干净。

    use super::*;

    fn figi(s: &str) -> Figi {
        s.parse().unwrap()
    }

    fn sample_book() -> BookMessage {
        let mut m = BookMessage {
            figi: figi("BBG000000123"),
            gateway_seq: 42,
            gateway_ts: 1_700_000_000_000_000_000,
            bid_count: 2,
            ask_count: 1,
            ..Default::default()
        };
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
