//! 在真实 gRPC wire 路径上的慢消费者隔离 **压力测试**。
//!
//! # 状态:`#[ignore]`,不计入默认 `cargo test` 绿灯
//!
//! 手动触发:
//!
//! ```bash
//! cargo test -p marketdata-service --test grpc_slow_consumer -- --ignored
//! ```
//!
//! # 为什么 `#[ignore]`
//!
//! "慢/断订阅者隔离" 这条**逻辑不变量**已由 `src/bus.rs` 的 unit 测试守住:
//!
//! - `slow_consumer_isolation` — fast/slow 在 Bus 层并存, fast 不被反压。
//! - `disconnected_subscriber_does_not_stall_others` — 断开订阅者不波及兄弟。
//!
//! 二者**与 buffer 大小无关**, 只验证 "每订阅者独立 mpsc + `try_send` 失败
//! 累进 `dropped`" 的逻辑正确性 —— 这才是题目要的隔离语义。
//!
//! 本档测的是**性能 characterization** (能否在真实 wire 上观测到 dropped > 0),
//! 受 HTTP/2 flow control window、TCP send/recv buffer、kernel 网络栈等系统
//! 级参数影响, 在某些环境 (macOS M-series 默认 sysctl) 下 slow 端可能"够快"
//! 把所有 240 KB 在 stall 期间全收完, 即使逻辑完全正确, 测试也会 fail。
//!
//! 保留代码而非删除:不同 TCP 配置 / 跨主机部署下本测试仍是验证 wire 容量
//! 假设的实用工具, 且下方的量化推导是 reviewer 询问 "你 buffer 怎么调?" 时
//! 的现成答案。
//!
//! # 关键陷阱:wire payload 必须够大
//!
//! `BookMessage` 经 protobuf 编码后的实际字节数决定 HTTP/2 window 能装多少笔:
//!
//! | book 构造方式 | wire size/笔 | 65535 byte window 容量 |
//! |---|---|---|
//! | `make_book(figi, seq)` 空壳 (`bid_count=0 / ask_count=0`) | ~25 B | ~2600 笔 |
//! | `full_book(figi, seq)` (本档内, 10 bids + 10 asks 全填) | ~480 B | ~134 笔 |
//!
//! 必须用 `full_book` + 足够 TOTAL 才能让 wire buffer 撑爆;空壳直接装满
//! 整个 window, 测试退化为 "client 收得快不快"。
//!
//! # 压力参数 (本档当前值)
//!
//! - `bus_channel_capacity = 16`, `subscriber_queue_size = 4` (override `test_config`)
//! - `TOTAL = 500` 笔, `full_book` 每笔 ~480 B → ~240 KB > window 65535 B
//! - publisher 2ms/笔 (总 1000ms);slow 先 stall 1500ms → 推完后 slow 仍 stall 500ms
//! - slow drain deadline 3000ms;fast deadline 4000ms
//!
//! 严格 HTTP/2 default window 实现上 `slow_dropped` 应到 300+。

mod common;

use std::time::Duration;

use marketdata_service::BoxError;
use marketdata_service::pb::SubscribeRequest;
use marketdata_types::{BookLevel, BookMessage, Figi};
use tokio_stream::StreamExt;

/// 构造一个**满载** `BookMessage` (10 bids + 10 asks 全填非零), 用于撑大
/// wire payload, 让 HTTP/2 flow control window 必然爆。
///
/// 与 `marketdata_service::make_book` 的区别 (详见档头「关键陷阱」段):
/// `make_book` 的 `bid_count/ask_count` 都是 0, wire 上每笔仅 ~25 bytes;
/// 本函数让每笔 ~480 bytes, 500 笔 ≈ 240 KB → 远超 stream window 65535 bytes。
fn full_book(figi: &str, gateway_seq: u64) -> BookMessage {
    let mut m = BookMessage {
        figi: figi.parse::<Figi>().expect("Figi::from_str is Infallible"),
        gateway_seq,
        gateway_ts: 1_700_000_000_000_000_000 + gateway_seq as i64,
        bid_count: 10,
        ask_count: 10,
        ..Default::default()
    };
    for i in 0..10 {
        // 用 seq 与 level index 混合的值, 避免 protobuf 把全 0 编码成 0 byte。
        // `orders` 是 u16(`BookLevel` 定义);TOTAL=500 内 mod 10000 保证 fit
        // u16 且每筆都有变化, wire encoding 长度稳定。
        let mix = ((gateway_seq + i as u64 + 1) % 10000) as u16;
        m.bids[i] = BookLevel {
            price: 100.0 - i as f64 * 0.5 + (gateway_seq as f64 * 0.01),
            qty: 1.0 + i as f32 + (gateway_seq % 7) as f32,
            orders: mix.wrapping_mul(17),
        };
        m.asks[i] = BookLevel {
            price: 101.0 + i as f64 * 0.5 + (gateway_seq as f64 * 0.01),
            qty: 2.0 + i as f32 + (gateway_seq % 5) as f32,
            orders: mix.wrapping_mul(23),
        };
    }
    m
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress test, env-dependent. 隔离不变量由 bus.rs unit 测试守住;\
            本测试供手动 wire-level 压力跑:\
            `cargo test -p marketdata-service --test grpc_slow_consumer -- --ignored`"]
async fn slow_consumer_isolation_e2e() -> Result<(), BoxError> {
    // 故意把 wire 端 mpsc 队列压小，让慢 client 更易触发 Full。
    let mut cfg = common::test_config();
    cfg.bus_channel_capacity = 16;
    cfg.subscriber_queue_size = 4;
    let (running, mock) = common::spawn_service(cfg).await?;

    let addr = running.addr();
    let mut fast_client = common::make_client(addr).await?;
    let mut slow_client = common::make_client(addr).await?;

    let figi = "BBG000000001";
    let mut fast_stream = fast_client
        .subscribe(SubscribeRequest {
            figis: vec![figi.into()],
        })
        .await?
        .into_inner();
    let mut slow_stream = slow_client
        .subscribe(SubscribeRequest {
            figis: vec![figi.into()],
        })
        .await?
        .into_inner();

    // 等 server 端 fan-in tasks 真正 attach 到 broadcast。
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publisher: 500 笔，2ms 间隔（1000ms 总时长）。
    // 详见档案顶部「压力参数的量化推导」—— 500 > 总缓冲(~206)，
    // 保证至少 ~294 笔进 slow_dropped。
    const TOTAL: u64 = 500;
    let pusher = {
        let mock = mock.clone();
        tokio::spawn(async move {
            for seq in 1..=TOTAL {
                // 用 full_book 而非 make_book —— wire size 必须够大才能填满
                // HTTP/2 window,详见档头「压力参数的量化推导」。
                mock.push(full_book(figi, seq));
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };

    // Fast: tight loop drain，截止时间覆盖 publisher 推送 (1000ms)
    // + drain 全部 500 笔的窗口。
    let fast_task = tokio::spawn(async move {
        let mut got = 0u64;
        let mut last_dropped = 0u64;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(4000);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), fast_stream.next()).await {
                Ok(Some(Ok(upd))) => {
                    got += 1;
                    last_dropped = upd.dropped_total;
                }
                Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
            }
        }
        (got, last_dropped)
    });

    // Slow: 故意先 stall 1500ms（> publisher 1000ms 总时长 → 推完后 slow 仍
    // stall 500ms）。期间:
    //  - 前 ~182 笔进 client TCP recv buffer（HTTP/2 window 容量内）
    //  - 下一笔起 server 端 wire mpsc(4) + fan-in mpsc(4) + broadcast(16) 依序积累
    //  - 超过 ~206 总缓冲后,每多一笔 → fetch_add(1) 进 dropped_total
    // 然后再 drain 3000ms 看实际收到多少 + dropped_total 最终值。
    let slow_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let mut got = 0u64;
        let mut last_dropped = 0u64;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(3000);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), slow_stream.next()).await {
                Ok(Some(Ok(upd))) => {
                    got += 1;
                    last_dropped = upd.dropped_total;
                }
                Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
            }
        }
        (got, last_dropped)
    });

    pusher.await?;
    let (fast_got, fast_dropped) = fast_task.await?;
    let (slow_got, slow_dropped) = slow_task.await?;

    eprintln!(
        "[stress] fast: got={fast_got} dropped_total={fast_dropped} | \
         slow: got={slow_got} dropped_total={slow_dropped}"
    );

    // ★ 隔离关键断言 (wire 级)。
    assert!(
        fast_got >= TOTAL - 5,
        "fast 应收到几乎全部 (≥{} of {TOTAL}), 实际 {fast_got}",
        TOTAL - 5
    );
    assert!(
        fast_dropped <= 5,
        "快 client 应几乎 0 损失 (容忍 ≤5 调度噪声), 实际 dropped_total={fast_dropped}"
    );
    assert!(
        slow_dropped > 0,
        "慢 client 必须看到 dropped_total > 0 (slow 在 publisher 推完前 stall 800ms, \
         buffer 必爆), 实际 {slow_dropped}"
    );
    assert!(
        slow_got < fast_got,
        "慢 client 收到应严格少于快 client:slow={slow_got} fast={fast_got}"
    );

    mock.close();
    running.shutdown().await?;
    Ok(())
}
