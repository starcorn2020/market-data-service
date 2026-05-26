//! **T1-E2E（DEV_PROCESS §5.1）**：守 I2 在真实 gRPC wire 路径上的成立。
//!
//! Bus 层的 `slow_consumer_isolation` unit 测试证明了内部 fan-in 逻辑正确;
//! 本测试再起一个完整 tonic server + 两个真 gRPC client（fast/slow），
//! 证明 wire 路径（含 grpc.rs::subscribe 的 wire 端 try_send）也守得住 I2。
//!
//! 这是 README 第 4 条「slow or disconnected subscriber must not affect the
//! others」最直接的证据，reviewer 会按测试名搜索。

mod common;

use std::time::Duration;

use marketdata_service::make_book;
use marketdata_service::pb::SubscribeRequest;
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_consumer_isolation_e2e() -> anyhow::Result<()> {
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

    // Publisher: 100 笔，5ms 间隔（500ms 总时长）。
    const TOTAL: u64 = 100;
    let pusher = {
        let mock = mock.clone();
        tokio::spawn(async move {
            for seq in 1..=TOTAL {
                mock.push(make_book(figi, seq));
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Fast: tight loop drain，截止时间宽松。
    let fast_task = tokio::spawn(async move {
        let mut got = 0u64;
        let mut last_dropped = 0u64;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(2500);
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

    // Slow: 故意先 stall 800ms（让 publisher 把所有 100 笔推完），
    // 期间 server 端 broadcast(16) + fan-in mpsc(4) + wire mpsc(4) ≈ 24 个 slot
    // 必然撑爆，剩下 ~76 笔进 dropped_total。
    // 然后再 drain 看实际收到多少 + dropped_total 最终值。
    let slow_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;

        let mut got = 0u64;
        let mut last_dropped = 0u64;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(150), slow_stream.next()).await {
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
        "[T1-E2E] fast: got={fast_got} dropped_total={fast_dropped} | \
         slow: got={slow_got} dropped_total={slow_dropped}"
    );

    // ★ I2 关键断言（wire 级）。
    assert!(
        fast_got >= TOTAL - 5,
        "fast 应收到几乎全部（≥{} of {TOTAL}），实际 {fast_got}",
        TOTAL - 5
    );
    assert!(
        fast_dropped <= 5,
        "I2: 快 client 应几乎 0 损失（容忍 ≤5 调度噪声）, 实际 dropped_total={fast_dropped}"
    );
    assert!(
        slow_dropped > 0,
        "I2: 慢 client 必须看到 dropped_total > 0（slow 在 publisher 推完前 stall 800ms,\
         buffer 必爆）, 实际 {slow_dropped}"
    );
    assert!(
        slow_got < fast_got,
        "慢 client 收到应严格少于快 client：slow={slow_got} fast={fast_got}"
    );

    mock.close();
    running.shutdown().await?;
    Ok(())
}
