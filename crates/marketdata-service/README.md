# `marketdata-service`

Market data middleware:接住 `feed-sim` 的 `BookMessage` 流, 對外同時提供
request/response (取最新快照) 與 pub/sub (推播即時更新), 慢/斷的訂閱者
不影響 ingest 與其他訂閱者。

---

## 架構

```
  FeedSubscriber (feed-sim, 黑盒)
            │  Upstream::receive  (trait 抽象,唯一接點)
            ▼
  ingest_loop  (std::thread, 單執行緒)
    │  put snapshot  ─→  Snapshot (Arc<DashMap<Figi, BookMessage>>)
    └  publish bus   ─→  Bus (Arc<DashMap<Figi, broadcast::Sender>>)
                              │  subscribe → fan-in 成 bounded mpsc
                              ▼
                    tonic gRPC server
                      - unary        GetSnapshot
                      - server-stream Subscribe
```

核心保證:

- **Ingest hot path 不阻塞**:整條 `std::thread`, fan-out 全部走 `try_send`, **沒有 `.await` 也不持鎖**。
- **訂閱者完全隔離**:每訂閱者獨立 mpsc + ring buffer 滿了直接 drop, 累計 `dropped_total` 給 client 對齊;慢/斷者不會反壓到 ingest 或其他訂閱者。
- **Vendor 型別不外洩**:`feed-sim` 只透過私有 `upstream::Upstream` trait 接觸, 對外 API 不暴露 `feed_sim::*`(未來換 iceoryx2 只動一個檔)。

題目六條 requirement 的具體出處:`ingest.rs` (§1) / `snapshot.rs` (§2) /
proto `oneof { Found, NotYet }` (§3) / `bus.rs` + `grpc.rs` (§4) /
`0.0.0.0:50051` gRPC over HTTP/2 (§5) / `src/bin/client.rs` (§6)。

---

## 關鍵設計取捨

- **`DashMap` 而非 `Arc<RwLock<HashMap>>`**:per-shard lock 讓 ingest 寫 FIGI-A 與 RPC 讀 FIGI-B 不互斥;單一 `RwLock` 會把所有讀寫串行化。
- **Per-FIGI broadcast channel 而非單一全局**:慢消費者隔離天然成立;訂閱者也不必 deserialize 不關心的 FIGI 流量。
- **滿了 drop + `dropped_total` 累計, 不踢訂閱者**:gRPC stream 重連成本不低, drop 個別訊息更輕量;wire schema 帶累積值, client 做差分判斷自己漏多少。
- **Ingest 用 `std::thread` 而非 `tokio::task`**:`FeedSubscriber` 是同步阻塞 + busy-poll, 丟進 tokio worker 會吃掉一個核心。
- **`Upstream` trait + 泛型靜態分派 (非 `Box<dyn>`)**:換 iceoryx2 時只動 `upstream/feed_sim.rs`, 其它 0 改動;同時讓整合測試能注入 `MockUpstream` 做 deterministic 驗證。
- **`BoxError = Box<dyn Error + Send + Sync>` 而非 `anyhow`**:邊界錯誤型別已足夠, 無新依賴。

---

## Build / Test / Run

> 工作區根目錄執行。命令以 PowerShell 為例,bash 把 `$env:NAME="value"` 換成 `NAME=value cmd`。`protoc` 由 `build.rs` 自動注入, 無系統依賴。

### Build + Test

```powershell
cargo build --release --workspace
cargo test --workspace
# 預期 60 passed + 1 ignored
#   - service: 30 unit + 6 grpc_basic + 1 ignored (wire 壓力測試)
#   - feed-sim: 19, types: 5
```

### Demo (server + client)

兩 terminal,server 出現 `[server] listening on ...` 後再起 client:

```powershell
# Terminal A — Server
$env:SIM_INSTRUMENTS="10"; $env:SIM_RATE_HZ="1000"
cargo run -p marketdata-service

# Terminal B — Client (預期 3 秒自動結束)
cargo run --bin client
```

驗收錨點:client 印出 `Found(seq=..., bids=5, asks=5)` 且 `dropped_total=0`。

- 看完整 wire payload:`$env:MDS_CLIENT_VERBOSE="1"`(額外 dump `Book`/`BookUpdate` 完整 proto 結構)。
- 跨主機:host A 起 server,host B 加 `$env:MDS_CLIENT_TARGET="http://<host-a-lan-ip>:50051"`。

---

## Env vars

| Env | 預設 | 作用 |
|---|---|---|
| `MDS_LISTEN` | `0.0.0.0:50051` | gRPC server 監聽 |
| `MDS_BUS_CAPACITY` | `1024` | 每 FIGI broadcast channel 容量 |
| `MDS_SUBSCRIBER_QUEUE` | `1024` | 每訂閱者 mpsc 容量 |
| `MDS_POLL_INTERVAL_MS` | `50` | ingest `wait()` poll 間隔 |
| `SIM_INSTRUMENTS` | `100` | 模擬 FIGI 數 |
| `SIM_RATE_HZ` | `1000` | 總速率 msg/s |
| `SIM_MAX_MESSAGES` | (無上限) | 訊息上限, 觸發 EOF |
| `MDS_CLIENT_TARGET` | `http://127.0.0.1:50051` | client 連線 endpoint |
| `MDS_CLIENT_FIGI` / `MDS_CLIENT_FIGIS` | `BBG000000001` | client 查 / 訂閱的 FIGI(後者逗號分隔) |
| `MDS_CLIENT_SECS` | `3` | client `Subscribe` 持續秒數 |
| `MDS_CLIENT_VERBOSE` | (未設) | 設任意值即啟用 wire payload pretty-print |

feed-sim 自家 env (`SIM_SEED` / `SIM_DEPTH` / `SIM_PACING` / `SIM_BUFFER_SIZE` /
`SIM_START_SEQ`) 見 `crates/feed-sim/`。

---

## Non-goals (對齊題目 Out of scope)

刻意保持:

- Persistence / auth / TLS(snapshot 純記憶體, 明文 gRPC)
- L3 book reconstruction(`Snapshot::put` 整份覆寫)
- HA / failover / multi-region(單一 process / 單一 endpoint)

## Future work

- 真實 iceoryx2 替換 `feed-sim` —— `Upstream` trait 為此鋪路, 只動一個檔。
- Production graceful shutdown —— `Service::run` 在 ingest EOF 分支加 shared shutdown channel, 讓 in-flight RPC 走 graceful drain。
- 跨主機自動化測試(docker compose / SSH tunnel)—— 當前由手動指令驗證。
