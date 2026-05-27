# `marketdata-service`

Market data middleware:接住 `feed-sim` 的 `BookMessage` 流,對外同時提供
request/response(取最新快照)與 pub/sub(推播即時更新),慢/斷的訂閱者
不影響 ingest 與其他訂閱者。

---

## 架構

```
  FeedSubscriber (feed-sim, 黑盒)
            │
            │ Upstream::receive  (trait 抽象,唯一接點)
            ▼
  ingest_loop  (std::thread,單執行緒)
    │  put snapshot  ─→  Snapshot (Arc<DashMap<Figi, BookMessage>>)
    └  publish bus   ─→  Bus (Arc<DashMap<Figi, broadcast::Sender>>)
                              │
                              │ subscribe → fan-in 成 bounded mpsc
                              ▼
                    tonic gRPC server
                      - unary        GetSnapshot
                      - server-stream Subscribe (per-subscriber 隔離)
```

核心保證:

- Ingest 是唯一一條 `std::thread`,fan-out 全部走 `try_send`,**hot path 上沒有 `.await` 也不持鎖**。
- 每個訂閱者有獨立 mpsc + ring buffer 滿了直接 drop(累計 `dropped_total` 給 client 對齊),**慢 / 斷的訂閱者不會反壓到 ingest 或其他訂閱者**。
- `feed-sim` 只透過私有 `upstream::Upstream` trait 接觸,**對外 API 不暴露 `feed_sim::*` 型別**(未來換 iceoryx2 只動一個檔)。

---

## 對應題目六條目

| § | 題目要求 | 解法 |
|---|---|---|
| 1 | Consumes `BookMessage`s | `ingest.rs` 單 std::thread 獨佔 `FeedSubscriber`,雙層 `wait()` + `while let Some(receive())` |
| 2 | Per-Figi 最新快照 | `snapshot.rs`:`Arc<DashMap<Figi, BookMessage>>`,整份覆蓋寫入(不合併) |
| 3 | Req/Resp + 明確「no data yet」 | proto `oneof { Found, NotYet }`,client 端 pattern match 強制處理空資料 |
| 4 | Pub/Sub + slow consumer 隔離 | Per-FIGI `tokio::broadcast` → 每訂閱者獨立 fan-in task → bounded mpsc → tonic stream;wire-pump `tokio::select!` 雙臂 cancel-safe |
| 5 | 同主機 + 跨主機 | 監聽 `0.0.0.0:50051`,單一 gRPC over HTTP/2,本機 / LAN 同 wire |
| 6 | Sample client | `cargo run --bin client` 端到端 demo 兩條 RPC |

---

## 套件選擇

| 套件 | 用途 |
|---|---|
| `tonic` + `prost` | gRPC server + proto 序列化;`unary` + `server-streaming` 與題目兩條 API 一對一,`.proto` 即 wire schema |
| `tonic-build` + `protoc-bin-vendored` | build 期生成 proto Rust 代碼,**reviewer 不需自行安裝 protoc** |
| `tokio (full)` + `tokio-stream` | async runtime + Stream adapter |
| `dashmap` | Snapshot + Bus 的並發 map,per-shard RwLock 不互相阻塞 |

無 `anyhow` —— 邊界用 `BoxError = Box<dyn Error + Send + Sync>` 已足夠。

---

## 關鍵設計取捨

- **`DashMap` 而非 `Arc<RwLock<HashMap>>`**:per-shard lock 讓 ingest 寫 FIGI-A 與 RPC 讀 FIGI-B 不互斥;單一 `RwLock` 會把所有讀寫串行化。
- **Per-FIGI broadcast channel 而非單一全局**:慢消費者隔離天然成立;訂閱者也不必 deserialize 不關心的 FIGI 流量。
- **滿了 drop + `dropped_total` 累計,不踢訂閱者**:gRPC stream 重連成本不低,drop 個別訊息更輕量;wire schema 帶累積值,client 做差分判斷自己漏多少。
- **Ingest 用 `std::thread` 而非 `tokio::task`**:`FeedSubscriber` 是同步阻塞 + busy-poll,丟進 tokio worker 會吃掉一個核心。
- **`Upstream` trait + 泛型靜態分派(非 `Box<dyn>`)**:換 iceoryx2 時只動 `upstream/feed_sim.rs`,client / wire / 測試 0 改動;同時讓整合測試能注入 `MockUpstream` 做 deterministic 驗證。

---

## Build / Test / Run

> 工作區根目錄執行。PowerShell 設 env 用 `$env:NAME="value"`,bash 用 `NAME=value cmd`。

### Build

```sh
cargo build --release --workspace
```

`protoc` 由 `build.rs` 自動注入,無系統依賴。

### Test

```sh
cargo test --workspace
# 預期:60 passed + 1 ignored
#   - service:36 (30 unit + 6 grpc_basic)
#   - feed-sim:19
#   - types:5
#   - ignored:1 (grpc_slow_consumer_isolation_e2e,wire 壓力測試,環境依賴)
```

分層驗收:

```sh
# gRPC 編解碼層(BookMessage ↔ proto Book)
cargo test -p marketdata-service --lib grpc::tests

# wire 整條 happy path + 邊界(端到端 tonic server + client + mock 上游)
cargo test -p marketdata-service --test grpc_basic

# slow / disconnected subscriber 隔離(I2 不變量,deterministic unit)
cargo test -p marketdata-service --lib bus::tests::slow_consumer_isolation
cargo test -p marketdata-service --lib bus::tests::disconnected_subscriber_does_not_stall_others
```

### Run server + client(README §6 demo)

需要**兩個 terminal**,先起 server 等到看到 `[server] listening on ...` 再起 client。

**Terminal A — Server**(PowerShell):

```powershell
$env:MDS_LISTEN="0.0.0.0:50051"
$env:SIM_INSTRUMENTS="10"
$env:SIM_RATE_HZ="1000"
cargo run -p marketdata-service
```

bash:

```sh
MDS_LISTEN=0.0.0.0:50051 SIM_INSTRUMENTS=10 SIM_RATE_HZ=1000 \
  cargo run -p marketdata-service
```

預期 stderr:

```text
[server] listening on 0.0.0.0:50051
[ingest] started (poll_interval=50ms, progress_every=100)
[ingest] received=100 snapshot.len=10 gaps=0 ...
```

**Terminal B — Client**:

```sh
cargo run --bin client
```

預期(3 秒後自動結束):

```text
[client] GetSnapshot(BBG000000001) -> Found(seq=5921, bids=5, asks=5)
[client] Subscribe(["BBG000000001"]) for 3s ...
[client] recv #50 dropped_total=0 (seq=6010 figi=BBG000000001)
[client] subscribe finished: received=178 dropped_total=0
```

驗收錨點:`Found(...)` 而非 `NotYet` + `dropped_total=0`。

### Client verbose 模式(看 wire payload 完整形態)

```powershell
$env:MDS_CLIENT_VERBOSE="1"
cargo run --bin client
```

額外 dump `Book` / `BookUpdate` 完整 proto 結構(GetSnapshot 結果 + Subscribe 第一筆),
看每檔 bid/ask 的 price/qty/orders。

### 跨主機 demo

Host A 起 server(`MDS_LISTEN=0.0.0.0:50051`);Host B:

```powershell
$env:MDS_CLIENT_TARGET="http://<host-a-lan-ip>:50051"
cargo run --bin client
```

### 結束 server

Terminal A 按 `Ctrl-C`,預期 graceful:

```text
[service] ctrl_c received, shutting down
[ingest] stopped: received=N snapshot.len=10 gaps=0 ...
[server] shut down gracefully
```

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
| `SIM_MAX_MESSAGES` | (無上限) | 訊息上限,觸發 EOF |
| `MDS_CLIENT_TARGET` | `http://127.0.0.1:50051` | client 連線 endpoint |
| `MDS_CLIENT_FIGI` | `BBG000000001` | client `GetSnapshot` FIGI |
| `MDS_CLIENT_FIGIS` | (同上) | client `Subscribe` FIGI 列表(逗號分隔) |
| `MDS_CLIENT_SECS` | `3` | client `Subscribe` 持續秒數 |
| `MDS_CLIENT_VERBOSE` | (未設) | 設任意值即啟用 wire payload pretty-print |

feed-sim 自家 env(`SIM_SEED` / `SIM_DEPTH` / `SIM_PACING` / `SIM_BUFFER_SIZE` / `SIM_START_SEQ`)見 `crates/feed-sim/`。

---

## Non-goals(對齊題目「Out of scope」)

當前 codebase 沒有對應入口,刻意保持:

- Persistence / auth / TLS(snapshot 純記憶體,明文 gRPC)
- L3 book reconstruction(`Snapshot::put` 整份覆寫)
- HA / failover / multi-region(單一 process / 單一 endpoint)

## Future work

- 真實 iceoryx2 替換 `feed-sim` —— `Upstream` trait 為此鋪路,只動一個檔。
- Production graceful shutdown —— `Service::run` 在 ingest EOF 分支加 shared shutdown channel,讓 in-flight RPC 走 graceful drain。
- 跨主機自動化測試(docker compose / SSH tunnel)—— 當前由手動指令驗證。
