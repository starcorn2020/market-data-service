# Market Data Service — 整體開發指南

供 AI agent 與工程師在實作 `crates/marketdata-service` 時對齊用。本文聚焦在**整個工作區的不變量、邊界與陷阱**；參數細節在 `crates/feed-sim/src/Congfig.md`。

> 命名選擇：根目錄 `Cargo.toml` 的 workspace member 用底線/連字號的 `marketdata-service`（與 `marketdata-types` 對齊），README 中提到的 `market-data-service` 是同一個 crate 的展示名稱。

---

## 0. 專案目標一句話

> 接住 `feed-sim` 噴出的 `BookMessage` 流，**對外同時提供 request/response（取最新快照）與 pub/sub（推播即時更新）兩種 API**，並保證**任一慢/斷的下游不會影響 ingest 與其他下游**。

`feed-sim` 是黑盒上游，將來會被真實的 iceoryx2 訂閱者替換。所有對它的依賴**只能透過 `feed_sim` crate 的 public API**（即 `FeedSubscriber` / `FeedSample` / `SubscriberConfig` / `Pacing` / `BookMessage` re-export）。

---

## 0.1 明確不做的事（Non-goals）

`README.md` §「Non-goals」原文列出的、**不可進入代碼**的範圍。本節是整份文件的**最高優先級規則**：任何後續章節若與此衝突，以本節為準。

### 0.1.1 清單

| # | Non-goal | 原文 | 在本專案的具體禁區 |
|---|---|---|---|
| N1 | Feed gateway → source 的傳輸 | "The transport from feed gateway to source (hidden on purpose)." | 不可繞過 `FeedSubscriber` 操作 `feed-sim` 內部 channel / 不可自己造數據 |
| N2 | 持久化 / 認證 / TLS | "Persistence, auth, TLS." | snapshot 只放記憶體；gRPC 用明文；無 `--auth` / `--token` / `--tls-*` 任何 flag |
| N3 | 從增量重建 L3 book | "Building an L3 book from increments — `BookMessage` is already a top-10 snapshot." | 不可實作 order-by-order 處理；`BookMessage` 直接整份覆蓋寫進 snapshot 表 |
| N4 | HA / failover / multi-region | "HA, failover, multi-region." | 不可有 `NODE_ID` / `NODE_REGION` / 心跳 / 健康檢查 routing / 多副本協調 / 跨節點 metadata |

### 0.1.2 三條派生規則（Meta-rules）

**MR-1（最重要）：設計點對應到 Non-goals 就立刻刪除。**

> 不要靠「加一點也無所謂」「未來可能要用」「展示工程感」的心態繞過。
> 任何 PR / 設計討論若提出對應 N1–N4 的功能，第一反應是**刪掉**，第二反應才是「能不能改寫成不違規的等價物」。

**MR-2：Non-goals 不是「禁止討論」，而是「禁止進入代碼」。**

> 允許在 `README.md` 的 "Future work" 段落、設計文件、面試 follow-up 時口頭提及。
> 不允許出現在 `Cargo.toml` 依賴、env vars、CLI flags、proto 訊息、模組名稱、註解中的 TODO。

**MR-3：判斷模糊時，預設視為違規。**

> 例如：「給 client 一個欄位讓它知道是哪台機器在 serve」聽起來無害，但這就是 N4 的入口（隱含 multi-instance 概念）。
> 預設不做；如果真的需要，先回到 README 找對應條款，找不到就**不做**。

### 0.1.3 對應的代碼層面禁止項

| 想做的事 | 違反 | 替代做法 |
|---|---|---|
| 自己 mock 一份 `BookMessage` 給測試用 | N1 | 用 `feed-sim` + `SIM_MAX_MESSAGES` + 固定 `SIM_SEED` |
| 把 snapshot 寫進 sqlite / 檔案 | N2 | 純記憶體 `DashMap<Figi, BookMessage>` |
| 對 gRPC 加 token 驗證 | N2 | 拒絕需求；明文即可 |
| 自己合併 increments 重建 book | N3 | 直接 `snapshots.insert(msg.figi, *msg)` |
| 加 `NODE_ID` / `NODE_REGION` env | N4 | 刪除；如需展示「節點」概念寫進 README future work |
| 跑兩個 instance 互備 | N4 | 單一進程；多 client 連同一個 endpoint |
| `GetNodeInfo` / `Health` 之類自報家門 RPC | N4 | 刪除；gRPC 內建的健康檢查也不要開 |

### 0.1.4 唯一例外：跨主機支援（README §5）

`README.md` 第 5 條明確要求 "Works for clients on the same host **and** on a remote machine"，這**不屬於** multi-region：

| 屬於 README §5 | 屬於 N4 multi-region |
|---|---|
| 同一個 service 同時 serve 本機 + 遠端 client | 多份 service instance 分散在多地 |
| 監聽 `0.0.0.0` 而不是 `127.0.0.1` | NODE_ID / NODE_REGION / 跨節點同步 |
| TCP / gRPC 跑在 LAN 上 | 故障切換、跨區複製 |

實作上**只需要監聽 `0.0.0.0`，加上一句 "tested on localhost and LAN" 的 README 說明**即可滿足 §5，不需要任何額外的「分散式」設施。

---

## 1. 工作區結構

```
market-data-service/
├── Cargo.toml                       # workspace 根；resolver = "3", edition = "2024"
├── README.md                        # 題目描述（不可改）
├── AI_DEV_GUILDELINE.md             # 本檔
└── crates/
    ├── marketdata-types/            # 共享資料型別（只讀依賴）
    │   └── src/lib.rs               # BookMessage / Figi / BookLevel / Flags / ExchangeHeader
    ├── feed-sim/                    # 上游模擬器（不可改實作；只可讀其 public API）
    │   ├── src/
    │   │   ├── lib.rs               # 對外 re-export
    │   │   ├── subscriber.rs        # FeedSubscriber + Pacer + 背景執行緒
    │   │   ├── sample.rs            # FeedSample 包裝（Deref → BookMessage）
    │   │   ├── generator.rs         # 決定性 BookMessage 流（每 FIGI 振盪）
    │   │   ├── config.rs            # SubscriberConfig / Pacing / from_env
    │   │   ├── rng.rs               # xoshiro256** PRNG（不依賴 `rand`）
    │   │   ├── error.rs             # FeedSimError + Result
    │   │   └── Congfig.md           # ★ 參數總表（須先讀）
    │   └── examples/print_messages.rs  # 官方調用範本，照抄即可
    └── marketdata-service/          # ★ 本次要實作的 crate
        ├── Cargo.toml               # 依賴 anyhow（理由見其註解）
        └── src/
            ├── lib.rs               # 對外 API 入口（目前空）
            └── client.rs            # 範例 client（README 第 6 條要求）
```

### 1.1 各 crate 角色

| Crate | 性質 | 你能做的事 |
|---|---|---|
| `marketdata-types` | **只讀依賴** | `use` 它的型別；**不可新增**或修改其中任何 pub 型別 |
| `feed-sim` | **只讀依賴** | 只能透過 `lib.rs` 暴露的 5 個 item 使用；不可繞過 `FeedSubscriber` 直接操作 channel |
| `marketdata-service` | **你的主場** | 自由實作；新增模組、依賴需在 `Cargo.toml` workspace 表中聲明 |

### 1.2 依賴方向（不可形成環）

```
marketdata-service ──> feed-sim ──> marketdata-types
                  └────────────────────────┘
```

`marketdata-service` 可同時依賴 `feed-sim` 與 `marketdata-types`，但**不要在介面上洩漏 `feed_sim` 型別**（除了 re-export `BookMessage`）；換 iceoryx2 時你只想動 ingest 內部，不想改對外 API。

---

## 2. 共享型別：`marketdata-types`

### 2.1 必須記住的事實

| 事實 | 影響 |
|---|---|
| `BookMessage` 是 `#[repr(C)] + Copy` | 可隨意按值傳遞；fan-out 時直接 `*sample` 拷貝即可，不要包 `Arc` 多此一舉 |
| `Figi` 是 `[u8; 12]` 且 `Copy + Hash + Eq` | **直接拿來當 HashMap key**；不要做成 `String` |
| `MAX_BOOK_DEPTH = 10` | `bids` / `asks` 是固定長度陣列；`bid_count`/`ask_count` 才是有效檔數 |
| `bids/asks` 須用 `.bids()` / `.asks()` 切片才安全 | 直接 index `m.bids[i]` 不會 panic 但會讀到無效資料 |
| `gateway_seq: u64` 全流嚴格遞增 | **gap 檢測的唯一可靠依據**；`packet_seq` 是 per-FIGI 的，較弱 |
| `gateway_ts` 是 wall-clock ns | 不要用它做 latency 量測（會被系統時鐘飄移污染）；用 `Instant` |
| `BookMessage` size ≈ 408 bytes（10 levels × 2 sides × 16B + header） | 拷貝便宜但不是免費；高頻 fan-out 時注意 cache footprint |

### 2.2 Figi 使用範例

```rust
use marketdata_types::Figi;

let key: Figi = "BBG000000123".parse().unwrap();   // FromStr 是 Infallible
map.insert(key, snapshot);                          // 直接當 HashMap key
println!("figi = {}", key.as_str());                // 自動 trim NUL padding
```

不要把 `figi.as_str()` 存進 HashMap key 再去查 —— 多一次 UTF-8 檢查，無意義。

---

## 3. 上游：`feed-sim` 的調用契約

**參數細節**：請直接讀 `crates/feed-sim/src/Congfig.md`，本節只列**行為層面**的不變量。

### 3.1 五個對外型別

```rust
pub use feed_sim::{
    FeedSubscriber,    // 主物件，建構即啟動背景生成
    FeedSample,        // receive() 回傳；Deref<Target = BookMessage>
    SubscriberConfig,  // 配置 struct
    Pacing,            // Steady / Bursty
    BookMessage,       // re-export from marketdata-types
};
```

### 3.2 三個方法的契約

| 方法 | 阻塞？ | 回傳 | 語意 |
|---|---|---|---|
| `receive() -> Result<Option<FeedSample>>` | **不阻塞** | `Ok(Some)` / `Ok(None)` | `None` ≠ 結束，只是「當下緩衝為空」 |
| `wait(Duration) -> Result<(), ()>` | **阻塞 `duration`** | `Ok(())` 繼續 / `Err(())` 結束 | **唯一合法的結束訊號** |
| `total_generated() -> u64` | 不阻塞 | 累積成功入 buffer 的數量 | 不等於消費者收到的數量（被丟的不算） |

`receive` 內部對 `TryRecvError::Disconnected` 也回 `Ok(None)`（防御性），所以**呼叫端永遠不會在 `receive()?` 上 panic**。

### 3.3 標準調用骨架（**請照抄、不要創新**）

```rust
let sub = FeedSubscriber::new(SubscriberConfig::from_env()?)?;
let poll = Duration::from_millis(50);

while sub.wait(poll).is_ok() {
    while let Some(sample) = sub.receive()? {
        on_message(&sample);   // sample 自動 Deref 為 &BookMessage
    }
}
// 退出迴圈後 sub 被 Drop → 背景執行緒 stop + join（500ms 內）
```

雙層 loop 是**設計上強制**的：

- 沒有外層 `wait()` → 變成忙等，CPU 100%。
- 沒有內層 `while let Some` → 每 `poll` 只取一筆，buffer 必然溢位丟訊息。

### 3.4 Slow consumer 語義（**最重要**）

`feed-sim` 的 buffer 滿時走 `TrySendError::Full` 分支**直接丟棄**這則訊息，**絕不阻塞生成器**。這是真實行情系統的標準行為。

> **這條規則必須一路傳染到 `marketdata-service` 的所有 fan-out 點**：每個 pub/sub 訂閱者都應該有自己的有界 queue，滿了就丟（或標記 lag，或斷線該訂閱者），**任何下游永遠不能反壓 ingest**。

### 3.5 生命週期與並發

| 規則 | 理由 |
|---|---|
| `FeedSubscriber` **不可共享**到多個 ingest 執行緒 | 內部 `Receiver` 是 `!Sync`，編譯就過不去 |
| **整個 service 只能有一個 ingest 點**呼叫 `receive()` | 多個 `receive()` 會破壞 `gateway_seq` 連續性檢測 |
| `Drop` 時自動 `stop + join` | **不要**自己包 `Arc<Mutex<>>`、不要寫 `shutdown()` 方法 |
| `from_env()` 讀進程級 env var | 啟動時讀一次後快取；不要在 hot path 反覆呼叫 |

### 3.6 確定性流

| 固定下列三項即可重現 | 不固定的 |
|---|---|
| `seed`、`start_seq`、`instruments`、`depth` | `gateway_ts`（wall-clock）、`header.time_ns`（wall-clock） |

回放測試只能比對「除 timestamp 外」的欄位（參考 `generator.rs::deterministic_payloads_for_same_seed`）。

---

## 4. `marketdata-service` 實作約束

### 4.1 必備的四個邏輯區塊

```
                       ┌──────────────────────────┐
                       │   FeedSubscriber (黑盒)  │
                       └────────────┬─────────────┘
                                    │ receive()
                                    ▼
        ┌───────────────────────────────────────────────┐
        │            Ingest 任務（單一執行緒）          │
        │  - 拉訊息                                      │
        │  - 維護 gateway_seq gap 統計                   │
        │  - 寫快照表                                    │
        │  - 廣播給訂閱者                                │
        └────────┬────────────────────────┬─────────────┘
                 │                        │
                 ▼                        ▼
        ┌────────────────┐       ┌────────────────────┐
        │  Snapshot 表    │       │  Subscription bus  │
        │  Figi → Book    │       │  Figi → [sub_id]   │
        │  （供 RPC 讀）  │       │  （fan-out 推播）  │
        └────────────────┘       └────────────────────┘
                 ▲                        ▲
                 │ 取最新                  │ subscribe / unsubscribe
                 ▼                        ▼
        ┌──────────────────────────────────────────────┐
        │             Transport / wire 層              │
        │  - 同主機 + 跨主機（README 第 5 條）         │
        └──────────────────────────────────────────────┘
```

### 4.2 各區塊的關鍵約束

#### Ingest

- **單一執行緒**獨佔 `FeedSubscriber`。
- 每筆訊息**先寫快照表、後廣播**（廣播失敗不可拖累快照寫入）。
- 維護 `last_seen_gateway_seq`：若 `msg.gateway_seq != prev + 1` 紀錄一筆 gap event（不要 panic）。
- **不要做 IO、不要 await 網路、不要持有跨訊息的鎖**。每筆訊息的處理時間決定 buffer 是否來得及排空。

#### Snapshot 表

- 結構：`Figi → BookMessage`（值類型；`Copy` 進去）。
- 讀寫比例極端不對稱：寫 = ingest 速率（每秒上千），讀 = RPC 速率（每秒個位數～數十）。
- 推薦選擇（按推薦順序）：
  1. `DashMap<Figi, BookMessage>` — 簡單、shard lock、讀寫都不阻塞。
  2. `Arc<RwLock<HashMap<Figi, BookMessage>>>` — 讀多寫少時尚可，但 ingest 是寫多。
  3. 自己寫無鎖：**不要**，三天時間不夠。
- **讀回應一律回拷貝**（`*entry.value()`），不要回 `&BookMessage` 跨執行緒。

#### Subscription bus（pub/sub fan-out）

- 每個訂閱者 = 一個有界 `mpsc` / `broadcast` channel slot。
- 廣播策略：
  - **每 FIGI 一個 broadcast channel**（訂閱者數量少時最直觀）；或
  - **單一廣播 + 訂閱者本地過濾**（訂閱者多但 FIGI 集中時更省）。
  - 三天 deliverable 建議走前者，理由：實作簡單、慢消費者隔離容易。
- 滿了的選擇（**寫進設計文件，二選一**）：
  - **Drop + lag counter**：丟掉最舊的，下次正常推播。
  - **Disconnect**：踢掉這個訂閱者，要求其重連 + 重取快照。
- **絕不可 `send().await` 阻塞 ingest**。一律用 `try_send`。

#### Transport（同主機 + 跨主機） ★ 已拍板：gRPC (tonic)

README 第 5 條要求 "Same wire protocol or different — your call, justify it."。本專案**選定 gRPC (tonic)**，單一協定同時 serve 本機與遠端 client。

| 決策 | 內容 |
|---|---|
| 框架 | `tonic` + `prost`，build 期由 `tonic-build` 從 `proto/marketdata.proto` 生成 |
| Request/Response | **unary RPC** `GetSnapshot` |
| Pub/Sub | **server-streaming RPC** `Subscribe` |
| 序列化 | protobuf（tonic 內建） |
| 監聽 | `0.0.0.0:<port>`，本機/遠端共用 |
| TLS | **不開**（N2 non-goal） |

**Justification（必須抄進交付 README，見 §14）**：

1. `unary` + `server-streaming` 與題目兩個 API **一對一對應**，wire schema 自帶語意，reviewer 無須猜測 framing 規則。
2. `.proto` 檔本身就是「寫下來的 design decision」，正面命中 README 「Write them down」要求。
3. 跨主機免費（HTTP/2 over TCP），不需要另外寫 length-prefixed framing。
4. Stream cancellation / deadline 內建：client 斷線時 server 立刻收到 `Status::Cancelled`，訂閱清理自動觸發。

**唯一陷阱**：tonic server-streaming 預設會把背壓傳回 producer。**必須**在 stream handler 內加一層 bounded `tokio::sync::mpsc::channel(SUBSCRIBER_QUEUE_SIZE)`，滿了走 `try_send` 丟最舊 + `dropped_total++`，**嚴禁** `tx.send().await`（否則違反 I1）。安全接法見 §4.3.4。

依賴（加進 `crates/marketdata-service/Cargo.toml`）：

```toml
[dependencies]
tonic = "0.12"
prost = "0.13"
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"
dashmap = "6"

[build-dependencies]
tonic-build = "0.12"
```

Reviewer 環境需先安裝 `protoc`（交付 README 必須提及）。

### 4.3 對外 API 形狀（已定案）

#### 4.3.1 Proto schema (`proto/marketdata.proto`)

```proto
syntax = "proto3";
package marketdata.v1;

service MarketData {
    rpc GetSnapshot(GetSnapshotRequest) returns (SnapshotResponse);
    rpc Subscribe(SubscribeRequest)     returns (stream BookUpdate);
}

message GetSnapshotRequest { string figi = 1; }

message SnapshotResponse {
    oneof result {
        Book   found   = 1;
        NotYet not_yet = 2;
    }
}
message NotYet {}

message SubscribeRequest {
    repeated string figis = 1;
}

message BookUpdate {
    Book   book          = 1;
    // ★ Server 端累積丟失計數,跨主機 lag 的唯一可靠管道。
    //   server 端每丟一筆就 +1,並在下一次成功送出時帶上當前值。
    //   client 用 (curr.dropped_total - prev.dropped_total) 算出
    //   兩筆之間的損失量。
    uint64 dropped_total = 2;
}

message Book {
    string figi        = 1;
    uint64 gateway_seq = 2;
    int64  gateway_ts  = 3;
    repeated Level bids = 4;
    repeated Level asks = 5;
}
message Level {
    double price  = 1;
    float  qty    = 2;
    uint32 orders = 3;
}
```

#### 4.3.2 Rust 內部 API (`lib.rs`)

```rust
pub struct Service { /* … */ }

impl Service {
    pub fn new(cfg: ServiceConfig) -> anyhow::Result<Self>;
    pub async fn run(self) -> anyhow::Result<()>;   // 跑 ingest + tonic server
}

pub enum SnapshotResponse {
    Found(BookMessage),
    NotYet,                              // README 第 3 條:明確的「沒資料」
}

pub struct Subscription {
    rx: tokio::sync::mpsc::Receiver<BookMessage>,
    dropped: Arc<AtomicU64>,
}
impl Subscription {
    pub async fn next(&mut self) -> Option<BookMessage>;
    pub fn dropped_total(&self) -> u64;  // ★ 進 wire 的 dropped_total 來源
}
```

#### 4.3.3 跨主機 lag 必須走 wire payload

**問題**：`Subscription::dropped_total()` 是 server 端 in-process 計數，遠端 client 看不到。若不傳給 client，跨主機場景下 client 完全無感「我漏了東西」。

**解法**：每筆 `BookUpdate` 的 `dropped_total` 欄位**強制帶上 server 端的累積丟失數**。client 端做差分：

```rust
let lost_this_interval = curr.dropped_total - prev.dropped_total;
if lost_this_interval > 0 {
    tracing::warn!("lost {} updates since last", lost_this_interval);
}
```

**為什麼用累積值而非 per-message delta**：

| | 累積值 ✓ | Delta ✗ |
|---|---|---|
| Client 重連對齊 | 自動：拿到第一筆就有基準 | 需要協商起點 |
| 丟失/亂序容忍 | 只要拿到任一筆就能算 | 中間斷一筆 client 就算錯 |
| Server 實作 | `AtomicU64::fetch_add(1)` | 要記住「上次送了多少」 |
| 上界 | `u64` 撐到熱寂 | n/a |

#### 4.3.4 broadcast → gRPC stream 的安全接法

這段是 **§4.2 「唯一陷阱」** 的標準解。**抄這段，不要創新**。

```rust
async fn subscribe(
    &self,
    req: Request<SubscribeRequest>,
) -> Result<Response<Self::SubscribeStream>, Status> {
    let (out_tx, out_rx) = mpsc::channel::<Result<BookUpdate, Status>>(
        self.cfg.subscriber_queue_size,
    );
    let dropped = Arc::new(AtomicU64::new(0));
    let mut bus_rx = self.bus.subscribe(req.into_inner().figis);

    let dropped_c = dropped.clone();
    tokio::spawn(async move {
        while let Some(book) = bus_rx.recv().await {
            let upd = BookUpdate {
                book: Some(book.into_proto()),
                dropped_total: dropped_c.load(Ordering::Relaxed),
            };
            // ★ try_send,不能 await,否則違反 I1
            match out_tx.try_send(Ok(upd)) {
                Ok(_) => {}
                Err(TrySendError::Full(_))   => { dropped_c.fetch_add(1, Ordering::Relaxed); }
                Err(TrySendError::Closed(_)) => { break; }  // client 已斷
            }
        }
    });

    Ok(Response::new(ReceiverStream::new(out_rx)))
}
```

`client.rs` 提供同步 demo：先 `GetSnapshot(figi)`，再 `Subscribe([figi…])`，跑 N 秒後 print 收到數量與最終 `dropped_total`。

---

## 5. 四大不變量（任何 PR / 提交都不可破壞）

| # | 不變量 | 守護方式 |
|---|---|---|
| **I1** | Ingest 執行緒永不被任何下游阻塞 | 所有 fan-out 一律 `try_send`；嚴禁 `send().await` / `lock().write()` 不限時 |
| **I2** | 慢/斷的訂閱者不影響其他訂閱者與快照表 | 訂閱者隔離在獨立 channel；寫快照表在廣播之前完成 |
| **I3** | `feed-sim` 的 public API 是唯一接點 | 不繞過 `FeedSubscriber` 操作 channel、不依賴 `feed_sim` 私有模組 |
| **I4** | 對外 API 不洩漏 `feed_sim::*` 型別（`BookMessage` 除外） | 換 iceoryx2 時只動 ingest 內部，不動 wire / 不動 client |

---

## 6. 錯誤處理約定

| 層 | 推薦做法 |
|---|---|
| `marketdata-service`（application） | `anyhow::Result` + `.context("…")`（已選定，見 `Cargo.toml` 註解） |
| 真正的 library 介面（如未來抽出 `marketdata-protocol`） | `thiserror` 定義具體 enum |
| `feed-sim` 邊界 | `FeedSimError::Config` 屬於啟動期錯誤，**fail-fast**；`Disconnected` 不會由 `receive()` 拋出，目前由 `wait()` 的 `Err(())` 表達 |

**不要**：

- 在 ingest 迴圈裡 `unwrap()` —— 一條訊息壞掉不應該殺整個服務。
- 把 `anyhow::Error` 包進公開介面 —— 對外用具體錯誤型別。
- 吞錯誤而不記 log —— 至少要 `tracing::warn!` 一行。

---

## 7. 並發與所有權

### 7.1 推薦 runtime

- 用 **`tokio` (full features)** 跑 transport 層。
- **Ingest 用 `std::thread` 而不是 `tokio::task`**：
  - 它是 CPU-bound + busy-poll，會佔住整個 worker。
  - 用 `std::thread::spawn` + `tokio::sync::mpsc` 把資料丟給 async 端。
- Ingest → async 的橋樑用 `tokio::sync::mpsc::channel(buf)` 而不是 `std::sync::mpsc`：避免 async 端 block_on。

### 7.2 共享狀態優先順序

1. **無共享**（每 task 自己一份）→ 永遠優先。
2. `Arc<DashMap<…>>` / `Arc<broadcast::Sender<…>>` → 大部分場景。
3. `Arc<RwLock<…>>` → 讀遠多於寫時。
4. `Arc<Mutex<…>>` → 最後手段，且絕不持鎖跨 `.await`。

---

## 8. 測試策略

| 測試類型 | 範圍 | 工具 |
|---|---|---|
| 單元測試 | snapshot 表更新、broadcast lag 處理、protocol 編解碼 | `#[test]` |
| 整合測試 | 起 `Service::run` + 連 client，跑 `SIM_MAX_MESSAGES=1000` 後驗證 | `tests/` 目錄 |
| 回歸：確定性 | 同 `seed` 兩次跑，比對 `gateway_seq` 序列 | 已由 `feed-sim` 保證 |
| 壓力：slow consumer | 起 2 個訂閱者，一個故意 sleep；驗證另一個不被影響 | 整合測試 |
| 壓力：disconnect | 訂閱中斷後驗證 ingest 仍正常、其他訂閱者無感 | 整合測試 |

**最低交付**：每個 fan-out / snapshot 路徑都要有一個對應的整合測試，否則 reviewer 無法相信 I1/I2。

---

## 9. 效能 / 延遲基線

`feed-sim` 預設 1000 msg/s，壓測可推到 50k msg/s（`Congfig.md` §5）。

| 量測點 | 目標 |
|---|---|
| Ingest 端對端 latency（`receive()` → 寫完 snapshot） | < 100 µs (p99) |
| Fan-out 廣播 latency（snapshot 寫完 → 訂閱者拿到） | < 500 µs (p99) |
| Snapshot RPC roundtrip（localhost） | < 1 ms (p99) |
| Slow consumer 對其他訂閱者的影響 | 0（這是不變量 I2 的量化要求） |

量測用 `Instant::now()`（**不要**用 `gateway_ts`），數據先寫進 ring buffer，背景執行緒定期 flush 到 log，避免量測本身污染 hot path。

---

## 10. README 任務逐條對照

| README 條目 | 對應實作位置 | 完成判定 |
|---|---|---|
| ① 消費 `BookMessage` | `ingest.rs` | 整合測試能跑滿 `SIM_MAX_MESSAGES` |
| ② Per-Figi 最新快照 | `snapshot.rs` | 寫入 → 立刻能 `get_snapshot` 讀到該 FIGI 最新一筆 |
| ③ Request/response API | `service.rs` + `client.rs` | RPC 取得 `SnapshotResponse::Found` / `NotYet` |
| ④ Pub/sub API + 慢消費者隔離 | `pubsub.rs` | 整合測試「slow consumer 不影響其他」通過 |
| ⑤ 同主機 + 跨主機 | `transport.rs` | 同份 server 二進位以 `127.0.0.1` 與 LAN IP 各跑一次 |
| ⑥ Sample client | `client.rs` | `cargo run --bin client` 同時 demo 兩種 API |

---

## 11. 反模式（**禁止**）

| 反模式 | 為什麼錯 | 正確做法 |
|---|---|---|
| 把 `FeedSubscriber` 包進 `Arc<Mutex<>>` 多執行緒共享 | `Receiver: !Sync`；且 mutex 化會破壞延遲 | 單一 ingest 執行緒 |
| 用 `tokio::task::spawn(async move { loop { sub.receive() ... }})` | 同步阻塞 API 阻塞 tokio worker | `std::thread::spawn` |
| 廣播用 `tx.send().await` | 慢消費者反壓到 ingest（破壞 I1） | `tx.try_send()` + lag counter |
| 對 `Ok(None)` 直接 `break` 結束迴圈 | `None` 不是結束訊號（破壞 §3.2） | 用 `wait()` 的 `Err(())` 判斷 |
| `unwrap()` 在 ingest hot loop | 一條壞訊息殺掉整個服務 | `match` + log + 繼續 |
| 把 `FeedSample` 跨執行緒邊界傳遞 | 雖然 `BookMessage: Copy`，但 `FeedSample` 不一定 | 改傳 `*sample`（`BookMessage` 值） |
| 在 `Snapshot` 表回 `&BookMessage` | 跨執行緒生命週期 nightmare | 回 `BookMessage`（一次拷貝，408 bytes） |
| 把 `feed_sim::FeedSubscriber` 寫進 service 對外 trait | 破壞 I4，未來換 iceoryx2 要重構整個 API | 對外只暴露你自己定義的 trait／struct |
| 用 `BookMessage.gateway_ts` 算 latency | wall-clock 飄移污染量測 | 用 `Instant` 在 ingest 入口打點 |

---

## 12. 開始實作前的檢查清單

啟動每個新模組前對照本檔：

- [ ] 讀過 `Congfig.md`，知道 `SubscriberConfig` 邊界？
- [ ] 你的程式碼有沒有違反 §5 的任何一條不變量？
- [ ] Fan-out 路徑全部走 `try_send`？
- [ ] Ingest 路徑沒有 `.await` / 不限時的鎖？
- [ ] 對外 API 沒有 leak `feed_sim::*`？
- [ ] 對應 README 的哪一條（§10 對照表）？
- [ ] 寫了至少一個對應的整合測試？

---

## 13. TODO（隨實作補完）

- [ ] `marketdata-service` 對 `feed-sim` 的封裝層 trait 定案（為未來 iceoryx2 鋪路；參考 I4）
- [ ] `gateway_seq` gap 偵測：閾值、上報通道、復原語意
- [ ] Backpressure 觀測點：每訂閱者 `dropped_total`、bus 容量、ingest buffer 使用率
- [ ] 跨主機 demo 步驟寫進交付 README（§14.2）

> 已定案、不再列為 TODO 的項目：wire protocol（§4.2 = gRPC）、wire schema（§4.3.1 = proto 定稿）、lag 傳遞（§4.3.3 = `dropped_total` 累積值）。

---

## 14. 最終交付清單（Write them down）

README 明示 "The design decisions are the point. Write them down."。

> **本檔 (`AI_DEV_GUILDELINE.md`) 屬於「內部開發規範」，不可直接當交付物。** 最終 zip 必須包含一份獨立的 **`crates/marketdata-service/README.md`**（或根目錄 `DESIGN.md`），把已經定案的決策**用敘事方式**整理出來，作為架構評分的得分亮點。

### 14.1 必寫的設計決策（每條一個段落，附理由）

| 決策 | 對應本檔章節 | 一句話理由（交付文件中須展開） |
|---|---|---|
| 採用 `DashMap<Figi, BookMessage>` 做 snapshot 表 | §4.2 | shard lock + 讀寫不互斥，ingest 寫多場景最簡單可行 |
| Per-FIGI broadcast channel 做 fan-out | §4.2 | 慢消費者隔離天然成立；訂閱/取消 O(1) |
| 滿了走 drop + `dropped_total` 累計（不 disconnect） | §4.2 / §4.3.3 | client 仍可繼續收後續更新；lag 透過 wire payload 帶回 |
| gRPC (tonic) 作為唯一 wire protocol | §4.2 | `unary` + `server-streaming` 與題目兩個 API 一對一；`.proto` 即 design doc |
| `dropped_total` 用累積值而非 per-message delta | §4.3.3 | client 重連時自動對齊；不依賴有序傳遞 |
| Snapshot RPC 回 `oneof { Found, NotYet }` | §4.3.1 | README 第 3 條要求 "clearly-defined no data yet" |
| Ingest 用 `std::thread`，不用 `tokio::task` | §7.1 | `FeedSubscriber` 是同步阻塞 API，避免吃掉 tokio worker |
| `FeedSubscriber` 不對外暴露（包在私有 `upstream` 模組） | I4 (§5) | 換 iceoryx2 時 client / wire schema 0 改動 |

### 14.2 必寫的執行說明

| 段落 | 內容 |
|---|---|
| Build | `cargo build --release`；`protoc` 安裝指引（macOS `brew install protobuf` / Ubuntu `apt install protobuf-compiler` / Windows scoop） |
| Test | `cargo test` 全部通過；個別跑 `cargo test -p marketdata-service slow_consumer` 等關鍵案例 |
| Run server | 完整 env var 清單（最少 `SIM_INSTRUMENTS` / `SIM_RATE_HZ` / `LISTEN`） |
| Run sample client | 本機 (`grpc://127.0.0.1:50051`) 與跨主機 (`grpc://<lan-ip>:50051`) 各一段命令 |
| 預期輸出 | 範例 stdout，reviewer 不需要猜該看什麼 |

### 14.3 必寫的「Future work」（口頭討論可加分，**不入代碼**）

> 與 §0.1 Non-goals 對齊：明確列出「我知道這些有價值，但這次刻意不做」。

- 真實 iceoryx2 替換 `feed-sim` 的步驟（trait 抽象在 I4 已預留接口）
- Multi-region 部署考量（與 **N4** 對齊：「現階段刻意不做」）
- Persistent snapshot for cold start（與 **N2** 對齊）
- Auth / TLS（與 **N2** 對齊）
- L3 book reconstruction from increments（與 **N3** 對齊）
- Snapshot snapshot-on-subscribe（client subscribe 時自動帶最新一份）

### 14.4 交付文件「不要做的事」

- **不要**把 `AI_DEV_GUILDELINE.md` 直接複製進 service README — 它是給開發過程用的，太細、太硬，會把 reviewer 淹沒。
- **不要**在交付文件中重複本檔 §11 反模式列表 — reviewer 想看的是「你決定做什麼 + 為何」，不是「你不會犯什麼錯」。
- **不要**把未實作的功能寫進 §14.1 — 只寫**真的進了代碼**的決策；半成品全部歸到 §14.3 Future work。
- **不要**寫超過 2 頁 — take-home 評分時間有限，密度比長度重要。
