# Market Data Service 開發筆記

供 `marketdata-service` 在串接 `feed-sim` 時的參考。新增實作前先對照「參數」一節確認界線。

---

## 參數

所有參數定義在 `crates/feed-sim/src/config.rs` 的 `SubscriberConfig`，可透過環境變數或直接建構 struct 傳入。底層強制不變量會在 `SubscriberConfig::validate()` 與 `FeedSubscriber::new()` 中 fail-fast。

### 1. `SubscriberConfig` 主參數表

| 參數 | 環境變數 | 型別 | 預設值 | 合法範圍 | 越界 / 不合法行為 | 備註 |
|---|---|---|---|---|---|---|
| `instruments` | `SIM_INSTRUMENTS` | `u32` | `100` | `> 0` | `FeedSimError::Config("instruments must be > 0")` | 模擬的 FIGI 數量 |
| `rate_hz` | `SIM_RATE_HZ` | `u32` | `1_000` | `> 0` | `Config("rate_hz must be > 0")` | 所有 FIGI 合計目標速率 (msg/s) |
| `pacing` | `SIM_PACING` | `Pacing` enum | `Steady` | `steady` / `bursty:N` (N ≥ 1) | 解析失敗或 `burst_size == 0` 皆回 `Config` | 詳見下方 §2 |
| `depth` | `SIM_DEPTH` | `u8` | `5` | `1..=10` (`MAX_BOOK_DEPTH`) | `Config("depth must be in 1..=10 (got X)")` | 每則訊息盤口檔位數 |
| `max_messages` | `SIM_MAX_MESSAGES` | `Option<u64>` | `None` (無限) | 任意 `u64` | 不校驗 ⚠️ `Some(0)` 目前會立即停止 | 總訊息上限 |
| `seed` | `SIM_SEED` | `u64` | `0xDEAD_BEEF_CAFE_F00D` | 任意 (含 0) | 不校驗 | 決定性隨機種子，固定即可重現流 |
| `start_seq` | `SIM_START_SEQ` | `u64` | `1` | 任意 | 不校驗 | `gateway_seq` 起始值 |
| `buffer_size` | `SIM_BUFFER_SIZE` | `usize` | `1024` | `> 0` | `Config("buffer_size must be > 0")` | 內部有界通道容量；滿了**丟最舊的** |

> **設計約定**：呼叫端拿到 `Ok(SubscriberConfig)` 後可假設上表所有不變量都已成立，不需重複校驗。

### 2. `Pacing` 節奏參數

| 變體 | 字串格式 (`SIM_PACING`) | 子欄位 | 子欄位界線 | 行為 |
|---|---|---|---|---|
| `Pacing::Steady` | `steady` (大小寫無關，前後可有空白) | — | — | 間隔固定 `1/rate_hz` 秒 |
| `Pacing::Bursty { burst_size }` | `bursty:<N>` | `burst_size: u32` | `> 0` | 連續發 N 則後 sleep，使長期速率仍為 `rate_hz` |

字串解析錯誤對照：

| 輸入 | 結果 |
|---|---|
| `"steady"` / `"STEADY"` / `"  steady  "` | `Ok(Steady)` |
| `"bursty:32"` | `Ok(Bursty { burst_size: 32 })` |
| `"bursty:0"` | parser 通過，但 `validate()` 拒絕 |
| `"bursty:abc"` | `Config("invalid SIM_PACING bursty:N ...")` |
| 其他字串 | `Config("SIM_PACING must be 'steady' or 'bursty:N' ...")` |

### 3. 跨 crate 常數

| 常數 | 定義位置 | 值 | 用途 |
|---|---|---|---|
| `MAX_BOOK_DEPTH` | `marketdata-types::lib` | `10` | `BookMessage.bids` / `asks` 陣列長度；`depth` 上界 |

### 4. 實作時要注意的「規格陷阱」

開發 `marketdata-service` 串接時，下列項目當前的 validator **沒擋**，需要在 service 層自行決定策略：

| 項目 | 現況 | 建議處理 |
|---|---|---|
| `max_messages = Some(0)` | 當前實作會立刻結束 | service 層若視為「無限」應改傳 `None`；若視為非法應在外層擋掉 |
| `buffer_size` 上界 | 僅校驗 `> 0`，無上界 | 由運維/配置層自律，避免巨量分配 |
| `start_seq = 0` | 允許 | 若有「seq 必須 ≥ 1」的下游假設，需在 service 層校驗 |
| `seed = 0` | 允許 | 若 RNG 對 0 種子敏感，需測試確認 |
| `from_env()` 並發 | 依賴進程級 env var | 測試中避免多執行緒同時呼叫；建議在 service 啟動時呼叫一次後快取 |

### 5. 推薦設定 (參考)

| 場景 | `instruments` | `rate_hz` | `pacing` | `depth` | `buffer_size` |
|---|---|---|---|---|---|
| 本地 smoke test | 10 | 100 | `steady` | 5 | 256 |
| 開發 / 單測 | 100 (預設) | 1_000 (預設) | `steady` | 5 | 1024 |
| 壓力測試 | 500 | 50_000 | `bursty:128` | 10 | 8192 |
| 重現性回放 | 固定 | 固定 | 固定 | 固定 | 固定 + 固定 `seed` / `start_seq` |

### 6. 錯誤型別 (`crates/feed-sim/src/error.rs`)

```rust
pub enum FeedSimError {
    Config(String),     // 所有參數相關錯誤都走這條
    Spawn(String),      // 背景產生執行緒啟動失敗
    Disconnected,       // 上游已停且 buffer 排空
}
pub type Result<T> = std::result::Result<T, FeedSimError>;
```

`marketdata-service` 在 `recv` 迴圈中至少要處理 `Disconnected` 作為正常結束訊號。

---

## TODO (待補)

- `marketdata-service` 對 `feed-sim` 的封裝層 API 設計
- 序列號 (`gateway_seq`) 連續性與 gap 偵測策略
- backpressure 行為觀測 (`buffer_size` 滿載丟最舊)
