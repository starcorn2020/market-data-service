# `feed-sim`

In-process stand-in for the upstream feed. Black box — yields
`BookMessage` records through the same subscriber-style API the real
shared-memory gateway exposes, so swapping in the production source
later is mechanical.

## Usage

```rust
use std::time::Duration;
use feed_sim::{FeedSubscriber, SubscriberConfig};

let sub = FeedSubscriber::new(SubscriberConfig::from_env()?)?;

while sub.wait(Duration::from_millis(100)).is_ok() {
    while let Some(sample) = sub.receive()? {
        // `sample` derefs to &BookMessage
        handle_book(&sample);
    }
}
```

- `receive()` is non-blocking. `Ok(Some)` when there's a message,
  `Ok(None)` when the buffer is momentarily empty *or* the stream has
  ended.
- `wait(d)` sleeps for `d` and returns `Err(())` once the upstream is
  done and the buffer is drained — drive your loop off this.
- The internal buffer is bounded; if you fall behind, the simulator
  **drops messages** rather than blocking the generator.

## Configuration

Build a `SubscriberConfig` directly, or load from the environment with
`SubscriberConfig::from_env()`:

| Variable | Type | Default | Notes |
|---|---|---|---|
| `SIM_INSTRUMENTS` | `u32` | `100` | Number of distinct FIGIs. |
| `SIM_RATE_HZ` | `u32` | `1000` | Total msgs/sec across all FIGIs. |
| `SIM_DEPTH` | `u8` | `5` | Book levels per message (1..=10). |
| `SIM_MAX_MESSAGES` | `u64` | _unset_ | Stop after N messages. |
| `SIM_SEED` | `u64` | fixed | Deterministic stream seed. |
| `SIM_START_SEQ` | `u64` | `1` | Initial `gateway_seq`. |
| `SIM_BUFFER_SIZE` | `usize` | `1024` | Internal buffer capacity. |
| `SIM_PACING` | `steady` \| `bursty:N` | `steady` | Inter-arrival pattern. |

Same seed → same payload sequence (modulo wall-clock timestamps).

## Demo

```sh
cargo run --example print_messages -p feed-sim
SIM_INSTRUMENTS=10 SIM_RATE_HZ=500 SIM_MAX_MESSAGES=200 PRINT_EVERY=20 \
    cargo run --example print_messages -p feed-sim
```
