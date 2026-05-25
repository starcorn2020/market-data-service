//! Minimal demo: spin up the simulator and consume from it the way your
//! service would. Tweak the `SIM_*` env vars at runtime.
//!
//! ```sh
//! cargo run --example print_messages -p feed-sim
//! SIM_INSTRUMENTS=10 SIM_RATE_HZ=100 SIM_MAX_MESSAGES=200 PRINT_EVERY=20 \
//!     cargo run --example print_messages -p feed-sim
//! ```

use std::time::Duration;

use feed_sim::{FeedSubscriber, SubscriberConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SubscriberConfig::from_env()?;
    eprintln!("config: {config:#?}");

    let print_every: u64 = std::env::var("PRINT_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let sub = FeedSubscriber::new(config)?;
    let mut received: u64 = 0;

    while sub.wait(Duration::from_millis(100)).is_ok() {
        while let Some(sample) = sub.receive()? {
            received += 1;
            if print_every > 0 && received.is_multiple_of(print_every) {
                let bid = sample.best_bid().map(|l| l.price).unwrap_or(f64::NAN);
                let ask = sample.best_ask().map(|l| l.price).unwrap_or(f64::NAN);
                println!(
                    "#{received:>6}  seq={:>8}  figi={}  bid={:>10.4}  ask={:>10.4}",
                    sample.gateway_seq, sample.figi, bid, ask,
                );
            }
        }
    }

    eprintln!(
        "done: received={received} total_generated={}",
        sub.total_generated()
    );
    Ok(())
}
