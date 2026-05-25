//! Integration tests for the simulator. Exercises the full
//! `new → wait → receive → drop` lifecycle from outside the crate.

use std::time::{Duration, Instant};

use feed_sim::{FeedSubscriber, Pacing, SubscriberConfig};

fn capped(rate_hz: u32, max: u64) -> SubscriberConfig {
    SubscriberConfig {
        instruments: 8,
        rate_hz,
        pacing: Pacing::Steady,
        depth: 5,
        max_messages: Some(max),
        seed: 0xABCD_1234,
        start_seq: 1,
        buffer_size: 256,
    }
}

#[test]
fn end_to_end_drains_all_messages() {
    let cap = 200;
    let sub = FeedSubscriber::new(capped(20_000, cap)).unwrap();
    let mut count = 0u64;
    let deadline = Instant::now() + Duration::from_secs(3);

    'outer: while Instant::now() < deadline {
        match sub.wait(Duration::from_millis(20)) {
            Ok(()) => {
                while let Some(_s) = sub.receive().unwrap() {
                    count += 1;
                }
            }
            Err(()) => {
                while let Ok(Some(_s)) = sub.receive() {
                    count += 1;
                }
                break 'outer;
            }
        }
    }
    assert_eq!(count, cap);
}

#[test]
fn deref_to_book_message() {
    let sub = FeedSubscriber::new(capped(50_000, 1)).unwrap();
    // Wait for the single message to be generated.
    for _ in 0..50 {
        if let Ok(Some(sample)) = sub.receive() {
            // Both deref and AsRef should work.
            assert_eq!(sample.gateway_seq, 1);
            let _ = sample.figi.as_str();
            assert!(sample.best_bid().is_some());
            assert!(sample.best_ask().is_some());
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("never received the single capped message");
}
