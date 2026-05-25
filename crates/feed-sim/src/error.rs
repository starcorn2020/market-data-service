use std::fmt;

/// Errors returned by the simulator.
#[derive(Debug)]
pub enum FeedSimError {
    /// Configuration was invalid (e.g. zero rate, depth out of range).
    Config(String),
    /// Failed to spawn the background generator thread.
    Spawn(String),
    /// The upstream generator has stopped and the internal buffer is fully
    /// drained. Mirrors the "publisher gone" signal you'd get from a real
    /// subscriber.
    Disconnected,
}

impl fmt::Display for FeedSimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FeedSimError::Config(msg) => write!(f, "invalid feed-sim config: {msg}"),
            FeedSimError::Spawn(msg) => write!(f, "failed to spawn feed-sim generator: {msg}"),
            FeedSimError::Disconnected => f.write_str("feed-sim upstream disconnected"),
        }
    }
}

impl std::error::Error for FeedSimError {}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, FeedSimError>;
