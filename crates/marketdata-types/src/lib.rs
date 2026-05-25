//! Shared market data message types.
//!
//! For this assignment you only need [`BookMessage`] and the types it
//! references ([`BookLevel`], [`Figi`], [`BookFlags`], [`ConditionFlags`],
//! [`ExchangeHeader`]). Everything is `#[repr(C)]` and `Copy` so it's safe
//! to move around freely; the gateway upstream of you publishes these as a
//! continuous stream, one per book update.

#![warn(missing_docs)]

use std::fmt;
use std::str::FromStr;

// ---------------------------------------------------------------------------
// Figi
// ---------------------------------------------------------------------------

/// Length of a FIGI identifier (always 12 ASCII characters).
pub const FIGI_LEN: usize = 12;

/// Fixed-size FIGI identifier (Financial Instrument Global Identifier).
///
/// Always 12 ASCII characters; unused bytes are zero-padded. This is the
/// stable instrument key your service should use to index per-instrument
/// state.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct Figi(pub [u8; FIGI_LEN]);

impl Figi {
    /// Returns the FIGI as a UTF-8 string, trimming trailing NUL padding.
    pub fn as_str(&self) -> &str {
        let len = self.0.iter().position(|&b| b == 0).unwrap_or(FIGI_LEN);
        // FIGIs are ASCII by construction.
        std::str::from_utf8(&self.0[..len]).unwrap_or("")
    }

    /// Returns `true` if the FIGI is all zeros (unset).
    pub fn is_empty(&self) -> bool {
        self.0 == [0u8; FIGI_LEN]
    }
}

impl FromStr for Figi {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut buf = [0u8; FIGI_LEN];
        let len = s.len().min(FIGI_LEN);
        buf[..len].copy_from_slice(&s.as_bytes()[..len]);
        Ok(Self(buf))
    }
}

impl fmt::Debug for Figi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Figi({:?})", self.as_str())
    }
}

impl fmt::Display for Figi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

/// Book status flags. See associated constants for individual bits.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct BookFlags(u8);

impl BookFlags {
    /// Book may be incorrect or incomplete.
    pub const SUSPECT: Self = Self(0x01);

    /// Empty flag set.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// All currently-defined flags OR'd together.
    pub const fn all() -> Self {
        Self(Self::SUSPECT.0)
    }

    /// Constructs from raw bits, masking off any unknown bits.
    pub const fn from_bits_truncate(bits: u8) -> Self {
        Self(bits & Self::all().0)
    }

    /// Returns `true` if `other` is fully contained in `self`.
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Raw bit representation.
    pub const fn bits(self) -> u8 {
        self.0
    }
}

impl fmt::Debug for BookFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BookFlags(0x{:02x})", self.0)
    }
}

/// Trading condition flags. See associated constants for individual bits.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct ConditionFlags(u8);

impl ConditionFlags {
    /// Orders can be entered.
    pub const ENTER: Self = Self(0x01);
    /// Orders can be revised.
    pub const REVISE: Self = Self(0x02);
    /// Orders can be cancelled.
    pub const CANCEL: Self = Self(0x04);
    /// Orders can be executed.
    pub const TRADING: Self = Self(0x08);
    /// State not supported for this source.
    pub const UNKNOWN: Self = Self(0x10);
    /// Short-sale restrictions in effect.
    pub const SHORT_RESTRICTED: Self = Self(0x20);
    /// Single-price (e.g. auction) trading mode.
    pub const SINGLE_PRICE: Self = Self(0x40);

    /// Empty flag set.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// All currently-defined flags OR'd together.
    pub const fn all() -> Self {
        Self(
            Self::ENTER.0
                | Self::REVISE.0
                | Self::CANCEL.0
                | Self::TRADING.0
                | Self::UNKNOWN.0
                | Self::SHORT_RESTRICTED.0
                | Self::SINGLE_PRICE.0,
        )
    }

    /// Constructs from raw bits, masking off any unknown bits.
    pub const fn from_bits_truncate(bits: u8) -> Self {
        Self(bits & Self::all().0)
    }

    /// Returns `true` if `other` is fully contained in `self`.
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Raw bit representation.
    pub const fn bits(self) -> u8 {
        self.0
    }
}

impl fmt::Debug for ConditionFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConditionFlags(0x{:02x})", self.0)
    }
}

// ---------------------------------------------------------------------------
// Book messages
// ---------------------------------------------------------------------------

/// Maximum number of book levels (bid or ask) carried in a [`BookMessage`].
pub const MAX_BOOK_DEPTH: usize = 10;

/// Exchange-provided header.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct ExchangeHeader {
    /// Exchange timestamp (nanoseconds since UNIX epoch). `0` if unknown.
    pub time_ns: i64,
    /// Exchange sequence number. `0` if unknown.
    pub seq: u32,
}

/// One price level in the order book.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct BookLevel {
    /// Price. `+/-inf` is permitted (e.g. market orders during auctions).
    pub price: f64,
    /// Aggregate quantity at this level. `0.0` if unknown.
    pub qty: f32,
    /// Number of orders aggregated at this level. `0` if unknown, clipped to `u16::MAX`.
    pub orders: u16,
}

/// Top-of-book snapshot (up to [`MAX_BOOK_DEPTH`] levels per side).
///
/// Each message is a complete snapshot of the top `bid_count`/`ask_count`
/// levels as the gateway saw them at `gateway_ts`.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BookMessage {
    /// Gateway-assigned strictly-monotonic sequence number across the
    /// whole stream.
    pub gateway_seq: u64,
    /// Nanoseconds since UNIX epoch when the gateway received the upstream
    /// packet. `0` if unset.
    pub gateway_ts: i64,
    /// Per-instrument packet sequence number from the gateway.
    pub packet_seq: u32,
    /// Exchange header (timestamp + exchange sequence).
    pub header: ExchangeHeader,
    /// Numeric contract identifier (exchange-assigned).
    pub contract_id: u32,
    /// FIGI identifier — your primary instrument key.
    pub figi: Figi,
    /// Book status flags.
    pub book_flags: BookFlags,
    /// Trading condition flags.
    pub condition_flags: ConditionFlags,
    /// Number of valid bid levels (`<= MAX_BOOK_DEPTH`).
    pub bid_count: u8,
    /// Number of valid ask levels (`<= MAX_BOOK_DEPTH`).
    pub ask_count: u8,
    /// Bid levels, best to worst. Only the first `bid_count` are valid.
    pub bids: [BookLevel; MAX_BOOK_DEPTH],
    /// Ask levels, best to worst. Only the first `ask_count` are valid.
    pub asks: [BookLevel; MAX_BOOK_DEPTH],
}

impl Default for BookMessage {
    fn default() -> Self {
        Self {
            gateway_seq: 0,
            gateway_ts: 0,
            packet_seq: 0,
            header: ExchangeHeader::default(),
            contract_id: 0,
            figi: Figi::default(),
            book_flags: BookFlags::empty(),
            condition_flags: ConditionFlags::empty(),
            bid_count: 0,
            ask_count: 0,
            bids: [BookLevel::default(); MAX_BOOK_DEPTH],
            asks: [BookLevel::default(); MAX_BOOK_DEPTH],
        }
    }
}

impl BookMessage {
    /// Returns the valid bid levels (`&self.bids[..self.bid_count]`).
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids[..self.bid_count.min(MAX_BOOK_DEPTH as u8) as usize]
    }

    /// Returns the valid ask levels (`&self.asks[..self.ask_count]`).
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks[..self.ask_count.min(MAX_BOOK_DEPTH as u8) as usize]
    }

    /// Best bid (level 0), if any.
    pub fn best_bid(&self) -> Option<&BookLevel> {
        self.bids().first()
    }

    /// Best ask (level 0), if any.
    pub fn best_ask(&self) -> Option<&BookLevel> {
        self.asks().first()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn figi_roundtrip() {
        let f: Figi = "BBG00ABCDEF1".parse().unwrap();
        assert_eq!(f.as_str(), "BBG00ABCDEF1");
        assert!(!f.is_empty());
    }

    #[test]
    fn figi_truncates_long_input() {
        let f: Figi = "BBG00ABCDEF1XTRA".parse().unwrap();
        assert_eq!(f.as_str(), "BBG00ABCDEF1");
    }

    #[test]
    fn figi_empty_default() {
        assert!(Figi::default().is_empty());
        assert_eq!(Figi::default().as_str(), "");
    }

    #[test]
    fn flags_contains() {
        let f = ConditionFlags::ENTER;
        assert!(f.contains(ConditionFlags::ENTER));
        assert!(!f.contains(ConditionFlags::TRADING));
    }

    #[test]
    fn book_message_slices() {
        let mut m = BookMessage::default();
        m.bid_count = 2;
        m.ask_count = 3;
        m.bids[0].price = 100.0;
        m.bids[1].price = 99.5;
        m.asks[0].price = 100.5;
        assert_eq!(m.bids().len(), 2);
        assert_eq!(m.asks().len(), 3);
        assert_eq!(m.best_bid().unwrap().price, 100.0);
        assert_eq!(m.best_ask().unwrap().price, 100.5);
    }
}
