//! Tiny deterministic PRNG so we don't pull in the `rand` crate.
//!
//! `xoshiro256**` seeded via SplitMix64. Quality is good enough for
//! generating plausible quantities; nothing here is cryptographic.

pub(crate) struct Rng {
    s: [u64; 4],
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut x = seed;
        let mut next = || {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        let s = [next(), next(), next(), next()];
        // xoshiro256** explicitly forbids the all-zero state. SplitMix64
        // off zero won't produce one, but be defensive.
        let mut me = Self { s };
        if me.s == [0; 4] {
            me.s[0] = 1;
        }
        me
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Returns a u32 in `[0, n)`. Slightly biased but fine for our purposes.
    pub fn next_in_range(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        ((self.next_u64() >> 32) as u32) % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..32 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_differ() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        // Probability of collision in first 4 draws is astronomically small.
        let mut equal = true;
        for _ in 0..4 {
            if a.next_u64() != b.next_u64() {
                equal = false;
                break;
            }
        }
        assert!(!equal);
    }

    #[test]
    fn next_in_range_bounded() {
        let mut r = Rng::new(7);
        for _ in 0..10_000 {
            assert!(r.next_in_range(100) < 100);
        }
        assert_eq!(r.next_in_range(0), 0);
    }
}
