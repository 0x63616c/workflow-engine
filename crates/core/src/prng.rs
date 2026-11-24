//! Deterministic pseudo-random number generator.
//!
//! Everything random in WFE's deterministic core and in the simulator flows
//! from one of these, seeded from a single `u64`. The implementation is
//! in-tree and integer-only on purpose: a seed must replay identically on any
//! platform and with any dependency versions, forever.

/// xoshiro256++ seeded through splitmix64.
#[derive(Clone, Debug)]
pub struct Prng {
    s: [u64; 4],
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Prng {
    pub fn new(seed: u64) -> Prng {
        let mut sm = seed;
        let s = [splitmix64(&mut sm), splitmix64(&mut sm), splitmix64(&mut sm), splitmix64(&mut sm)];
        Prng { s }
    }

    /// Derive an independent generator. Used to give each simulated component
    /// its own stream, so adding a draw in one place does not perturb others.
    pub fn fork(&mut self) -> Prng {
        Prng::new(self.next_u64())
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[0].wrapping_add(self.s[3]).rotate_left(23).wrapping_add(self.s[0]);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    pub fn next_u128(&mut self) -> u128 {
        (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
    }

    /// Uniform in `[0, n)`. `n` must be non-zero.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "Prng::below(0): the range [0, 0) is empty; pass n >= 1");
        // <https://arxiv.org/abs/1805.10941>
        let (mut low, mut high) = halves(u128::from(self.next_u64()) * u128::from(n));
        if low < n {
            let threshold = n.wrapping_neg() % n;
            while low < threshold {
                (low, high) = halves(u128::from(self.next_u64()) * u128::from(n));
            }
        }
        high
    }

    /// Uniform in `[lo, hi]` (inclusive).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "Prng::range({lo}, {hi})");
        if lo == 0 && hi == u64::MAX {
            return self.next_u64();
        }
        lo + self.below(hi - lo + 1)
    }

    pub fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.below(len as u64)).expect("below(len) is less than len, which is a usize")
    }

    pub fn bool(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    /// True with probability `numerator / denominator`.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        assert!(denominator > 0);
        if numerator == 0 {
            return false;
        }
        if numerator >= denominator {
            return true;
        }
        self.below(denominator) < numerator
    }

    /// True with probability `r`.
    pub fn ratio(&mut self, r: Ratio) -> bool {
        self.chance(r.numerator, r.denominator)
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.index(items.len())]
    }

    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.index(i + 1);
            items.swap(i, j);
        }
    }

    /// Approximately exponentially distributed with the given mean, using
    /// integer arithmetic only (no libm, so it is bit-identical everywhere).
    pub fn exponential(&mut self, mean: u64) -> u64 {
        const FRACTION_BITS: u32 = 10;
        const LN_2_IN_THOUSANDTHS: u128 = 693;
        let geometric = u64::from((self.next_u64() | 1).leading_zeros());
        let fraction = self.next_u64() >> (64 - FRACTION_BITS);
        let mean_one_and_a_half = u128::from((geometric << FRACTION_BITS) + fraction);
        let scaled = (mean_one_and_a_half * LN_2_IN_THOUSANDTHS * u128::from(mean)) >> FRACTION_BITS;
        u64::try_from(scaled / 1000).unwrap_or(u64::MAX)
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// A probability expressed as an exact fraction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Ratio {
    pub numerator: u64,
    pub denominator: u64,
}

impl Ratio {
    pub const ZERO: Ratio = Ratio { numerator: 0, denominator: 1 };
    pub const ONE: Ratio = Ratio { numerator: 1, denominator: 1 };

    pub const fn new(numerator: u64, denominator: u64) -> Ratio {
        assert!(denominator > 0);
        Ratio { numerator, denominator }
    }

    pub const fn percent(p: u64) -> Ratio {
        Ratio { numerator: p, denominator: 100 }
    }

    pub fn is_zero(&self) -> bool {
        self.numerator == 0
    }
}

impl std::fmt::Display for Ratio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.numerator, self.denominator)
    }
}

fn halves(product: u128) -> (u64, u64) {
    let bytes = product.to_le_bytes();
    (u64::from_le_bytes(std::array::from_fn(|i| bytes[i])), u64::from_le_bytes(std::array::from_fn(|i| bytes[i + 8])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_replays_the_same_stream_forever() {
        let mut p = Prng::new(42);
        let stream: Vec<u64> = (0..3).map(|_| p.next_u64()).collect();
        assert_eq!(stream, [15_021_278_609_987_233_951, 5_881_210_131_331_364_753, 18_149_643_915_985_481_100]);

        let mut r = Prng::new(1);
        let below: Vec<u64> = [3, 1000, u64::MAX / 3 + 7, 3, 1000, u64::MAX / 3 + 7].map(|n| r.below(n)).to_vec();
        assert_eq!(below, [2, 747, 4_588_423_878_584_245_494, 0, 590, 825_761_009_356_306_781]);

        assert_eq!(Prng::new(9).exponential(1_000_000), 297_773);
    }

    #[test]
    fn below_is_in_range() {
        let mut p = Prng::new(7);
        for n in [1u64, 2, 3, 10, 1000, u64::MAX] {
            for _ in 0..200 {
                assert!(p.below(n) < n);
            }
        }
    }

    #[test]
    fn exponential_mean_is_close() {
        let mut p = Prng::new(1);
        let n = 20_000u64;
        let sum: u64 = (0..n).map(|_| p.exponential(1000)).sum();
        let mean = sum / n;
        assert!((900..1150).contains(&mean), "mean {mean}");
    }
}
