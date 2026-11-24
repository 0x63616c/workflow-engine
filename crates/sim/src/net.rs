//! A simulated network: delays, reorders, drops, duplicates and partitions.

use std::collections::{BTreeMap, BTreeSet};
use wfe_core::{Addr, Prng, Ratio};

#[derive(Clone, Debug)]
pub struct NetOptions {
    /// Delivery delay in ticks, uniform in `[delay_min, delay_max]`.
    pub delay_min: u64,
    pub delay_max: u64,
    pub drop: Ratio,
    pub duplicate: Ratio,
}

impl Default for NetOptions {
    fn default() -> NetOptions {
        NetOptions { delay_min: 1, delay_max: 5, drop: Ratio::percent(1), duplicate: Ratio::percent(1) }
    }
}

pub struct Net<M> {
    pub options: NetOptions,
    prng: Prng,
    queue: BTreeMap<(u64, u64), (Addr, Addr, M)>,
    seq: u64,
    cut_links: BTreeSet<(Addr, Addr)>,
    pub sent: u64,
    pub dropped: u64,
}

impl<M: Clone> Net<M> {
    pub fn new(options: NetOptions, prng: Prng) -> Net<M> {
        Net { options, prng, queue: BTreeMap::new(), seq: 0, cut_links: BTreeSet::new(), sent: 0, dropped: 0 }
    }

    pub fn send(&mut self, now: u64, from: Addr, to: Addr, message: M) {
        self.sent += 1;
        if self.cut_links.contains(&(from, to)) || self.prng.ratio(self.options.drop) {
            self.dropped += 1;
            return;
        }
        let copies = if self.prng.ratio(self.options.duplicate) { 2 } else { 1 };
        for _ in 0..copies {
            let delay = self.prng.range(self.options.delay_min, self.options.delay_max);
            self.seq += 1;
            self.queue.insert((now + delay, self.seq), (from, to, message.clone()));
        }
    }

    /// Everything due at or before `now`, in delivery order.
    pub fn deliver(&mut self, now: u64) -> Vec<(Addr, Addr, M)> {
        let later = self.queue.split_off(&(now + 1, 0));
        let due = std::mem::replace(&mut self.queue, later);
        due.into_values().filter(|(from, to, _)| !self.cut_links.contains(&(*from, *to))).collect()
    }

    pub fn cut(&mut self, from: Addr, to: Addr) {
        self.cut_links.insert((from, to));
    }

    /// Cut both directions between every pair across the two groups.
    pub fn partition(&mut self, a: &[Addr], b: &[Addr]) {
        for x in a {
            for y in b {
                self.cut_links.insert((*x, *y));
                self.cut_links.insert((*y, *x));
            }
        }
    }

    pub fn heal(&mut self) {
        self.cut_links.clear();
    }

    pub fn is_partitioned(&self) -> bool {
        !self.cut_links.is_empty()
    }

    pub fn pending(&self) -> usize {
        self.queue.len()
    }
}
