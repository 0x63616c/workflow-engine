#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_macros,
    reason = "test harness: seeds come from env vars so a failure can be replayed, and each seed prints its summary"
)]

use wfe_sim::findings;
use wfe_sim::world::{Options, run};

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[test]
fn seeds() {
    let first = env("WFE_SEED", 1);
    let count = env("WFE_SEEDS", 8);
    let ticks = env("WFE_TICKS", 6_000);
    for seed in first..first + count {
        let opts = Options::for_seed(seed, ticks);
        let report = findings::recorded("world", seed, &opts.describe(), || run(opts.clone()));
        println!(
            "seed {seed}: {} | {} workflows {:?} | {} ops, {} msgs, {} crashes, {} partitions, {} view changes, {} replicated, {} failovers",
            opts.describe(),
            report.workflows,
            report.by_status,
            report.ops,
            report.messages,
            report.crashes,
            report.partitions,
            report.view_changes,
            report.replicated,
            report.failovers
        );
    }
}

const SEEDS_THAT_ONCE_FAILED: [(u64, u64); 14] = [
    (55_656_838_775, 12_000),
    (55_656_839_446, 12_000),
    (944_256_378_930, 12_000),
    (407_509_655_152, 4_000),
    (407_509_655_884, 4_000),
    (406_789_499_107, 12_000),
    (406_789_499_126, 12_000),
    (1, 4_000),
    (235_872_500_034, 12_000),
    (65_102_195_754, 12_000),
    (65_102_196_224, 12_000),
    (65_102_196_777, 12_000),
    (168_242_463_073, 12_000),
    (65_102_202_321, 12_000),
];

#[test]
fn seeds_that_once_failed_still_pass() {
    for (seed, ticks) in SEEDS_THAT_ONCE_FAILED {
        let opts = Options::for_seed(seed, ticks);
        findings::recorded("world", seed, &opts.describe(), || run(opts.clone()));
    }
}

#[test]
fn same_seed_same_run() {
    let opts = Options::for_seed(env("WFE_SEED", 7), 2_000);
    assert_eq!(run(opts.clone()), run(opts));
}
