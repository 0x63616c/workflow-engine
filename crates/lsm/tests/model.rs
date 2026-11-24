use std::collections::BTreeMap;
use wfe_core::Prng;
use wfe_core::device::{Device, MemDevice};
use wfe_lsm::{
    CheckpointRef, GridConfig, Lsm, LsmOptions, StorageError, missing_blocks, read_block_raw, write_block_raw,
};

const GRID: GridConfig = GridConfig { offset: 8192, block_size: 4096, block_count: 4096 };

fn opts() -> LsmOptions {
    LsmOptions {
        memtable_bytes_max: 6 * 1024,
        l0_tables_max: 2,
        level_growth: 3,
        table_data_blocks_max: 3,
        cache_blocks: 8,
    }
}

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Checkpoint,
}

fn usize_of(n: u64) -> usize {
    usize::try_from(n).expect("test sizes are small and tests run on 64-bit hosts")
}

fn key(prng: &mut Prng, space: u64) -> Vec<u8> {
    format!("key/{:06}", prng.below(space)).into_bytes()
}

fn gen_op(prng: &mut Prng, space: u64) -> Op {
    match prng.below(100) {
        0..=64 => {
            let len = usize_of(if prng.chance(1, 20) { prng.range(1500, 3500) } else { prng.below(200) });
            let mut value = vec![0u8; len];
            prng.fill(&mut value);
            Op::Put(key(prng, space), value)
        }
        65..=96 => Op::Delete(key(prng, space)),
        _ => Op::Checkpoint,
    }
}

fn apply(lsm: &mut Lsm, dev: &mut MemDevice, op: &Op) -> Option<CheckpointRef> {
    match op {
        Op::Put(k, v) => lsm.put(k, v).expect("generated keys and values stay within the LSM size limits"),
        Op::Delete(k) => lsm.delete(k),
        Op::Checkpoint => {}
    }
    lsm.maintain(dev).expect("a MemDevice never faults, so maintenance always succeeds");
    if matches!(op, Op::Checkpoint) {
        let checkpoint = lsm.checkpoint(dev).expect("a MemDevice never faults, so checkpoints always succeed");
        dev.sync();
        lsm.checkpoint_durable();
        return Some(checkpoint);
    }
    None
}

fn check_reads(lsm: &mut Lsm, dev: &mut MemDevice, model: &BTreeMap<Vec<u8>, Vec<u8>>, prng: &mut Prng, space: u64) {
    for _ in 0..3 {
        let k = key(prng, space);
        assert_eq!(
            lsm.get(dev, &k).expect("a MemDevice never faults, so reads succeed"),
            model.get(&k).cloned(),
            "get {:?}",
            String::from_utf8_lossy(&k)
        );
    }
    let (mut a, mut b) = (key(prng, space), key(prng, space));
    if a > b {
        std::mem::swap(&mut a, &mut b);
    }
    let end: &[u8] = if prng.chance(1, 4) { &[] } else { &b };
    let limit = usize_of(prng.range(1, 40));
    let reverse = prng.bool();
    let got = lsm.scan(dev, &a, end, limit, reverse).expect("a MemDevice never faults, so scans succeed");
    let in_range = model.iter().filter(|(k, _)| k.as_slice() >= a.as_slice() && (end.is_empty() || k.as_slice() < end));
    let want: Vec<(Vec<u8>, Vec<u8>)> = if reverse {
        in_range.rev().take(limit).map(|(k, v)| (k.clone(), v.clone())).collect()
    } else {
        in_range.take(limit).map(|(k, v)| (k.clone(), v.clone())).collect()
    };
    assert_eq!(got, want, "scan reverse={reverse} limit={limit}");
}

fn full_scan(lsm: &mut Lsm, dev: &mut MemDevice) -> Vec<(Vec<u8>, Vec<u8>)> {
    lsm.scan(dev, b"", b"", usize::MAX, false).expect("a MemDevice never faults, so scans succeed")
}

#[test]
fn matches_model() {
    for seed in 0..12 {
        let mut prng = Prng::new(seed);
        let space = *prng.pick(&[20u64, 300, 3000]);
        let mut dev = MemDevice::new();
        let mut lsm = Lsm::open(&mut dev, GRID, opts(), None).unwrap();
        let mut model = BTreeMap::new();
        for _ in 0..4000 {
            let op = gen_op(&mut prng, space);
            match &op {
                Op::Put(k, v) => {
                    model.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    model.remove(k);
                }
                Op::Checkpoint => {}
            }
            apply(&mut lsm, &mut dev, &op);
            check_reads(&mut lsm, &mut dev, &model, &mut prng, space);
        }
        let all: Vec<_> = model.into_iter().collect();
        assert_eq!(full_scan(&mut lsm, &mut dev), all, "seed {seed}");
        let stats = lsm.stats();
        assert!(stats.compactions > 0 && stats.flushes > 0, "seed {seed}: {stats:?}");
    }
}

#[test]
fn crash_replay_is_physically_deterministic() {
    for seed in 100..110 {
        let mut prng = Prng::new(seed);
        let space = 500;
        let (mut dev_a, mut dev_b) = (MemDevice::new(), MemDevice::new());
        let mut a = Lsm::open(&mut dev_a, GRID, opts(), None).unwrap();
        let mut b = Lsm::open(&mut dev_b, GRID, opts(), None).unwrap();
        let mut checkpoint: Option<CheckpointRef> = None;
        let mut durable_image: Vec<u8> = Vec::new();
        let mut ops_since_checkpoint: Vec<Op> = Vec::new();
        for _ in 0..3000 {
            let op = gen_op(&mut prng, space);
            let ref_a = apply(&mut a, &mut dev_a, &op);
            let ref_b = apply(&mut b, &mut dev_b, &op);
            assert_eq!(ref_a, ref_b, "seed {seed}: checkpoints diverged");
            ops_since_checkpoint.push(op);
            if ref_b.is_some() {
                checkpoint = ref_b;
                durable_image = dev_b.bytes().to_vec();
                ops_since_checkpoint.clear();
            }
            if prng.chance(1, 150) {
                let crash_loses_unsynced_writes = prng.bool();
                if crash_loses_unsynced_writes {
                    *dev_b.bytes_mut() = durable_image.clone();
                }
                drop(b);
                b = Lsm::open(&mut dev_b, GRID, opts(), checkpoint).unwrap();
                for op in &ops_since_checkpoint {
                    assert_eq!(apply(&mut b, &mut dev_b, op), None);
                }
            }
            assert_eq!(a.fingerprint(), b.fingerprint(), "seed {seed}: trees that applied the same ops differ");
        }
        assert_eq!(apply(&mut a, &mut dev_a, &Op::Checkpoint), apply(&mut b, &mut dev_b, &Op::Checkpoint));
        assert_eq!(full_scan(&mut a, &mut dev_a), full_scan(&mut b, &mut dev_b));
        let checkpoint = a.checkpoint_ref().unwrap();
        assert!(
            missing_blocks(&mut dev_a, &GRID, &checkpoint, usize::MAX).is_empty(),
            "seed {seed}: replica A lacks a block its checkpoint references"
        );
        assert!(
            missing_blocks(&mut dev_b, &GRID, &checkpoint, usize::MAX).is_empty(),
            "seed {seed}: replica B lacks a block its checkpoint references"
        );
        assert_eq!(a.stats().blocks_used, b.stats().blocks_used);
    }
}

#[test]
fn faults_are_detected_and_repaired_from_a_peer() {
    let mut prng = Prng::new(7);
    let (mut dev_a, mut dev_b) = (MemDevice::new(), MemDevice::new());
    let mut a = Lsm::open(&mut dev_a, GRID, opts(), None).unwrap();
    let mut b = Lsm::open(&mut dev_b, GRID, opts(), None).unwrap();
    for _ in 0..2500 {
        let op = gen_op(&mut prng, 800);
        apply(&mut a, &mut dev_a, &op);
        apply(&mut b, &mut dev_b, &op);
    }
    let checkpoint = apply(&mut a, &mut dev_a, &Op::Checkpoint).unwrap();
    assert_eq!(apply(&mut b, &mut dev_b, &Op::Checkpoint), Some(checkpoint));
    let expected = full_scan(&mut a, &mut dev_a);
    let used = b.stats().blocks_used;
    drop(b);

    let mut corrupted_blocks = 0;
    for address in 1..=GRID.block_count {
        if read_block_raw(&mut dev_b, &GRID, address).is_some() && prng.chance(1, 3) {
            let offset = usize_of(GRID.block_offset(address)) + prng.index(usize_of(GRID.block_size.into()));
            dev_b.bytes_mut()[offset] ^= 0x40;
            corrupted_blocks += 1;
        }
    }
    assert!(
        corrupted_blocks > 10 && used > 30,
        "test is too small to mean anything: {corrupted_blocks} corrupted of {used} used blocks"
    );
    let opened = Lsm::open(&mut dev_b, GRID, opts(), Some(checkpoint))
        .and_then(|mut lsm| lsm.scan(&mut dev_b, b"", b"", usize::MAX, false));
    assert!(matches!(opened, Err(StorageError::Fault(_))));

    let mut rounds = 0;
    loop {
        let missing = missing_blocks(&mut dev_b, &GRID, &checkpoint, 16);
        if missing.is_empty() {
            break;
        }
        for block in missing {
            let (found, bytes) = read_block_raw(&mut dev_a, &GRID, block.address).unwrap();
            assert_eq!(found, block);
            assert!(write_block_raw(&mut dev_b, &GRID, block, &bytes));
        }
        rounds += 1;
        assert!(rounds < 10_000);
    }
    let mut b = Lsm::open(&mut dev_b, GRID, opts(), Some(checkpoint)).unwrap();
    assert_eq!(full_scan(&mut b, &mut dev_b), expected);
}

#[test]
fn space_is_reclaimed() {
    let mut prng = Prng::new(3);
    let grid = GridConfig { block_count: 600, ..GRID };
    let mut dev = MemDevice::new();
    let mut lsm = Lsm::open(&mut dev, grid, opts(), None).unwrap();
    let mut peak = 0;
    for i in 0..30_000u32 {
        let op = if i % 97 == 96 {
            Op::Checkpoint
        } else if prng.chance(1, 5) {
            Op::Delete(key(&mut prng, 400))
        } else {
            let mut value = vec![0u8; usize_of(prng.below(300))];
            prng.fill(&mut value);
            Op::Put(key(&mut prng, 400), value)
        };
        apply(&mut lsm, &mut dev, &op);
        peak = peak.max(lsm.stats().blocks_used);
    }
    assert!(
        peak < 300,
        "peak blocks used: {peak}; 400 keys of ~150 bytes is ~20 blocks of live data, so blocks are leaking"
    );
    assert!(dev.len() as u64 <= grid.end_offset());
}

#[test]
fn one_different_value_changes_the_fingerprint() {
    for seed in 200..204 {
        let mut prng = Prng::new(seed);
        let (mut dev_a, mut dev_b) = (MemDevice::new(), MemDevice::new());
        let mut a = Lsm::open(&mut dev_a, GRID, opts(), None).unwrap();
        let mut b = Lsm::open(&mut dev_b, GRID, opts(), None).unwrap();
        for _ in 0..1000 {
            let op = gen_op(&mut prng, 500);
            apply(&mut a, &mut dev_a, &op);
            apply(&mut b, &mut dev_b, &op);
        }
        apply(&mut a, &mut dev_a, &Op::Put(b"key/diverged".to_vec(), b"a".to_vec()));
        apply(&mut b, &mut dev_b, &Op::Put(b"key/diverged".to_vec(), b"b".to_vec()));
        assert_ne!(a.fingerprint(), b.fingerprint(), "seed {seed}: a memtable difference went unseen");
        apply(&mut a, &mut dev_a, &Op::Checkpoint);
        apply(&mut b, &mut dev_b, &Op::Checkpoint);
        assert_ne!(a.fingerprint(), b.fingerprint(), "seed {seed}: a difference in a flushed table went unseen");
    }
}
