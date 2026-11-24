//! The execution framework, after Temporal's CHASM.
//!
//! An **execution** is a tree of nodes addressed by path, owned by one
//! archetype (workflow, deployment...). Everything that happens to it is a
//! **transition**: open a [`Tx`], read and write typed nodes, commit. A
//! commit is atomic and writes, together:
//!
//! - the dirty nodes, each stamped with the transition's logical time
//!   ([`Vt`]) and the shard's change sequence;
//! - the execution's **logical tasks** (part of its replicated state), after
//!   dropping every task its archetype no longer considers valid: tasks are
//!   never cancelled, only validated;
//! - the **physical rows** derived from those tasks: one wake-up row for the
//!   earliest timer, one task-queue row per dispatchable task, one outbox
//!   row if a side effect is pending. Physical rows are a local, regenerable
//!   index and are never replicated;
//! - one row in the shard's change index, which is what cell-to-cell
//!   replication reads.
//!
//! Transitions are pure: no I/O, and time comes from the log. Side effects
//! are performed afterwards by the primary, which reads state, acts, and
//! writes the outcome back as a new transition.

use crate::shard::Shard;
use crate::{decode_stored, keys};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use wfe_core::api::{DeploymentVersion, TaskKind};
use wfe_core::codec::encode;
use wfe_core::{CellId, Id128, Timestamp};
use wfe_lsm::StorageError;
use wfe_vsr::Store;

/// Archetypes.
pub const WORKFLOW: u8 = 1;
pub const DEPLOYMENT: u8 = 2;

/// Logical time of a transition: `count` grows by one per committed
/// transition of an execution, `epoch` is the cell's failover epoch. The
/// greater value wins when two cells disagree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Vt {
    pub epoch: u64,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub created: Vt,
    pub updated: Vt,
    /// Shard change sequence of the write (local; rewritten on replication).
    pub seq: u64,
    /// Tombstone: kept so that deletions replicate.
    pub deleted: bool,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExecKey {
    pub arch: u8,
    pub id: String,
    pub run: Id128,
}

/// Where a dispatchable task waits for a worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSlot {
    pub queue: String,
    pub kind: TaskKind,
    /// Only workers of this version may take it.
    pub pinned: Option<DeploymentVersion>,
    /// 0..100, fixed per workflow id: decides which side of a ramp it is on.
    pub ramp: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalTask {
    pub id: u64,
    /// Timer: becomes due at this time.
    pub at: Option<Timestamp>,
    /// Waits in a task queue for a worker.
    pub queue: Option<QueueSlot>,
    /// Needs the primary to perform a side effect.
    pub outbound: bool,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

/// The root node of every execution. Replicated.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    pub vt: Vt,
    pub created_at: Timestamp,
    pub tasks: Vec<LogicalTask>,
    pub next_task_id: u64,
    pub closed: bool,
}

/// Per-execution bookkeeping that never leaves this shard.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Local {
    /// This execution's row in the change index.
    pub change_seq: u64,
    /// Physical rows by tag (`t`, `o`, `q<task id>`).
    pub rows: BTreeMap<String, serde_bytes::ByteBuf>,
    pub vis_key: Option<serde_bytes::ByteBuf>,
    pub counted: Option<DeploymentVersion>,
    pub open: bool,
    /// Per target cell: the change sequence it has acknowledged.
    pub shipped: BTreeMap<CellId, u64>,
    /// Per source cell: how much of this execution has been received.
    pub received: BTreeMap<CellId, crate::Received>,
}

/// A queue row's value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRef {
    pub key: ExecKey,
    pub task_id: u64,
    pub ramp: u8,
}

/// What the archetype says about an execution at commit.
#[derive(Default)]
pub struct Summary {
    /// `(sort key suffix, value)` of the row shown in listings.
    pub visibility: Option<(Vec<u8>, Vec<u8>)>,
    /// Counted as an open workflow.
    pub open: bool,
    /// The version it is pinned to while open (for drainage).
    pub counted: Option<DeploymentVersion>,
}

pub trait Hooks {
    fn validate(&self, tx: &mut Tx, task: &LogicalTask) -> Result<bool, StorageError>;
    fn summary(&self, tx: &mut Tx) -> Result<Summary, StorageError>;
}

struct Slot {
    stored_with_tombstones: Option<Node>,
    value: Option<Vec<u8>>,
    dirty: bool,
}

/// One transition of one execution.
pub struct Tx<'a, 'b> {
    pub store: &'a mut Store<'b>,
    pub shard: &'a mut Shard,
    pub now: Timestamp,
    pub key: ExecKey,
    pub meta: Meta,
    pub local: Local,
    existed: bool,
    meta_before: Option<Meta>,
    nodes: BTreeMap<String, Slot>,
    prefix: Vec<u8>,
}

impl<'a, 'b> Tx<'a, 'b> {
    pub fn open(
        store: &'a mut Store<'b>,
        shard: &'a mut Shard,
        now: Timestamp,
        key: ExecKey,
    ) -> Result<Tx<'a, 'b>, StorageError> {
        let prefix = keys::exec(&key);
        let root: Option<Node> = store.get(&prefix)?.map(|b| decode_stored(&b));
        let local = store.get(&keys::local(&key))?.map(|b| decode_stored(&b)).unwrap_or_default();
        let meta_before: Option<Meta> = root.filter(|n| !n.deleted).map(|n| decode_stored(&n.data));
        let existed = meta_before.is_some();
        let meta = meta_before.clone().unwrap_or(Meta { created_at: now, next_task_id: 1, ..Meta::default() });
        Ok(Tx { store, shard, now, key, meta, local, existed, meta_before, nodes: BTreeMap::new(), prefix })
    }

    pub fn exists(&self) -> bool {
        self.existed
    }

    fn slot(&mut self, path: &str) -> Result<&mut Slot, StorageError> {
        assert!(!path.is_empty(), "the root node belongs to the framework");
        if !self.nodes.contains_key(path) {
            let mut key = self.prefix.clone();
            key.extend_from_slice(path.as_bytes());
            let stored: Option<Node> = self.store.get(&key)?.map(|b| decode_stored(&b));
            let value = stored.as_ref().filter(|n| !n.deleted).map(|n| n.data.clone());
            self.nodes.insert(path.to_string(), Slot { stored_with_tombstones: stored, value, dirty: false });
        }
        Ok(self.nodes.get_mut(path).expect("the slot is inserted above whenever it was not already cached"))
    }

    pub fn get<T: DeserializeOwned>(&mut self, path: &str) -> Result<Option<T>, StorageError> {
        Ok(self.slot(path)?.value.as_ref().map(|b| decode_stored(b)))
    }

    pub fn put<T: Serialize>(&mut self, path: &str, value: &T) -> Result<(), StorageError> {
        let slot = self.slot(path)?;
        slot.value = Some(encode(value));
        slot.dirty = true;
        Ok(())
    }

    pub fn delete(&mut self, path: &str) -> Result<(), StorageError> {
        let slot = self.slot(path)?;
        if slot.value.is_some() {
            slot.value = None;
            slot.dirty = true;
        }
        Ok(())
    }

    /// Live nodes under `prefix` with path greater than `after`, in order.
    /// Sees this transition's own writes.
    pub fn list<T: DeserializeOwned>(
        &mut self,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<(String, T)>, StorageError> {
        let mut start = self.prefix.clone();
        start.extend_from_slice(if after.is_empty() { prefix.as_bytes() } else { after.as_bytes() });
        if !after.is_empty() {
            start.push(0);
        }
        let mut end = self.prefix.clone();
        end.extend_from_slice(prefix.as_bytes());
        let end = wfe_core::keys::prefix_end(&end);
        let mut out: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut cursor = start;
        loop {
            let want = limit.saturating_sub(out.len()).saturating_add(16);
            let rows = self.store.scan(&cursor, &end, want, false)?;
            let exhausted = rows.len() < want;
            for (key, value) in &rows {
                let path = String::from_utf8(key[self.prefix.len()..].to_vec())
                    .expect("node paths are stored from &str, so they are valid UTF-8");
                let node: Node = decode_stored(value);
                if !node.deleted && !self.nodes.contains_key(&path) {
                    out.insert(path, node.data);
                }
            }
            if exhausted || out.len() >= limit {
                break;
            }
            let Some((last_key, _)) = rows.last() else { break };
            cursor = last_key.clone();
            cursor.push(0);
        }
        let last_from_store = if out.len() >= limit { out.keys().nth(limit - 1).cloned() } else { None };
        for (path, slot) in self.nodes.range(prefix.to_string()..) {
            if !path.starts_with(prefix) {
                break;
            }
            if path.as_str() <= after || last_from_store.as_ref().is_some_and(|last| path > last) {
                continue;
            }
            if let Some(value) = &slot.value {
                out.insert(path.clone(), value.clone());
            }
        }
        Ok(out.into_iter().take(limit).map(|(path, data)| (path, decode_stored(&data))).collect())
    }

    pub fn add_task<T: Serialize>(
        &mut self,
        at: Option<Timestamp>,
        queue: Option<QueueSlot>,
        outbound: bool,
        body: &T,
    ) -> u64 {
        let id = self.meta.next_task_id;
        self.meta.next_task_id += 1;
        self.meta.tasks.push(LogicalTask { id, at, queue, outbound, body: encode(body) });
        id
    }

    pub fn remove_task(&mut self, id: u64) {
        self.meta.tasks.retain(|t| t.id != id);
    }

    /// Commit the transition. Returns whether anything changed.
    pub fn commit(mut self, hooks: &dyn Hooks, force: bool) -> Result<bool, StorageError> {
        let dirty = self.nodes.values().any(|s| {
            s.dirty && s.value != s.stored_with_tombstones.as_ref().filter(|n| !n.deleted).map(|n| n.data.clone())
        });
        if !dirty && !force && self.meta_before.as_ref() == Some(&self.meta) {
            return Ok(false);
        }
        self.keep_only_tasks_that_still_validate(hooks)?;
        let summary = hooks.summary(&mut self)?;
        let changed = dirty || self.meta_before.as_ref() != Some(&self.meta);
        if changed {
            self.meta.vt = Vt { epoch: self.shard.state.epoch.max(self.meta.vt.epoch), count: self.meta.vt.count + 1 };
            self.shard.state.change_seq += 1;
        }
        let vt = self.meta.vt;
        let seq = self.shard.state.change_seq;
        if changed {
            for (path, slot) in &self.nodes {
                if !slot.dirty {
                    continue;
                }
                let before = slot.stored_with_tombstones.as_ref().filter(|n| !n.deleted).map(|n| &n.data);
                if before == slot.value.as_ref() {
                    continue;
                }
                let created = match (&slot.stored_with_tombstones, &slot.value) {
                    (Some(n), _) if !n.deleted => n.created,
                    _ => vt,
                };
                let node = Node {
                    created,
                    updated: vt,
                    seq,
                    deleted: slot.value.is_none(),
                    data: slot.value.clone().unwrap_or_default(),
                };
                let mut key = self.prefix.clone();
                key.extend_from_slice(path.as_bytes());
                self.store.put(&key, &encode(&node))?;
            }
            let root = Node { created: Vt::default(), updated: vt, seq, deleted: false, data: encode(&self.meta) };
            self.store.put(&self.prefix, &encode(&root))?;
        }
        self.finish(summary, changed)?;
        Ok(changed)
    }

    fn keep_only_tasks_that_still_validate(&mut self, hooks: &dyn Hooks) -> Result<(), StorageError> {
        let tasks = std::mem::take(&mut self.meta.tasks);
        let mut kept = Vec::with_capacity(tasks.len());
        for task in tasks {
            if hooks.validate(self, &task)? {
                kept.push(task);
            }
        }
        self.meta.tasks = kept;
        Ok(())
    }

    pub(crate) fn finish(&mut self, summary: Summary, changed: bool) -> Result<(), StorageError> {
        let local_before = self.local.clone();
        if !self.existed && self.local.change_seq == 0 {
            self.shard.state.executions += 1;
        }
        if changed {
            if self.local.change_seq != 0 {
                self.store.delete(&keys::change(self.local.change_seq));
            }
            self.local.change_seq = self.shard.state.change_seq;
            self.store.put(&keys::change(self.local.change_seq), &encode(&self.key))?;
        }

        self.sync_task_rows()?;
        self.sync_listing_row(&summary)?;
        self.sync_counters(summary)?;
        if self.local != local_before {
            self.store.put(&keys::local(&self.key), &encode(&self.local))?;
        }
        self.existed = true;
        Ok(())
    }

    fn sync_task_rows(&mut self) -> Result<(), StorageError> {
        let runs_here = self.shard.state.active;
        let mut desired: BTreeMap<String, (Vec<u8>, Vec<u8>)> = BTreeMap::new();
        if runs_here && let Some(at) = self.meta.tasks.iter().filter_map(|t| t.at).min() {
            desired.insert("t".into(), (keys::timer(at, &self.key), Vec::new()));
            self.shard.timer_added(at);
        }
        let side_effects_run_here = self.shard.state.active || self.key.arch == WORKFLOW;
        if side_effects_run_here && self.meta.tasks.iter().any(|t| t.outbound) {
            desired.insert("o".into(), (keys::outbox(&self.key), Vec::new()));
        }
        for task in &self.meta.tasks {
            let Some(slot) = &task.queue else { continue };
            let tag = format!("q{}", task.id);
            let row = match self.local.rows.get(&tag) {
                Some(existing) => existing.to_vec(),
                None => {
                    self.shard.state.queue_seq += 1;
                    keys::queue(&slot.queue, slot.kind, slot.pinned.as_ref(), self.shard.state.queue_seq)
                }
            };
            let value = encode(&TaskRef { key: self.key.clone(), task_id: task.id, ramp: slot.ramp });
            desired.insert(tag, (row, value));
        }
        for (tag, row) in &self.local.rows {
            if desired.get(tag).is_none_or(|(key, _)| key != row.as_ref()) {
                self.store.delete(row);
            }
        }
        let mut rows = BTreeMap::new();
        for (tag, (key, value)) in desired {
            if self.local.rows.get(&tag).is_none_or(|row| row.as_ref() != key) {
                self.store.put(&key, &value)?;
            }
            rows.insert(tag, serde_bytes::ByteBuf::from(key));
        }
        self.local.rows = rows;
        Ok(())
    }

    fn sync_listing_row(&mut self, summary: &Summary) -> Result<(), StorageError> {
        let vis_key = summary.visibility.as_ref().map(|(suffix, _)| keys::visibility(self.key.arch, suffix));
        if let Some(old) = &self.local.vis_key
            && vis_key.as_deref() != Some(old.as_ref())
        {
            self.store.delete(old);
        }
        if let (Some(key), Some((_, value))) = (&vis_key, &summary.visibility) {
            self.store.put(key, value)?;
        }
        self.local.vis_key = vis_key.map(serde_bytes::ByteBuf::from);
        Ok(())
    }

    fn sync_counters(&mut self, summary: Summary) -> Result<(), StorageError> {
        if summary.open != self.local.open {
            if summary.open {
                self.shard.state.open_workflows += 1;
            } else {
                self.shard.state.open_workflows -= 1;
            }
            self.local.open = summary.open;
        }
        if summary.counted != self.local.counted {
            if let Some(old) = self.local.counted.take() {
                let key = keys::pinned_count(&old);
                let n: u64 = self.store.get(&key)?.map(|b| decode_stored(&b)).unwrap_or(0);
                if n <= 1 {
                    self.store.delete(&key);
                } else {
                    self.store.put(&key, &encode(&(n - 1)))?;
                }
            }
            if let Some(new) = &summary.counted {
                let key = keys::pinned_count(new);
                let n: u64 = self.store.get(&key)?.map(|b| decode_stored(&b)).unwrap_or(0);
                self.store.put(&key, &encode(&(n + 1)))?;
            }
            self.local.counted = summary.counted;
        }
        Ok(())
    }

    pub(crate) fn upsert_replicated(&mut self, path: &str, mut node: Node) -> Result<bool, StorageError> {
        let mut key = self.prefix.clone();
        key.extend_from_slice(path.as_bytes());
        let stored: Option<Node> = self.store.get(&key)?.map(|b| decode_stored(&b));
        if stored.as_ref().is_some_and(|mine| mine.updated >= node.updated) {
            return Ok(false);
        }
        node.seq = self.shard.state.change_seq;
        self.store.put(&key, &encode(&node))?;
        if path.is_empty() {
            self.meta = decode_stored(&node.data);
        }
        Ok(true)
    }
}
