//! Moving the active role from one cell to another.
//!
//! [`plan_roles`] returns one step at a time. The caller applies it,
//! observes both cells again and asks again, until the answer is
//! [`RoleStep::Done`]. An interrupted failover therefore resumes from
//! whatever state the cells are actually in.

use std::collections::BTreeMap;
use wfe_core::CellId;
use wfe_core::api::CellInfo;

/// Replication from one cell to another, summed over the source's shards.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkView {
    /// Source shards that have the link. A link is created shard by shard,
    /// so an interrupted creation leaves it on only some of them.
    pub shards: u32,
    /// Executions still to ship.
    pub pending: u64,
    pub paused: bool,
    /// Every shard has had its latest change acknowledged.
    pub caught_up: bool,
}

/// What one cell reported about itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CellView {
    pub cell: CellId,
    pub shard_count: u32,
    /// Shards that answered.
    pub shards_up: u32,
    /// Shards that answered and accept writes.
    pub active_shards: u32,
    /// The highest epoch any shard reported.
    pub epoch: u64,
    pub links: BTreeMap<CellId, LinkView>,
}

impl CellView {
    pub fn of(info: &CellInfo) -> CellView {
        let mut links: BTreeMap<CellId, LinkView> = BTreeMap::new();
        for link in info.shards.iter().flat_map(|s| &s.replication) {
            let view = links.entry(link.target.cell).or_insert(LinkView {
                shards: 0,
                pending: 0,
                paused: false,
                caught_up: true,
            });
            view.shards += 1;
            view.pending += link.pending;
            view.paused |= link.paused;
            view.caught_up &= link.acked_seq >= link.head_seq;
        }
        let count = |shards: usize| u32::try_from(shards).unwrap_or(u32::MAX);
        CellView {
            cell: info.cell,
            shard_count: info.shard_count,
            shards_up: count(info.shards.len()),
            active_shards: count(info.shards.iter().filter(|s| s.active).count()),
            epoch: info.shards.iter().map(|s| s.epoch).max().unwrap_or(0),
            links,
        }
    }

    pub fn every_shard_answered(&self) -> bool {
        self.shard_count > 0 && self.shards_up == self.shard_count
    }

    pub fn is_active(&self) -> bool {
        self.every_shard_answered() && self.active_shards == self.shard_count
    }

    pub fn is_standby(&self) -> bool {
        self.every_shard_answered() && self.active_shards == 0
    }

    fn replicates_every_shard_to(&self, target: CellId) -> bool {
        self.links.get(&target).is_some_and(|link| link.shards >= self.shard_count)
    }

    fn has_shipped_everything_to(&self, target: CellId) -> Result<(), String> {
        match self.links.get(&target) {
            None => Err(format!("cell {} does not replicate to cell {target}", self.cell)),
            Some(link) if link.shards < self.shard_count => Err(format!(
                "only {} of cell {}'s {} shards replicate to cell {target}",
                link.shards, self.cell, self.shard_count
            )),
            Some(link) if link.pending == 0 && link.caught_up => Ok(()),
            Some(link) if link.paused => Err(format!(
                "replication from cell {} to cell {target} is paused with {} executions to ship; resume it",
                self.cell, link.pending
            )),
            Some(link) => {
                Err(format!("cell {} still has {} executions to ship to cell {target}", self.cell, link.pending))
            }
        }
    }
}

/// The cell the role moves away from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Other<'a> {
    /// There is no other cell.
    Absent,
    /// It did not answer, so it may still be active.
    Unreachable,
    Seen(&'a CellView),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoleStep {
    /// The wanted cell is active and the other one follows it.
    Done,
    /// Nothing to do yet; time or a recovering cell fixes it.
    Wait(String),
    /// Nothing will change until a person acts.
    Blocked(String),
    Demote {
        cell: CellId,
        epoch: u64,
    },
    Promote {
        cell: CellId,
        epoch: u64,
    },
    CreateReplication {
        on: CellId,
        target: CellId,
    },
    DeleteReplication {
        on: CellId,
        target: CellId,
    },
}

/// The next step toward `want` being the active cell and `other` a standby
/// that follows it. `force` promotes without waiting for `other` to hand
/// over what it has not shipped, and without `other` at all.
pub fn plan_roles(want: &CellView, other: Other<'_>, force: bool) -> RoleStep {
    if !want.every_shard_answered() {
        return RoleStep::Wait(format!(
            "only {} of cell {}'s {} shards answered",
            want.shards_up, want.cell, want.shard_count
        ));
    }
    let other = match other {
        Other::Seen(view) if view.active_shards > 0 => {
            return RoleStep::Demote { cell: view.cell, epoch: want.epoch.max(view.epoch) + 1 };
        }
        Other::Seen(view) if view.every_shard_answered() => view,
        Other::Absent if want.is_active() => return RoleStep::Done,
        Other::Absent => return RoleStep::Promote { cell: want.cell, epoch: want.epoch + 1 },
        Other::Seen(_) | Other::Unreachable if want.is_active() => {
            return RoleStep::Wait("the other cell is unreachable; it will follow once it answers".into());
        }
        Other::Seen(_) | Other::Unreachable if force => {
            return RoleStep::Promote { cell: want.cell, epoch: want.epoch + 1 };
        }
        Other::Seen(_) | Other::Unreachable => {
            return RoleStep::Blocked(format!(
                "cell {} is not promoted: the other cell cannot be reached and may be active; force to promote anyway",
                want.cell
            ));
        }
    };
    let next_epoch = want.epoch.max(other.epoch) + 1;
    if want.is_active() {
        return if !want.replicates_every_shard_to(other.cell) {
            RoleStep::CreateReplication { on: want.cell, target: other.cell }
        } else if other.links.contains_key(&want.cell) {
            RoleStep::DeleteReplication { on: other.cell, target: want.cell }
        } else {
            RoleStep::Done
        };
    }
    let a_promotion_was_interrupted = want.active_shards > 0;
    if a_promotion_was_interrupted {
        return RoleStep::Promote { cell: want.cell, epoch: next_epoch };
    }
    if !other.replicates_every_shard_to(want.cell) {
        return RoleStep::CreateReplication { on: other.cell, target: want.cell };
    }
    match other.has_shipped_everything_to(want.cell) {
        Ok(()) => RoleStep::Promote { cell: want.cell, epoch: next_epoch },
        Err(_) if force => RoleStep::Promote { cell: want.cell, epoch: next_epoch },
        Err(why) => RoleStep::Wait(why),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(id: CellId, active_shards: u32, epoch: u64, links: &[(CellId, u64)]) -> CellView {
        CellView {
            cell: id,
            shard_count: 2,
            shards_up: 2,
            active_shards,
            epoch,
            links: links
                .iter()
                .map(|(target, pending)| {
                    (*target, LinkView { shards: 2, pending: *pending, paused: false, caught_up: true })
                })
                .collect(),
        }
    }

    #[test]
    fn an_orderly_failover_demotes_drains_promotes_then_reverses_replication() {
        let west = cell(2, 0, 2, &[]);
        let step = plan_roles(&west, Other::Seen(&cell(1, 2, 1, &[(2, 5)])), false);
        assert_eq!(step, RoleStep::Demote { cell: 1, epoch: 3 });
        let step = plan_roles(&west, Other::Seen(&cell(1, 0, 3, &[(2, 5)])), false);
        assert!(matches!(step, RoleStep::Wait(_)), "{step:?}");
        let step = plan_roles(&west, Other::Seen(&cell(1, 0, 3, &[(2, 0)])), false);
        assert_eq!(step, RoleStep::Promote { cell: 2, epoch: 4 });
        let step = plan_roles(&cell(2, 2, 4, &[]), Other::Seen(&cell(1, 0, 3, &[(2, 0)])), false);
        assert_eq!(step, RoleStep::CreateReplication { on: 2, target: 1 });
        let step = plan_roles(&cell(2, 2, 4, &[(1, 0)]), Other::Seen(&cell(1, 0, 3, &[(2, 0)])), false);
        assert_eq!(step, RoleStep::DeleteReplication { on: 1, target: 2 });
        let step = plan_roles(&cell(2, 2, 4, &[(1, 0)]), Other::Seen(&cell(1, 0, 3, &[])), false);
        assert_eq!(step, RoleStep::Done);
    }

    #[test]
    fn a_half_demoted_or_half_promoted_cell_is_finished_first() {
        let west = cell(2, 0, 2, &[]);
        let step = plan_roles(&west, Other::Seen(&cell(1, 1, 3, &[(2, 0)])), false);
        assert_eq!(step, RoleStep::Demote { cell: 1, epoch: 4 });
        let step = plan_roles(&cell(2, 1, 4, &[]), Other::Seen(&cell(1, 0, 3, &[(2, 9)])), false);
        assert_eq!(step, RoleStep::Promote { cell: 2, epoch: 5 });
    }

    #[test]
    fn an_unreachable_cell_blocks_promotion_unless_forced() {
        let west = cell(2, 0, 2, &[]);
        assert!(matches!(plan_roles(&west, Other::Unreachable, false), RoleStep::Blocked(_)));
        assert_eq!(plan_roles(&west, Other::Unreachable, true), RoleStep::Promote { cell: 2, epoch: 3 });
        let step = plan_roles(&west, Other::Seen(&cell(1, 0, 3, &[(2, 5)])), true);
        assert_eq!(step, RoleStep::Promote { cell: 2, epoch: 4 });
    }

    #[test]
    fn a_link_that_only_some_shards_have_is_finished_before_the_role_moves() {
        let mut east = cell(1, 0, 3, &[(2, 0)]);
        east.links.get_mut(&2).expect("the fixture links east to west").shards = 1;
        let step = plan_roles(&cell(2, 0, 2, &[]), Other::Seen(&east), false);
        assert_eq!(step, RoleStep::CreateReplication { on: 1, target: 2 });
    }

    #[test]
    fn the_only_cell_is_promoted() {
        assert_eq!(plan_roles(&cell(1, 0, 1, &[]), Other::Absent, false), RoleStep::Promote { cell: 1, epoch: 2 });
        assert_eq!(plan_roles(&cell(1, 2, 2, &[]), Other::Absent, false), RoleStep::Done);
    }

    #[test]
    fn a_standby_with_no_link_gets_one_before_the_role_moves() {
        let step = plan_roles(&cell(2, 0, 2, &[]), Other::Seen(&cell(1, 0, 3, &[])), false);
        assert_eq!(step, RoleStep::CreateReplication { on: 1, target: 2 });
    }
}
