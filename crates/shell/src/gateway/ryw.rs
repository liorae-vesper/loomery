// SPDX-License-Identifier: MPL-2.0

//! Read-your-writes: serve a read only once the local replica has caught up.
//!
//! A client that wrote at log index *N* sends `X-Min-Index: N` on its next read
//! (`design.md` §2.3). A replica that has already applied *N* serves from local
//! applied state; one that has not holds (bounded by a configured timeout) and
//! then serves. If it still cannot catch up, the gateway **forwards to the
//! leader** rather than answering with stale data.
//!
//! [`ensure_min_index`] is the whole gate; the HTTP layer only parses the
//! header and maps [`RywOutcome`] onto a status code.

use std::time::Duration;

use crate::raft::RaftGroup;

/// What the read-your-writes gate decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RywOutcome {
    /// The local replica has applied `min_index`; serve the read locally.
    Recent,
    /// The local replica cannot catch up in time; forward to this leader.
    ForwardToLeader {
        /// The leader's node id.
        leader: u64,
    },
    /// No reachable leader is known — answer `503` and let the client retry.
    Unavailable,
}

/// Waits up to `hold` for the local replica to apply `min_index`.
///
/// Returns [`RywOutcome::Recent`] when the wait succeeds. On timeout, the
/// replica's own leadership decides whether the request can be forwarded.
pub async fn ensure_min_index(group: &RaftGroup, min_index: u64, hold: Duration) -> RywOutcome {
    let caught_up = group
        .raft()
        .wait(Some(hold))
        .applied_index_at_least(Some(min_index), "read-your-writes")
        .await
        .is_ok();

    if caught_up {
        return RywOutcome::Recent;
    }

    let receiver = group.raft().metrics();
    let metrics = receiver.borrow();
    classify(
        metrics.last_applied.map_or(0, |log_id| log_id.index),
        min_index,
        metrics.current_leader,
        metrics.id,
    )
}

/// Classifies a read that could not catch up.
///
/// Pure, so the forwarding policy is unit-tested without a cluster:
///
/// * already applied → serve locally (a race just resolved);
/// * a known leader that is someone else → forward there;
/// * this replica is the leader, or no leader is known → unavailable.
fn classify(applied: u64, min_index: u64, leader: Option<u64>, local: u64) -> RywOutcome {
    if applied >= min_index {
        return RywOutcome::Recent;
    }

    match leader {
        Some(leader) if leader != local => RywOutcome::ForwardToLeader { leader },
        _ => RywOutcome::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::GroupOps;
    use crate::test_support::bootstrap_value;
    use loomery_genesis::Step;

    #[test]
    fn a_caught_up_replica_serves_locally() {
        assert_eq!(classify(10, 10, Some(1), 2), RywOutcome::Recent);
        assert_eq!(classify(11, 10, None, 2), RywOutcome::Recent);
    }

    #[test]
    fn a_lagging_follower_forwards_to_a_known_leader() {
        assert_eq!(
            classify(5, 10, Some(1), 2),
            RywOutcome::ForwardToLeader { leader: 1 }
        );
    }

    #[test]
    fn a_lagging_leader_or_leaderless_replica_is_unavailable() {
        assert_eq!(classify(5, 10, Some(2), 2), RywOutcome::Unavailable);
        assert_eq!(classify(5, 10, None, 2), RywOutcome::Unavailable);
    }

    #[tokio::test]
    async fn the_gate_waits_for_the_local_replica_and_then_serves() {
        let mut group = RaftGroup::boot_single_node(1).await.unwrap();
        let bootstrap = bootstrap_value();
        group
            .propose(bootstrap.command(Step::AssignLeader).unwrap())
            .await
            .unwrap();

        let applied = group
            .raft()
            .metrics()
            .borrow()
            .last_applied
            .map_or(0, |log_id| log_id.index);
        assert!(applied > 0);

        assert_eq!(
            ensure_min_index(&group, applied, Duration::from_millis(10)).await,
            RywOutcome::Recent
        );

        // A single-node leader cannot be forwarded away, so a min-index beyond
        // what can ever be applied is unavailable rather than stale.
        assert_eq!(
            ensure_min_index(&group, applied + 1_000, Duration::from_millis(30)).await,
            RywOutcome::Unavailable
        );
    }
}
