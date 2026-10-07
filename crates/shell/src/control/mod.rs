// SPDX-License-Identifier: MPL-2.0

//! The control plane: the control group's read models and tenant lifecycle.
//!
//! The control group holds the tenant-placement records
//! ([`loomery_core::tenant`]).

mod controller;
mod router;

#[cfg(test)]
mod e2e_tests;

pub use controller::bootstrap_for;
pub use controller::incomplete;
pub use controller::provision;
pub use controller::resume;
pub use router::Route;
pub use router::Router;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::config::GroupConfig;
    use crate::group::GroupOps;
    use crate::raft::RaftGroup;
    use loomery_core::actor::Actor;
    use loomery_core::envelope::Command;
    use loomery_core::envelope::Payload;
    use loomery_core::id::Id;
    use loomery_core::key::Key;
    use loomery_core::tenant;
    use loomery_core::timestamp::Timestamp;
    use openraft::BasicNode;

    fn command(command_type: &str, payload: &str) -> Command {
        Command {
            envelope_version: 1,
            id: Id::from("cmd-control"),
            aggregate_id: Id::from("org-1"),
            organization_id: Id::from("org-1"),
            workspace_id: None,
            occurred_at: Timestamp::from(1_700_000_000_000),
            causation_key: Key::new(
                &loomery_core::Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9),
                payload,
            ),
            correlation_key: Key::new(
                &loomery_core::Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9),
                "control-plane",
            ),
            actor: Actor::System,
            command_type: command_type.to_owned(),
            payload: Payload {
                version: 1,
                data: payload.to_owned(),
            },
        }
    }

    /// A control group writes a tenant placement, then restarts over the same
    /// database and finds the record replayed.
    #[tokio::test]
    async fn a_control_group_preserves_tenant_records_across_a_restart() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_path_buf();
        let register = command(
            tenant::REGISTER,
            r#"{"group_id":"tenant-1","replicas":[{"node_id":1,"address":"http://127.0.0.1:7001"}],"leader_user_id":"018f2c3d-4e5f-7071-8293-a4b5c6d7e8f0"}"#,
        );

        {
            let mut group =
                RaftGroup::boot_persistent(1, "control".to_owned(), &path, GroupConfig::default())
                    .await
                    .unwrap();

            // A fresh control group is initialized once, on its bootstrap node.
            group
                .raft()
                .initialize(BTreeMap::from([(1u64, BasicNode::default())]))
                .await
                .unwrap();
            group
                .raft()
                .wait(Some(Duration::from_secs(5)))
                .metrics(
                    |metrics| metrics.current_leader.is_some(),
                    "leader available",
                )
                .await
                .unwrap();

            group.propose(register).await.unwrap();
            let tenants = group.state_machine().tenants().await;
            assert_eq!(tenants.len(), 1);
            assert_eq!(tenants[0].1.group_id.as_deref(), Some("tenant-1"));

            group.shutdown().await.unwrap();
        }

        // Restart over the same database: recovery replays the control log.
        let group =
            RaftGroup::boot_persistent(1, "control".to_owned(), &path, GroupConfig::default())
                .await
                .unwrap();

        let tenants = group.state_machine().tenants().await;
        assert_eq!(tenants.len(), 1);
        assert_eq!(tenants[0].1.group_id.as_deref(), Some("tenant-1"));

        group.shutdown().await.unwrap();
    }
}
