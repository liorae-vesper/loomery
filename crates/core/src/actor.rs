// SPDX-License-Identifier: MPL-2.0

//! Actor identity — *who* performed an action.
//!
//! Every command and event is attributable to an [`Actor`]: a user (an
//! [`Id`]), the system itself, or a saga (the workflow runner, e.g.
//! `InvitationSaga`). The [`EventEnvelope`](crate::envelope::Event)
//! carries the actor of every event (§6 of `docs/design.md`).

use crate::id::Id;
use serde::{Deserialize, Serialize};

/// The actor that performed an action.
///
/// Carried on the [`EventEnvelope`](crate::envelope::Event) so every
/// event is attributable. Matching on the variant is deterministic — the
/// pure core treats actors as opaque, comparable data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd)]
pub enum Actor {
    /// A human user, identified by their [`Id`].
    User {
        /// The user's identifier.
        id: Id,
    },
    /// The system itself (e.g. background maintenance, or an OIDC
    /// auto-provisioning path).
    System,
    /// A saga — a long-running workflow that emits events on behalf of a
    /// user. `user_id` is the user the saga acts for (if any), `name` the
    /// saga identity (e.g. `"InvitationSaga"`).
    Saga {
        /// The user this saga acts on behalf of if any.
        user_id: Option<Id>,
        /// The saga's identity string.
        name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_variants_survive_a_serde_round_trip() {
        let actors = [
            Actor::User {
                id: Id::from("user-1"),
            },
            Actor::System,
            Actor::Saga {
                user_id: Some(Id::from("user-1")),
                name: "InvitationSaga".to_owned(),
            },
        ];

        for actor in actors {
            let json = serde_json::to_string(&actor).unwrap();
            let decoded: Actor = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, actor);
        }
    }

    #[test]
    fn actors_are_plain_comparable_data() {
        assert_eq!(
            Actor::User { id: Id::from("u1") },
            Actor::User { id: Id::from("u1") }
        );
        assert_ne!(Actor::User { id: Id::from("u1") }, Actor::System);
        assert_ne!(
            Actor::Saga {
                user_id: Some(Id::from("u1")),
                name: "A".to_owned()
            },
            Actor::Saga {
                user_id: Some(Id::from("u1")),
                name: "B".to_owned()
            }
        );
    }
}
