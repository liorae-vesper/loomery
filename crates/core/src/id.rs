// SPDX-License-Identifier: MPL-2.0

//! Identity type for the Trellis core.
//!
//! [`Id`] is the canonical identifier used across the domain — aggregates,
//! organizations, causation/correlation keys. Values are `UUIDv7` strings, and
//! the string form *is* the wire form (`JSON`) and the debug form.

use serde::{Deserialize, Serialize};
use std::ops::Deref;
use uuid::Uuid;

/// A `UUIDv7`-based identifier.
///
/// Canonical form is the hyphenated 36-character `UUIDv7` string, which is also
/// the serde wire form (a plain `JSON` string, see D4 in `docs/design.md`).
///
/// **Determinism rule:** the pure core never generates ids. [`Id::new`] is a
/// shell-side (or test-helper) convenience; the core receives ids injected
/// through the command envelope.
#[derive(Clone, PartialEq, PartialOrd, Debug, Eq, Hash, Ord, Serialize, Deserialize)]
pub struct Id(String);

impl Id {
    /// Creates a new [`Id`] from a fresh `UUIDv7` value.
    ///
    /// Wall-clock based (v7 embeds a millisecond timestamp) and unique per
    /// call. The pure core must receive ids through
    /// the command envelope so replays stay deterministic.
    #[must_use]
    pub fn new() -> Self {
        let uuid = Uuid::now_v7();
        Id(uuid.to_string())
    }
}

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&str> for Id {
    fn from(value: &str) -> Self {
        Id(value.to_owned())
    }
}

impl From<String> for Id {
    fn from(value: String) -> Self {
        Id(value)
    }
}

impl Deref for Id {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_ids_are_unique() {
        assert_ne!(Id::new(), Id::new());
    }

    #[test]
    fn new_ids_are_uuid_v7_shaped() {
        let id = Id::new();
        // "xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx"
        assert_eq!(id.len(), 36);
        assert_eq!(id.as_bytes()[14], b'7'); // version nibble: v7
        assert!(matches!(id.as_bytes()[19], b'8'..=b'b')); // variant: RFC 4122
    }

    #[test]
    fn derefs_to_a_str() {
        let id = Id::from("test-id");
        let s: &str = &id;
        assert_eq!(s, "test-id");
    }

    #[test]
    fn serde_round_trip_keeps_the_value() {
        let id = Id::from("test-id");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""test-id""#);
        let decoded: Id = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, id);
    }

    #[test]
    fn ids_compare_lexicographically() {
        let a = Id::from("a");
        let b = Id::from("b");
        assert!(a < b);
        let mut set = std::collections::HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&a));
        assert!(!set.contains(&Id::from("c")));
    }
}
