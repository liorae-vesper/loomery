// SPDX-License-Identifier: MPL-2.0

//! Identity types for the Trellis core.
//!
//! [`Id`] is the canonical identifier for aggregates, organizations,
//! workspaces and every other entity: the 36-character hyphenated UUID
//! string, which is also the serde wire form (a bare `JSON` string, D4 in
//! `docs/design.md`).
//!
//! The UUID *version* is not part of the type's contract, because ids come in
//! two populations (D12) and mixing them up is the one way to duplicate state:
//!
//! - **minted** — [`Id::new`] makes a fresh `UUIDv7`, shell-side, **once per
//!   intent**. The value is carried through retries, so a retry is the same
//!   command rather than a second one.
//! - **derived** — a [`Key`](crate::key::Key) `UUIDv5`, re-derived from the
//!   same inputs by every replica, resumer and client
//!   (`Id::from(key)`).
//!
//! [`Id::new`] must never be called from the pure core: it reads the clock.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use thiserror::Error;
use uuid::{Uuid, Variant};

/// A canonical UUID identifier — `UUIDv7` when minted, `UUIDv5` when derived.
///
/// Canonical form is the hyphenated 36-character lowercase UUID string, which
/// is also the serde wire form (a plain `JSON` string, see D4 in
/// `docs/design.md`).
///
/// **Determinism rule:** the pure core never generates ids. [`Id::new`] is
/// shell-side (or a test helper); the core receives ids injected through the
/// command envelope, or derives them from one
/// ([`Command::event_id`](crate::envelope::Command::event_id)) — never mints
/// its own.
///
/// # Trusted vs untrusted strings
///
/// [`From<&str>`](Id::from) and [`From<String>`](Id::from) adopt a string
/// **without validating it**, for ids already known to be canonical (tests,
/// ids read back from our own log). For an id arriving from outside the
/// process — a client header, a URL path — use [`Id::parse`], which enforces
/// the canonical spelling. (`From` and `TryFrom` cannot coexist for the same
/// types, so the validating path cannot be a `TryFrom`.)
#[derive(Clone, PartialEq, PartialOrd, Debug, Eq, Hash, Ord, Serialize, Deserialize)]
pub struct Id(String);

impl Id {
    /// Creates a new [`Id`] from a fresh `UUIDv7` value.
    ///
    /// Wall-clock based (v7 embeds a millisecond timestamp) and unique per
    /// call. Mint **once per intent** and reuse it across retries: minting per
    /// attempt is what turns a retry into a duplicate (D12).
    #[must_use]
    pub fn new() -> Self {
        let uuid = Uuid::now_v7();
        Id(uuid.to_string())
    }

    /// Validates `value` as a canonical UUID id.
    ///
    /// Use this for every id that enters the core from outside the process.
    /// The UUID version is deliberately unconstrained — minted ids are `v7`,
    /// derived ids are `v5` — but the spelling is not: uppercase, braces,
    /// `urn:uuid:`, and the 32-character simple form are all rejected, so one
    /// id never has two spellings.
    ///
    /// # Errors
    ///
    /// [`InvalidId`] if `value` is not a UUID, is not canonically spelled, or
    /// does not use the `RFC 4122` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use trellis_core::id::Id;
    ///
    /// let id = Id::new(); // minted shell-side
    /// assert_eq!(Id::parse(&id).unwrap(), id);
    ///
    /// // Aliases and malformed input do not parse.
    /// assert!(Id::parse(&id.to_uppercase()).is_err());
    /// assert!(Id::parse("not-an-id").is_err());
    /// ```
    pub fn parse(value: &str) -> Result<Id, InvalidId> {
        parse_canonical(value)?;
        // Proven canonical, so the input text can be adopted as-is.
        Ok(Id(value.to_owned()))
    }
}

/// Why a string was rejected as a canonical [`Id`].
///
/// Deliberately free of the offending input: ids arrive from untrusted
/// clients, and echoing an attacker-controlled string back through an error
/// response is a needless liability.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidId {
    /// The input is not a UUID in any accepted textual form.
    #[error("id is not a UUID")]
    NotAUuid,
    /// The input is a UUID, but not in the canonical lowercase hyphenated
    /// form (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).
    #[error("id is not in canonical lowercase hyphenated form")]
    NotCanonical,
    /// The input is canonical, but its variant bits are not `RFC 4122`.
    #[error("id must use the RFC 4122 variant")]
    WrongVariant,
}

/// Parses `value` and checks the canonical spelling and variant every UUID
/// id must have.
///
/// Shared by [`Id::parse`] and `Key`'s strict `TryFrom` so the "one id, one
/// spelling" rule has exactly one implementation. Crate-private: the public
/// contracts are those two entry points.
pub(crate) fn parse_canonical(value: &str) -> Result<Uuid, InvalidId> {
    let uuid = Uuid::parse_str(value).map_err(|_| InvalidId::NotAUuid)?;

    // Exactly one spelling per id. Uppercase, braces, URNs and the
    // 32-character simple form all parse as the same UUID, but admitting them
    // would let two spellings name one id.
    if uuid.to_string() != value {
        return Err(InvalidId::NotCanonical);
    }

    if uuid.get_variant() != Variant::RFC4122 {
        return Err(InvalidId::WrongVariant);
    }

    Ok(uuid)
}

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Id {
    /// Writes the canonical UUID text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Id {
    /// Adopts `value` **without validating it** — see the type docs for when
    /// [`Id::parse`] is the right entry point instead.
    fn from(value: &str) -> Self {
        Id(value.to_owned())
    }
}

impl From<String> for Id {
    /// Adopts `value` **without validating it** — see the type docs for when
    /// [`Id::parse`] is the right entry point instead.
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
    use crate::key::Key;

    /// A stable namespace for the derived ids below.
    const NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

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
    fn from_string_keeps_the_value() {
        // Shell-side ids often arrive as owned strings; `From<String>`
        // must agree with `From<&str>`.
        let id = Id::from("test-id".to_owned());
        assert_eq!(id, Id::from("test-id"));
        assert_eq!(&*id, "test-id");
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

    #[test]
    fn default_id_is_a_fresh_v7_id() {
        // `Id::default` is `Id::new` — a shell-side/test-helper convenience.
        assert_ne!(Id::default(), Id::default());
        assert_eq!(Id::default().len(), 36);
    }

    #[test]
    fn display_writes_the_canonical_text() {
        let id = Id::new();
        assert_eq!(id.to_string(), &*id);
    }

    // ---------------------------------------------------------------------
    // Strict parsing: every id entering the process from outside is checked.

    #[test]
    fn parse_accepts_minted_and_derived_ids() {
        // Minted (v7) and derived (v5) ids are both legitimate entity ids.
        let minted = Id::new();
        assert_eq!(Id::parse(&minted).unwrap(), minted);

        let derived = Id::from(Key::new(&NS, "org:42"));
        assert_eq!(Id::parse(&derived).unwrap(), derived);
    }

    #[test]
    fn parse_rejects_a_non_uuid() {
        for input in ["", "not-an-id", "0b70f891-312b-5838-88f7-bbbf6acc462"] {
            assert_eq!(Id::parse(input), Err(InvalidId::NotAUuid), "{input}");
        }
    }

    #[test]
    fn parse_rejects_non_canonical_spellings() {
        let canonical = Id::new().to_string();
        let aliases = [
            canonical.to_uppercase(),
            canonical.replace('-', ""),
            format!("{{{canonical}}}"),
            format!("urn:uuid:{canonical}"),
        ];

        for alias in aliases {
            assert_eq!(Id::parse(&alias), Err(InvalidId::NotCanonical), "{alias}");
        }
    }

    #[test]
    fn parse_rejects_other_variants() {
        // Version nibble `7`, but NCS variant bits (`0`..=`7`).
        assert_eq!(
            Id::parse("018f2c3d-4e5f-7071-0293-a4b5c6d7e8f9"),
            Err(InvalidId::WrongVariant)
        );
    }
}
