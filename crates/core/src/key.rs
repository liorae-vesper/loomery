// SPDX-License-Identifier: MPL-2.0

//! Causation and idempotency key types for the Trellis core.

use serde::{Deserialize, Serialize};

use thiserror::Error;
use uuid::{Uuid, Variant, Version};

/// A deterministic, namespace-scoped identifier derived by `UUIDv5` hashing.
///
/// Unlike [`crate::id::Id`], a `Key` is *derived*, not generated: [`Key::new`] hashes a
/// namespace together with data (RFC 4122 §4.3) and wraps the resulting
/// `UUIDv5` in its canonical hyphenated string form — the same form [`crate::id::Id`]
/// uses, so the two are indistinguishable on the wire.
///
/// **Determinism rule:** the same `(namespace, data)` pair always derives the
/// same key — on every replica, in every replay, across processes and across
/// versions of this crate. There is no clock read, no randomness and no I/O.
/// That is what makes a `Key` suitable for causation/dedup identities that
/// must be minted without coordination (the genesis bootstrap worker, for
/// example) and re-derived identically after a crash, instead of being
/// persisted.
///
/// Choose a namespace that is itself stable and well-known — a domain
/// constant, not a freshly generated [`Uuid`]. A changing namespace changes
/// every key derived under it, and the determinism guarantee is lost.
///
/// # Constructing a `Key`
///
/// - [`Key::new`] *derives* a key from a namespace plus data.
/// - [`Key::try_from`] *validates* a key that already exists as a string. Use
///   it for keys arriving from outside the core (a client-supplied
///   idempotency header, a fixture, a log line): it accepts the canonical
///   lowercase hyphenated `UUIDv5` form and nothing else, so one key never
///   has two spellings and a `UUIDv7` [`Id`] cannot be smuggled in where a
///   `Key` is expected.
///
/// The fallible direction is `TryFrom`, so there is no `From<&str>` or
/// `From<String>` for `Key`; [`String::from`] converts a `Key` back to its
/// canonical text.
///
/// # Examples
///
/// ```
/// use trellis_core::id::Key;
/// use uuid::Uuid;
///
/// let namespace = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);
///
/// // Derivation is pure: equal inputs give equal keys.
/// assert_eq!(
///     Key::new(&namespace, "genesis:create-default-workspace"),
///     Key::new(&namespace, "genesis:create-default-workspace"),
/// );
///
/// // Data — and the namespace — separate the key space.
/// assert_ne!(
///     Key::new(&namespace, "genesis:create-default-workspace"),
///     Key::new(&namespace, "genesis:assign-owner"),
/// );
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Eq, Hash, Ord, Serialize, Deserialize)]
pub struct Key(String);

impl Key {
    /// Derives a [`Key`] from `namespace` and `data` with `UUIDv5`.
    ///
    /// Pure and deterministic: no clock, no randomness, no I/O. Because the
    /// same `(namespace, data)` pair always derives the same key, callers may
    /// re-derive an identity after a restart rather than persisting it.
    ///
    /// `data` is hashed as its UTF-8 bytes, so the encoding is the caller's
    /// contract: use a stable, unambiguous one (for example
    /// `"genesis:create-default-workspace"` or `format!("workspace:{id}")`).
    /// Case and whitespace changes produce a different key.
    ///
    /// This *mints* a key. To admit one that already exists as a string, use
    /// [`Key::try_from`], which validates it strictly.
    #[must_use]
    pub fn new(namespace: &Uuid, data: &str) -> Key {
        let uuid = Uuid::new_v5(namespace, data.as_bytes());
        Key(uuid.to_string())
    }

    /// Validates `value` as a canonical `UUIDv5` and returns the parsed UUID.
    ///
    /// Private: the public contract is [`TryFrom`].
    fn parse_canonical(value: &str) -> Result<Uuid, InvalidKey> {
        let uuid = Uuid::parse_str(value).map_err(|_| InvalidKey::NotAUuid)?;

        // Exactly one spelling per key. Uppercase, braces, URNs and the
        // 32-character simple form all parse as the same UUID, but admitting
        // them would let two spellings name one key.
        if uuid.to_string() != value {
            return Err(InvalidKey::NotCanonical);
        }

        if uuid.get_version() != Some(Version::Sha1) {
            return Err(InvalidKey::WrongVersion(uuid.get_version_num()));
        }

        if uuid.get_variant() != Variant::RFC4122 {
            return Err(InvalidKey::WrongVariant);
        }

        Ok(uuid)
    }
}

/// Why a string was rejected as a [`Key`].
///
/// Deliberately free of the offending input: keys arrive from untrusted
/// clients, and echoing an attacker-controlled string back through an error
/// response is a needless liability.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidKey {
    /// The input is not a UUID in any accepted textual form.
    #[error("key is not a UUID")]
    NotAUuid,
    /// The input is a UUID, but not in the canonical lowercase hyphenated
    /// form (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).
    #[error("key is not in canonical lowercase hyphenated form")]
    NotCanonical,
    /// The input is canonical, but not a version 5 (`SHA-1` derived) UUID.
    #[error("key must be a UUIDv5, got version {0}")]
    WrongVersion(usize),
    /// The input is canonical, but its variant bits are not `RFC 4122`.
    #[error("key must use the RFC 4122 variant")]
    WrongVariant,
}

impl TryFrom<&str> for Key {
    type Error = InvalidKey;

    /// Validates a borrowed string as a canonical `UUIDv5` key.
    ///
    /// # Errors
    ///
    /// [`InvalidKey`] if `value` is not a UUID, is not in the canonical
    /// lowercase hyphenated form, is not version 5, or does not use the
    /// `RFC 4122` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use trellis_core::id::Key;
    /// use uuid::Uuid;
    ///
    /// let namespace = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);
    /// let key = Key::new(&namespace, "genesis:assign-owner");
    /// let canonical = String::from(&key);
    ///
    /// // The canonical text round-trips...
    /// assert!(Key::try_from(canonical.as_str()).is_ok());
    /// // ...an alias spelling is rejected, so a key has one spelling only...
    /// assert!(Key::try_from(canonical.to_uppercase().as_str()).is_err());
    /// // ...and so is a `UUIDv7` id smuggled in as a key.
    /// assert!(Key::try_from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9").is_err());
    /// ```
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let uuid = Key::parse_canonical(value)?;
        Ok(Key(uuid.to_string()))
    }
}

impl TryFrom<String> for Key {
    type Error = InvalidKey;

    /// Validates an owned string as a canonical `UUIDv5` key, adopting its
    /// allocation instead of re-formatting the UUID.
    ///
    /// # Errors
    ///
    /// As [`TryFrom<&str>`](Key::try_from): [`InvalidKey`] for anything that
    /// is not a canonical `UUIDv5`.
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Key::parse_canonical(&value)?;
        // `value` is proven canonical, so it can be adopted as-is.
        Ok(Key(value))
    }
}

impl From<Key> for String {
    /// Returns the canonical `UUIDv5` text.
    fn from(key: Key) -> Self {
        key.0
    }
}

impl From<&Key> for String {
    /// Returns a copy of the canonical `UUIDv5` text.
    fn from(key: &Key) -> Self {
        key.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------
    // Key: deterministic, namespace-scoped UUIDv5 derivation.

    /// A fixed test namespace — any UUID will do, as long as it never changes.
    const NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    #[test]
    fn key_derivation_is_deterministic() {
        assert_eq!(Key::new(&NS, "org:42"), Key::new(&NS, "org:42"));
    }

    #[test]
    fn key_separates_data_and_namespaces() {
        assert_ne!(Key::new(&NS, "org:42"), Key::new(&NS, "org:43"));
        assert_ne!(
            Key::new(&NS, "org:42"),
            Key::new(&Uuid::from_u128(0), "org:42")
        );
    }

    #[test]
    fn key_matches_the_frozen_uuid_v5_derivation() {
        // A key that was logged, persisted, or used as a dedup identity must
        // stay derivable forever. This pins RFC 4122 §4.3 (SHA-1 over the
        // namespace bytes and the name) against a silent change of hash,
        // encoding, or separators.
        let key = Key::new(&NS, "org:42");
        assert_eq!(key.0, "0b70f891-312b-5838-88f7-bbbf6acc4629");
        assert_eq!(key.0, Uuid::new_v5(&NS, b"org:42").to_string());
    }

    #[test]
    fn key_is_uuid_v5_shaped_and_canonical() {
        let key = Key::new(&NS, "org:42");
        assert_eq!(key.0.len(), 36);
        assert_eq!(key.0.as_bytes()[14], b'5'); // version nibble: v5
        assert!(matches!(key.0.as_bytes()[19], b'8'..=b'b')); // variant: RFC 4122
        assert!(key.0.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        assert_eq!(key.0, key.0.to_lowercase());
        assert_eq!(
            Uuid::parse_str(&key.0).unwrap(),
            Uuid::new_v5(&NS, b"org:42")
        );
    }

    #[test]
    fn key_serde_round_trip_keeps_the_value() {
        let key = Key::new(&NS, "org:42");
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, format!(r#""{}""#, key.0));
        let decoded: Key = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn keys_compare_and_hash_by_value() {
        let a = Key::new(&NS, "a");
        let b = Key::new(&NS, "b");
        // Ord follows the canonical string form, not the insertion order.
        assert_eq!(a.cmp(&b), a.0.cmp(&b.0));
        let mut set = std::collections::HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&Key::new(&NS, "a")));
        assert!(!set.contains(&b));
    }

    // ---------------------------------------------------------------------
    // Key validation: strings re-entering the core are strictly checked.

    #[test]
    fn key_try_from_accepts_a_canonical_key() {
        let key = Key::new(&NS, "org:42");

        // The borrowed and the owned path both accept it, and both round-trip
        // through the canonical text produced by `From<Key> for String`.
        assert_eq!(Key::try_from(String::from(&key).as_str()).unwrap(), key);
        assert_eq!(Key::try_from(String::from(key.clone())).unwrap(), key);
    }

    #[test]
    fn key_try_from_rejects_a_non_uuid() {
        for input in ["", "not-a-key", "0b70f891-312b-5838-88f7-bbbf6acc462"] {
            assert_eq!(Key::try_from(input), Err(InvalidKey::NotAUuid), "{input}");
        }
    }

    #[test]
    fn key_try_from_rejects_non_canonical_spellings() {
        // Every alias below names the same UUID, but only the canonical
        // lowercase hyphenated form is allowed to name a key.
        let canonical = String::from(Key::new(&NS, "org:42"));
        let aliases = [
            canonical.to_uppercase(),
            canonical.replace('-', ""),
            format!("{{{canonical}}}"),
            format!("urn:uuid:{canonical}"),
        ];

        for alias in aliases {
            assert_eq!(
                Key::try_from(alias.as_str()),
                Err(InvalidKey::NotCanonical),
                "{alias}"
            );
        }
    }

    #[test]
    fn key_try_from_rejects_other_uuid_versions() {
        // Canonical and RFC 4122, but a v7 `Id` — not a v5 `Key`.
        assert_eq!(
            Key::try_from("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9"),
            Err(InvalidKey::WrongVersion(7))
        );
        // The nil UUID reports version 0.
        assert_eq!(
            Key::try_from("00000000-0000-0000-0000-000000000000"),
            Err(InvalidKey::WrongVersion(0))
        );
    }

    #[test]
    fn key_try_from_rejects_other_variants() {
        // Version nibble `5`, but NCS variant bits (`0`..=`7`) — the only
        // difference from the frozen key in the golden test above.
        assert_eq!(
            Key::try_from("0b70f891-312b-5838-0871-bbbf6acc4629"),
            Err(InvalidKey::WrongVariant)
        );
    }

    #[test]
    fn key_converts_back_to_its_canonical_string() {
        let key = Key::new(&NS, "org:42");
        let expected = Uuid::new_v5(&NS, b"org:42").to_string();

        assert_eq!(String::from(&key), expected);
        assert_eq!(String::from(key), expected);
    }

    // ---------------------------------------------------------------------
    // Property tests: derivation is a pure, injective function of its inputs.

    use proptest::prelude::*;

    proptest! {
        // Same inputs, same key — no clock, no randomness, no hidden state.
        #[test]
        fn key_is_deterministic_for_any_inputs(namespace in any::<[u8; 16]>(), data in ".*") {
            let ns = Uuid::from_bytes(namespace);
            prop_assert_eq!(Key::new(&ns, &data), Key::new(&ns, &data));
        }

        // Within a namespace, different data never collapses onto one key.
        #[test]
        fn key_separates_data_within_a_namespace(
            namespace in any::<[u8; 16]>(),
            left in ".{1,32}",
            right in ".{1,32}",
        ) {
            prop_assume!(left != right);
            let ns = Uuid::from_bytes(namespace);
            prop_assert_ne!(Key::new(&ns, &left), Key::new(&ns, &right));
        }

        // The namespace partitions the key space: same data under a
        // different namespace gives an unrelated key.
        #[test]
        fn key_separates_namespaces(
            left in any::<[u8; 16]>(),
            right in any::<[u8; 16]>(),
            data in ".*",
        ) {
            prop_assume!(left != right);
            prop_assert_ne!(
                Key::new(&Uuid::from_bytes(left), &data),
                Key::new(&Uuid::from_bytes(right), &data),
            );
        }

        // Whatever the inputs, the result is a canonical UUIDv5 string that
        // parses back to exactly the UUID the algorithm prescribes.
        #[test]
        fn key_is_always_a_canonical_uuid_v5(namespace in any::<[u8; 16]>(), data in ".*") {
            let ns = Uuid::from_bytes(namespace);
            let key = Key::new(&ns, &data);
            prop_assert_eq!(key.0.len(), 36);
            prop_assert_eq!(
                Uuid::parse_str(&key.0).unwrap(),
                Uuid::new_v5(&ns, data.as_bytes()),
            );
        }

        // The wire form round-trips for arbitrary keys.
        #[test]
        fn key_serde_round_trips_for_any_inputs(namespace in any::<[u8; 16]>(), data in ".*") {
            let key = Key::new(&Uuid::from_bytes(namespace), &data);
            let json = serde_json::to_string(&key).unwrap();
            let decoded: Key = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(decoded, key);
        }

        // Every derived key is admitted by `TryFrom` from its canonical text,
        // and an alias spelling never is.
        #[test]
        fn key_try_from_round_trips_and_rejects_aliases(
            namespace in any::<[u8; 16]>(),
            data in ".*",
        ) {
            let key = Key::new(&Uuid::from_bytes(namespace), &data);
            let canonical = String::from(&key);
            let upper = canonical.to_uppercase();

            prop_assert_eq!(Key::try_from(canonical.as_str()).unwrap(), key.clone());
            // A UUID made only of digits would be unchanged by uppercasing;
            // there is no alias to reject in that (vanishingly rare) case.
            prop_assume!(upper != canonical);
            prop_assert_eq!(
                Key::try_from(upper.as_str()),
                Err(InvalidKey::NotCanonical)
            );
            prop_assert_eq!(Key::try_from(canonical).unwrap(), key);
        }
    }
}
