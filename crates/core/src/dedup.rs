// SPDX-License-Identifier: MPL-2.0

//! The idempotency window — the dedup store of the pure core.
//!
//! A bounded FIFO index over processed causation keys ([`Registry`]): `insert`
//! records a hit, `lookup` answers the dedup check, and the oldest entries
//! evict once the window exceeds `max_entries`. Eviction follows **insertion
//! order** (a FIFO queue), not hash-map order — so every replica folds the
//! index identically and the window stays deterministic.

use crate::key::Key;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use std::collections::VecDeque;

/// Dedup metadata for a recorded key.
///
/// Leaked through [`Registry::lookup`]'s guard return type, so it is `pub` —
/// but its fields stay private and are read through the accessors below (the
/// shell is a different crate and cannot see `pub(crate)` fields).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Log index of the first event the recorded command produced.
    pub(crate) first_log_index: usize,
    /// Fingerprint of the recorded command's intent.
    pub(crate) fingerprint: Key,
}

impl Entry {
    /// Log index of the first event the recorded command produced.
    #[must_use]
    pub fn first_log_index(&self) -> usize {
        self.first_log_index
    }

    /// Fingerprint of the recorded command's intent
    /// ([`Command::fingerprint`](crate::envelope::Command::fingerprint)).
    ///
    /// A hit whose fingerprint differs from the incoming command's is a
    /// **reused idempotency key on a different request**: answer a conflict,
    /// never replay the recorded result (D12).
    #[must_use]
    pub fn fingerprint(&self) -> &Key {
        &self.fingerprint
    }
}

/// A bounded, deterministic idempotency window keyed by the command's
/// `causation_key`.
///
/// The only dedup store of the core (P3): the shell records the causation
/// key *after* the events are durably appended and applied, so a `lookup`
/// hit means the command was already processed.
#[derive(Debug)]
pub struct Registry {
    max_entries: usize,
    window: VecDeque<Key>,
    entries: DashMap<Key, Entry>,
}

impl Registry {
    /// Creates a dedup window that holds at most `max_entries` ids before
    /// evicting the oldest.
    #[must_use]
    pub fn new(max_entries: usize) -> Registry {
        Registry {
            max_entries,
            entries: DashMap::new(),
            window: VecDeque::new(),
        }
    }

    /// Returns the dedup metadata for `key`, if it was recorded.
    ///
    /// The returned guard borrows the registry — drop it (or clone the
    /// [`Entry`]) before taking another concurrent lookup, or use the
    /// `dashmap::mapref::one::Ref` deref to read the metadata directly.
    #[must_use]
    pub fn lookup(&self, key: &Key) -> Option<Ref<'_, Key, Entry>> {
        self.entries.get(key)
    }

    /// Records `key`, evicting the oldest recorded key once the window is full.
    ///
    /// `fingerprint` is the recorded command's intent fingerprint
    /// ([`Command::fingerprint`](crate::envelope::Command::fingerprint)):
    /// recording it is what lets a later hit be checked for *key reuse* — the
    /// same `causation_key` arriving with a different intent — instead of
    /// blindly replaying the first result.
    ///
    /// Recording an already-recorded key is a no-op (idempotent) — the shell
    /// checks [`lookup`](Registry::lookup) first and treats a hit as a
    /// dedup replay instead of re-executing.
    ///
    /// Takes `&mut self`: the FIFO frontier is plain data, mutated in place
    /// during the fold (unlike `lookup`, which stays `&self` for concurrent
    /// shell reads via `dashmap`).
    pub fn insert(&mut self, key: Key, fingerprint: Key, first_log_index: usize) {
        if self.entries.contains_key(&key) {
            return;
        }

        while self.window.len() >= self.max_entries {
            let Some(oldest) = self.window.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }

        self.window.push_back(key.clone());
        self.entries.insert(
            key,
            Entry {
                first_log_index,
                fingerprint,
            },
        );
    }

    /// The recorded entries, in **insertion order** — the FIFO window that
    /// [`insert`](Registry::insert) evicts from.
    ///
    /// Each tuple is `(causation_key, intent fingerprint, first_log_index)`.
    /// Re-inserting them in this order rebuilds an identical registry, which is
    /// how a shell serializes the window into a state-machine snapshot.
    #[must_use]
    pub fn window_entries(&self) -> Vec<(Key, Key, usize)> {
        self.window
            .iter()
            .filter_map(|key| {
                self.entries.get(key).map(|entry| {
                    (
                        key.clone(),
                        entry.fingerprint.clone(),
                        entry.first_log_index,
                    )
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// A stable namespace for the deterministic test keys below.
    const KEY_NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

    fn key(n: u8) -> Key {
        Key::new(&KEY_NS, &format!("id-{n}"))
    }

    /// The intent fingerprint recorded alongside `key(n)`.
    fn fingerprint(n: u8) -> Key {
        Key::new(&KEY_NS, &format!("intent-{n}"))
    }

    #[test]
    fn records_and_lookup_hits() {
        let mut reg = Registry::new(10);

        assert!(reg.lookup(&key(1)).is_none());

        reg.insert(key(1), fingerprint(1), 7);
        let hit = reg.lookup(&key(1)).unwrap();
        assert_eq!(hit.first_log_index, 7);
        assert_eq!(hit.first_log_index(), 7);
        // The recorded intent survives, so reuse can be detected later.
        assert_eq!(hit.fingerprint(), &fingerprint(1));
    }

    #[test]
    fn evicts_oldest_when_window_is_full() {
        let mut reg = Registry::new(2);

        reg.insert(key(1), fingerprint(1), 1);
        reg.insert(key(2), fingerprint(2), 2);
        assert!(reg.lookup(&key(1)).is_some());

        reg.insert(key(3), fingerprint(3), 3);

        assert!(reg.lookup(&key(1)).is_none()); // oldest, evicted
        assert!(reg.lookup(&key(2)).is_some());
        assert!(reg.lookup(&key(3)).is_some());
    }

    #[test]
    fn window_stays_bounded() {
        let mut reg = Registry::new(3);

        for n in 1..=100 {
            reg.insert(key(n), fingerprint(n), usize::from(n));
        }

        assert_eq!(reg.window.len(), 3);
        assert!(reg.lookup(&key(97)).is_none()); // evicted long ago
        assert!(reg.lookup(&key(98)).is_some());
        assert!(reg.lookup(&key(100)).is_some());
    }

    #[test]
    fn window_entries_are_in_insertion_order() {
        let mut reg = Registry::new(2);

        reg.insert(key(1), fingerprint(1), 1);
        reg.insert(key(2), fingerprint(2), 2);
        reg.insert(key(3), fingerprint(3), 3); // evicts key(1)

        let entries = reg.window_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (key(2), fingerprint(2), 2));
        assert_eq!(entries[1], (key(3), fingerprint(3), 3));

        // Re-inserting them in order rebuilds the same window.
        let mut rebuilt = Registry::new(2);
        for (cause, intent, index) in reg.window_entries() {
            rebuilt.insert(cause, intent, index);
        }
        assert_eq!(rebuilt.window_entries(), entries);
    }

    #[test]
    fn re_inserting_a_recorded_id_is_a_no_op() {
        let mut reg = Registry::new(2);

        reg.insert(key(1), fingerprint(1), 1);
        // Same key: neither the index nor the recorded intent is refreshed.
        reg.insert(key(1), fingerprint(9), 999);
        reg.insert(key(2), fingerprint(2), 2);

        let hit = reg.lookup(&key(1)).unwrap();
        assert_eq!(hit.first_log_index(), 1);
        assert_eq!(hit.fingerprint(), &fingerprint(1));
        assert_eq!(reg.window.len(), 2);
    }

    // ---------------------------------------------------------------------
    // Property tests: the registry must behave exactly like a tiny reference
    // model (a bounded FIFO of distinct ids) under arbitrary insert storms.

    use proptest::prelude::*;

    proptest! {
        // After ANY sequence of check-then-insert operations, the registry
        // hits exactly the ids its reference model would keep: at most
        // `max_entries` of the most recently seen distinct ids.
        #[test]
        fn window_matches_reference_model(
            max_entries in 1usize..=10,
            ids in prop::collection::vec(any::<u8>(), 0..200),
        ) {
            let mut reg = Registry::new(max_entries);
            // Reference model: distinct ids in insertion order, capped at the
            // window size — the exact eviction semantics we claim.
            let mut model: VecDeque<u8> = VecDeque::new();
            let mut seen: std::collections::HashSet<u8> = std::collections::HashSet::new();

            for raw in ids {
                let id = key(raw);
                let first_log_index = usize::from(raw);

                // Shell semantics: dedup check first, execute only on a miss.
                if reg.lookup(&id).is_some() {
                    continue;
                }
                reg.insert(id, fingerprint(raw), first_log_index);

                seen.insert(raw);
                if !model.contains(&raw) {
                    if model.len() >= max_entries {
                        model.pop_front();
                    }
                    model.push_back(raw);
                }
            }

            // The window is bounded and in sync with the model.
            assert!(reg.window.len() <= max_entries);
            assert_eq!(reg.window.len(), model.len());

            // Every model survivor is a hit; every hit's metadata is intact.
            for raw in &model {
                let hit = reg.lookup(&key(*raw)).unwrap();
                assert_eq!(hit.first_log_index, usize::from(*raw));
                assert_eq!(hit.fingerprint(), &fingerprint(*raw));
            }

            // Every id that was seen but NOT a model survivor is a miss:
            // eviction removed exactly the ids the model dropped.
            for raw in &seen {
                if !model.contains(raw) {
                    assert!(reg.lookup(&key(*raw)).is_none());
                }
            }
        }

        // Re-inserting an already-recorded id never refreshes its metadata:
        // the first recorded log index survives, even if a "retry" proposes a
        // different one.
        #[test]
        fn re_insert_never_refreshes_first_index(
            raw in any::<u8>(),
            max_entries in 1usize..=10,
        ) {
            let mut reg = Registry::new(max_entries);
            let id = key(raw);

            reg.insert(id.clone(), fingerprint(raw), usize::from(raw));
            reg.insert(id.clone(), fingerprint(raw), usize::from(raw) + 1000);

            let hit = reg.lookup(&id).unwrap();
            assert_eq!(hit.first_log_index(), usize::from(raw));
            assert_eq!(reg.window.len(), 1);
        }
    }
}
