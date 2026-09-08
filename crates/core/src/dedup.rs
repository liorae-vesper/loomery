// SPDX-License-Identifier: MPL-2.0

//! The idempotency window — the dedup store of the pure core.
//!
//! A bounded FIFO index over processed command ids ([`Registry`]): `insert`
//! records a hit, `lookup` answers the dedup check, and the oldest entries
//! evict once the window exceeds `max_entries`. Eviction follows **insertion
//! order** (a FIFO queue), not hash-map order — so every replica folds the
//! index identically and the window stays deterministic.

use crate::id::Id;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use std::collections::VecDeque;

/// Dedup metadata for a recorded id.
///
/// Leaked through [`Registry::lookup`]'s guard return type, so it is `pub`
/// — but its fields stay private; callers read them through the `Ref` deref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Log index of the first event the recorded command produced.
    pub(crate) first_log_index: usize,
}

/// A bounded, deterministic idempotency window keyed by command id.
///
/// The only dedup store of the core (P3): the shell records the causation
/// key *after* the events are durably appended and applied, so a `lookup`
/// hit means the command was already processed.
pub struct Registry {
    max_entries: usize,
    window: VecDeque<Id>,
    entries: DashMap<Id, Entry>,
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

    /// Returns the dedup metadata for `id`, if it was recorded.
    ///
    /// The returned guard borrows the registry — drop it (or clone the
    /// [`Entry`]) before taking another concurrent lookup, or use the
    /// `dashmap::mapref::one::Ref` deref to read the metadata directly.
    #[must_use]
    pub fn lookup(&self, id: &Id) -> Option<Ref<'_, Id, Entry>> {
        self.entries.get(id)
    }

    /// Records `id`, evicting the oldest recorded id once the window is full.
    ///
    /// Recording an already-recorded id is a no-op (idempotent) — the shell
    /// checks [`lookup`](Registry::lookup) first and treats a hit as a
    /// dedup replay instead of re-executing.
    ///
    /// Takes `&mut self`: the FIFO frontier is plain data, mutated in place
    /// during the fold (unlike `lookup`, which stays `&self` for concurrent
    /// shell reads via `dashmap`).
    pub fn insert(&mut self, id: Id, first_log_index: usize) {
        if self.entries.contains_key(&id) {
            return;
        }

        while self.window.len() >= self.max_entries {
            let Some(oldest) = self.window.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }

        self.window.push_back(id.clone());
        self.entries.insert(id, Entry { first_log_index });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> Id {
        Id::from(format!("id-{n}"))
    }

    #[test]
    fn records_and_lookup_hits() {
        let mut reg = Registry::new(10);

        assert!(reg.lookup(&id(1)).is_none());

        reg.insert(id(1), 7);
        let hit = reg.lookup(&id(1)).unwrap();
        assert_eq!(hit.first_log_index, 7);
    }

    #[test]
    fn evicts_oldest_when_window_is_full() {
        let mut reg = Registry::new(2);

        reg.insert(id(1), 1);
        reg.insert(id(2), 2);
        assert!(reg.lookup(&id(1)).is_some());

        reg.insert(id(3), 3);

        assert!(reg.lookup(&id(1)).is_none()); // oldest, evicted
        assert!(reg.lookup(&id(2)).is_some());
        assert!(reg.lookup(&id(3)).is_some());
    }

    #[test]
    fn window_stays_bounded() {
        let mut reg = Registry::new(3);

        for n in 1..=100 {
            reg.insert(id(n), usize::from(n));
        }

        assert_eq!(reg.window.len(), 3);
        assert!(reg.lookup(&id(97)).is_none()); // evicted long ago
        assert!(reg.lookup(&id(98)).is_some());
        assert!(reg.lookup(&id(100)).is_some());
    }

    #[test]
    fn re_inserting_a_recorded_id_is_a_no_op() {
        let mut reg = Registry::new(2);

        reg.insert(id(1), 1);
        reg.insert(id(1), 999); // same key: should not refresh or duplicate
        reg.insert(id(2), 2);

        assert!(reg.lookup(&id(1)).is_some());
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
                let id = Id::from(format!("id-{raw}"));
                let first_log_index = usize::from(raw);

                // Shell semantics: dedup check first, execute only on a miss.
                if reg.lookup(&id).is_some() {
                    continue;
                }
                reg.insert(id, first_log_index);

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
                let hit = reg.lookup(&Id::from(format!("id-{raw}"))).unwrap();
                assert_eq!(hit.first_log_index, usize::from(*raw));
            }

            // Every id that was seen but NOT a model survivor is a miss:
            // eviction removed exactly the ids the model dropped.
            for raw in &seen {
                if !model.contains(raw) {
                    assert!(reg.lookup(&Id::from(format!("id-{raw}"))).is_none());
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
            let id = Id::from(format!("id-{raw}"));

            reg.insert(id.clone(), usize::from(raw));
            reg.insert(id.clone(), usize::from(raw) + 1000); // bogus retry

            let hit = reg.lookup(&id).unwrap();
            assert_eq!(hit.first_log_index, usize::from(raw));
            assert_eq!(reg.window.len(), 1);
        }
    }
}
