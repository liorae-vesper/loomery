//! Injected timestamps for the Trellis core.
//!
//! [`Timestamp`] is a millisecond-since-epoch value. The pure core never
//! reads the clock — timestamps arrive through the command envelope — so
//! replays and replicated applies stay deterministic (D5, `docs/design.md`).

use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// A millisecond-since-epoch timestamp.
///
/// Injected into command envelopes by the shell; the core treats it as
/// opaque, ordered data. Serializes as a bare `JSON` number, keeping the
/// wire format compact and deterministic.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone, Deserialize, Serialize)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The current wall-clock time in milliseconds.
    ///
    /// **Shell-side convenience only** — the pure core must use values
    /// injected through the command envelope so replays stay deterministic.
    #[must_use]
    pub fn now() -> Self {
        let ms = chrono::Utc::now().timestamp_millis();
        Timestamp(ms)
    }

    /// Returns the millisecond value.
    #[must_use]
    pub fn as_millis(&self) -> i64 {
        self.0
    }
}

impl From<i64> for Timestamp {
    fn from(ms: i64) -> Self {
        Timestamp(ms)
    }
}

impl From<Timestamp> for i64 {
    fn from(t: Timestamp) -> Self {
        t.0
    }
}

impl Deref for Timestamp {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_i64_and_as_millis_round_trip() {
        let t = Timestamp::from(1_700_000_000_000);
        assert_eq!(t.as_millis(), 1_700_000_000_000);
    }

    #[test]
    fn deref_exposes_the_millis() {
        let t = Timestamp::from(42);
        let ms: i64 = *t;
        assert_eq!(ms, 42);
    }

    #[test]
    fn ordering_follows_millis() {
        let older = Timestamp::from(1000);
        let newer = Timestamp::from(2000);
        assert!(older < newer);
        assert_eq!(older.min(newer), Timestamp::from(1000)); // Ord
    }

    #[test]
    fn serde_round_trip_uses_a_bare_json_number() {
        let t = Timestamp::from(1_700_000_000_000);
        let json = serde_json::to_string(&t).unwrap();
        assert_eq!(json, "1700000000000");
        let decoded: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, t);
    }

    #[test]
    fn now_tracks_the_wall_clock() {
        let before = chrono::Utc::now().timestamp_millis();
        let t = Timestamp::now();
        let after = chrono::Utc::now().timestamp_millis();
        assert!(before <= t.as_millis() && t.as_millis() <= after);
    }
}
