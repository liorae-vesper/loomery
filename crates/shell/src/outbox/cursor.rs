// SPDX-License-Identifier: MPL-2.0

//! Where the outbox's cursor survives a restart.
//!
//! The outbox's message identity (D11) makes a republish safe *while the
//! broker's dedup window still remembers the message*. A restart after a longer
//! outage would therefore deliver duplicates — the window is minutes, not days.
//! Persisting the cursor next to the group's database closes that gap: the
//! tailer resumes where it stopped, and the broker's dedup becomes the second
//! line of defence rather than the only one.
//!
//! The file is written **after** a publish succeeded, atomically (write, sync,
//! rename), so a crash either loses at most the last acknowledged batch — which
//! the dedup window absorbs — or replays it safely.

use std::path::Path;
use std::path::PathBuf;

use super::Cursor;

/// The file name a group's cursor uses inside its database directory.
pub const CURSOR_FILE: &str = "outbox-cursor.json";

/// A group's persisted outbox cursor.
pub struct CursorStore {
    path: PathBuf,
}

impl CursorStore {
    /// A store backed by exactly `path`.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The store inside a group's database directory.
    #[must_use]
    pub fn in_group_dir(dir: &Path) -> Self {
        Self::new(dir.join(CURSOR_FILE))
    }

    /// The backing file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads the cursor.
    ///
    /// A missing file means "nothing published yet" and is not an error: a fresh
    /// deployment starts at the beginning of the log.
    ///
    /// # Errors
    ///
    /// The file exists but cannot be read, parsed, or is a malformed cursor.
    pub async fn load(&self) -> anyhow::Result<Cursor> {
        match tokio::fs::read_to_string(&self.path).await {
            Ok(json) => serde_json::from_str(&json)
                .map_err(|error| anyhow::anyhow!("parsing {}: {error}", self.path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Cursor::start()),
            Err(error) => Err(anyhow::anyhow!("reading {}: {error}", self.path.display())),
        }
    }

    /// Persists `cursor` atomically.
    ///
    /// # Errors
    ///
    /// The directory or file could not be written or renamed.
    pub async fn store(&self, cursor: Cursor) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }

        let json = serde_json::to_vec_pretty(&cursor)?;
        let temporary = self.path.with_extension("json.tmp");
        {
            use tokio::io::AsyncWriteExt;

            let mut file = tokio::fs::File::create(&temporary).await?;
            file.write_all(&json).await?;
            file.flush().await?;
            // Durability before the rename: a torn write must never replace a
            // good cursor, so the bytes reach the disk first.
            file.sync_all().await?;
        }
        tokio::fs::rename(&temporary, &self.path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_missing_cursor_file_starts_at_the_beginning() {
        let root = tempfile::tempdir().unwrap();
        let store = CursorStore::in_group_dir(root.path());
        assert_eq!(store.load().await.unwrap(), Cursor::start());
    }

    #[tokio::test]
    async fn a_cursor_round_trips_through_the_file() {
        let root = tempfile::tempdir().unwrap();
        let store = CursorStore::in_group_dir(root.path());
        let cursor = Cursor {
            log_index: 42,
            position: 3,
        };

        store.store(cursor).await.unwrap();

        assert_eq!(store.load().await.unwrap(), cursor);
        assert!(store.path().exists());
        assert_eq!(
            store.path().extension().and_then(|e| e.to_str()),
            Some("json")
        );
    }

    #[tokio::test]
    async fn storing_replaces_the_previous_cursor_without_a_temp_file_left_over() {
        let root = tempfile::tempdir().unwrap();
        let store = CursorStore::in_group_dir(root.path());

        store
            .store(Cursor {
                log_index: 1,
                position: 0,
            })
            .await
            .unwrap();
        store
            .store(Cursor {
                log_index: 9,
                position: 2,
            })
            .await
            .unwrap();

        assert_eq!(store.load().await.unwrap().log_index, 9);
        let leftovers: Vec<String> = std::fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(leftovers, vec![CURSOR_FILE.to_owned()]);
    }

    #[tokio::test]
    async fn a_malformed_cursor_file_is_an_error_not_a_silent_restart() {
        let root = tempfile::tempdir().unwrap();
        let store = CursorStore::in_group_dir(root.path());
        tokio::fs::write(store.path(), b"{ not a cursor }")
            .await
            .unwrap();

        let error = store.load().await.unwrap_err();
        assert!(error.to_string().contains("outbox-cursor.json"), "{error}");
    }

    #[tokio::test]
    async fn an_unknown_field_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let store = CursorStore::in_group_dir(root.path());
        tokio::fs::write(
            store.path(),
            br#"{"log_index": 1, "position": 0, "extra": true}"#,
        )
        .await
        .unwrap();

        assert!(store.load().await.is_err());
    }
}
