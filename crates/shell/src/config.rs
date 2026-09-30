// SPDX-License-Identifier: MPL-2.0
//! Serializable tuning knobs for persistent networked groups.
use serde::{Deserialize, Serialize};

/// gRPC transport limits and connection tuning. Durations are milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    /// Connection establishment deadline.
    pub connect_timeout_ms: u64,
    /// Upper bound on an RPC deadline (also capped by `OpenRaft`'s TTL).
    pub request_timeout_ms: u64,
    /// Maximum encoded request or response size.
    pub max_message_bytes: usize,
    /// TCP keepalive interval.
    pub tcp_keepalive_ms: u64,
    /// HTTP/2 initial stream window in bytes.
    pub stream_window_bytes: u32,
    /// HTTP/2 initial connection window in bytes.
    pub connection_window_bytes: u32,
}
impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 1000,
            request_timeout_ms: 5000,
            max_message_bytes: 16 * 1024 * 1024,
            tcp_keepalive_ms: 30000,
            stream_window_bytes: 1024 * 1024,
            connection_window_bytes: 4 * 1024 * 1024,
        }
    }
}
/// `RocksDB` resource tuning. WAL synchronization is always enabled for Raft safety.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Bytes allocated per memtable.
    pub write_buffer_bytes: usize,
    /// Maximum number of memtables.
    pub max_write_buffers: i32,
    /// Background flush/compaction job limit.
    pub max_background_jobs: i32,
    /// Open file limit (-1 means unlimited).
    pub max_open_files: i32,
    /// Shared block cache capacity in bytes.
    pub block_cache_bytes: usize,
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            write_buffer_bytes: 64 * 1024 * 1024,
            max_write_buffers: 2,
            max_background_jobs: 2,
            max_open_files: 512,
            block_cache_bytes: 64 * 1024 * 1024,
        }
    }
}
/// Settings for a group; `OpenRaft`'s configuration exposes election, heartbeat,
/// replication and snapshot tuning directly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GroupConfig {
    /// Consensus configuration, validated before startup.
    pub raft: openraft::Config,
    /// Peer transport settings.
    pub transport: TransportConfig,
    /// Persistent storage settings.
    pub storage: StorageConfig,
}
impl GroupConfig {
    /// Checks limits before opening a database or starting a node.
    /// # Errors
    /// Returns an error for zero/invalid resource limits or consensus timings.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.raft.clone().validate()?;
        anyhow::ensure!(
            self.transport.connect_timeout_ms > 0
                && self.transport.request_timeout_ms > 0
                && self.transport.max_message_bytes > 0
                && self.transport.tcp_keepalive_ms > 0
                && self.transport.stream_window_bytes > 0
                && self.transport.connection_window_bytes > 0,
            "transport limits must be positive"
        );
        anyhow::ensure!(
            self.storage.write_buffer_bytes > 0
                && self.storage.block_cache_bytes > 0
                && self.storage.max_write_buffers >= 2
                && self.storage.max_background_jobs > 0
                && (self.storage.max_open_files == -1 || self.storage.max_open_files > 0),
            "invalid RocksDB resource limits"
        );
        Ok(())
    }
}
