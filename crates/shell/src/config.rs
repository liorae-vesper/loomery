// SPDX-License-Identifier: MPL-2.0
//! Serializable tuning knobs for persistent networked groups.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    /// Opt-in inbound TLS; absent means plaintext HTTP/2.
    pub server_tls: Option<ServerTls>,
    /// Opt-in outbound TLS; requires HTTPS peer addresses.
    pub client_tls: Option<ClientTls>,
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
            server_tls: None,
            client_tls: None,
        }
    }
}
/// PEM certificate chain and private key file paths. File contents are never
/// part of serialized configuration or debug output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsIdentity {
    /// PEM certificate chain file.
    pub certificate: PathBuf,
    /// PEM private key file.
    pub private_key: PathBuf,
}
/// TLS settings for the shared gRPC listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTls {
    /// Server certificate chain and private key.
    pub identity: TlsIdentity,
    /// Optional PEM CA bundle. When set, every client must present a valid
    /// certificate signed by a trusted CA (mutual TLS).
    pub client_ca_certificate: Option<PathBuf>,
}
/// TLS settings for outbound peer connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTls {
    /// PEM CA bundle for verifying peer certificates.
    pub ca_certificate: PathBuf,
    /// Optional client certificate/key for mutual TLS.
    pub identity: Option<TlsIdentity>,
    /// Optional expected certificate name; defaults to the peer URI host.
    /// Useful when connecting by IP to certificates with a DNS identity.
    pub server_name: Option<String>,
}
impl TlsIdentity {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.certificate.as_os_str().is_empty() && !self.private_key.as_os_str().is_empty(),
            "TLS certificate and private key paths must not be empty"
        );
        Ok(())
    }
}
impl TransportConfig {
    /// Validates transport limits and TLS configuration shapes.
    /// # Errors
    /// Returns an error for invalid limits, empty paths or empty server names.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.connect_timeout_ms > 0
                && self.request_timeout_ms > 0
                && self.max_message_bytes > 0
                && self.tcp_keepalive_ms > 0
                && self.stream_window_bytes > 0
                && self.connection_window_bytes > 0,
            "transport limits must be positive"
        );
        if let Some(tls) = &self.server_tls {
            tls.identity.validate()?;
            anyhow::ensure!(
                tls.client_ca_certificate
                    .as_ref()
                    .is_none_or(|p| !p.as_os_str().is_empty()),
                "client CA certificate path must not be empty"
            );
        }
        if let Some(tls) = &self.client_tls {
            anyhow::ensure!(
                !tls.ca_certificate.as_os_str().is_empty(),
                "peer CA certificate path must not be empty"
            );
            anyhow::ensure!(
                tls.server_name
                    .as_ref()
                    .is_none_or(|s| !s.trim().is_empty()),
                "TLS server name must not be empty"
            );
            if let Some(identity) = &tls.identity {
                identity.validate()?;
            }
        }
        Ok(())
    }
}
/// Recovery contract for applied state. Snapshot-backed mode is experimental.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatePersistence {
    /// Synchronize the complete applied state before each apply batch returns.
    #[default]
    Checkpoint,
    /// Apply in memory; recover from durable snapshots and the committed log.
    Snapshot,
}
/// `RocksDB` resource tuning. WAL synchronization is always enabled for Raft safety.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Experimental recovery mode; fixed for the lifetime of a database.
    pub state_persistence: StatePersistence,
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
            state_persistence: StatePersistence::Checkpoint,
            write_buffer_bytes: 64 * 1024 * 1024,
            max_write_buffers: 2,
            max_background_jobs: 2,
            max_open_files: 512,
            block_cache_bytes: 64 * 1024 * 1024,
        }
    }
}
/// Leader proposal queue and command batching. Enable only after every replica
/// supports batch entries; one command per batch preserves the legacy format.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProposalConfig {
    /// Maximum commands sharing one Raft entry. One disables batching.
    pub max_batch_commands: usize,
    /// Maximum sum of serialized command bytes per batch (excluding envelope).
    /// With batching enabled, larger individual commands are rejected locally.
    pub max_batch_bytes: usize,
    /// Maximum collection delay after the first queued command, in milliseconds.
    /// Zero collects only commands already queued.
    pub max_delay_ms: u64,
    /// Channel capacity; producers wait when full. The worker additionally
    /// holds a collected/in-flight batch and at most one deferred command.
    pub queue_capacity: usize,
}
impl Default for ProposalConfig {
    fn default() -> Self {
        Self {
            max_batch_commands: 1,
            max_batch_bytes: 256 * 1024,
            max_delay_ms: 1,
            queue_capacity: 1024,
        }
    }
}
impl ProposalConfig {
    /// Checks queue and batch limits before spawning a writer.
    /// # Errors
    /// Returns an error when any count or byte limit is zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_batch_commands > 0 && self.max_batch_bytes > 0 && self.queue_capacity > 0,
            "proposal batch count, bytes and queue capacity must be positive"
        );
        Ok(())
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
    /// Opt-in batching of concurrent commands before proposing one Raft entry.
    pub proposals: ProposalConfig,
}
impl GroupConfig {
    /// Checks limits before opening a database or starting a node.
    /// # Errors
    /// Returns an error for zero/invalid resource limits or consensus timings.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.raft.clone().validate()?;
        self.transport.validate()?;
        self.proposals.validate()?;
        anyhow::ensure!(
            self.proposals.max_batch_commands == 1
                || self.proposals.max_batch_bytes <= self.transport.max_message_bytes / 2,
            "proposal byte limit must leave room for transport encoding"
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
