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
    /// How long a streaming RPC may produce nothing before it is treated as
    /// stalled. Deliberately its own knob: the RPC timeouts below and openraft's
    /// `hard_ttl`/`soft_ttl` are per-request budgets (on the replication path
    /// `hard_ttl` is the heartbeat interval), and aborting a replication stream
    /// on a per-response deadline tears it down mid-burst under ordinary load.
    pub stream_stall_timeout_ms: u64,
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
            stream_stall_timeout_ms: 10_000,
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
                && self.stream_stall_timeout_ms > 0
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
        // Snapshot fragments are the transport's own framing: a chunk that does
        // not fit a transport message would turn a lagging follower into a
        // snapshot transfer that can never complete.
        anyhow::ensure!(
            self.raft.snapshot_max_chunk_size.saturating_mul(2)
                <= u64::try_from(self.transport.max_message_bytes)?,
            "snapshot chunk size must leave room for transport encoding"
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

/// The HTTP gateway's bind address and read-your-writes hold.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    /// The address the gateway listens on (`host:port`).
    ///
    /// Port `0` asks the operating system for a free port, which is what tests
    /// and ephemeral deployments use.
    pub bind: String,
    /// How long a read waits for the caller's own write (`X-Min-Index`).
    pub ryw_hold_ms: u64,
}
impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".to_owned(),
            ryw_hold_ms: 50,
        }
    }
}
impl HttpConfig {
    /// Checks the bind address shape.
    /// # Errors
    /// Returns an error when the address has no host or a bad port.
    pub fn validate(&self) -> anyhow::Result<()> {
        let (host, port) = self
            .bind
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("http.bind must be host:port"))?;
        anyhow::ensure!(!host.trim().is_empty(), "http.bind must have a host");
        port.parse::<u16>()
            .map_err(|_| anyhow::anyhow!("http.bind port must be a number"))?;
        Ok(())
    }
}

/// The NATS `JetStream` runtime's connection, stream and consumer settings.
///
/// The stream, subjects and dedup identity follow design D11; the defaults match
/// the constants the outbox and the integration suite use.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NatsConfig {
    /// The broker URL (`nats://host:4222`).
    pub url: String,
    /// The stream the outbox publishes into.
    pub stream: String,
    /// The subjects the stream captures: everything the outbox publishes.
    pub subjects: String,
    /// The subset the saga consumer subscribes to, when narrower than the stream.
    ///
    /// Defaults to [`NatsConfig::subjects`]. A host that runs only some tenants
    /// (or a test that must not see other tenants' events) narrows this without
    /// changing what the stream holds.
    pub filter_subject: Option<String>,
    /// The durable consumer name the saga runner pulls with.
    pub durable: String,
    /// How long the broker waits before redelivering an unacked message.
    pub ack_wait_ms: u64,
    /// The broker's dedup window for the outbox's message ids (D11).
    ///
    /// The persisted cursor is what makes a restart correct; the window is the
    /// second line of defence and only applies to an existing stream.
    pub duplicate_window_secs: u64,
    /// Whether the saga consumer starts from the beginning of the stream.
    ///
    /// `true` (the default) replays what the stream already holds, so a host that
    /// was down while an event was published still runs its saga; handlers are
    /// replay-safe by derived identity (D12).
    pub deliver_all: bool,
    /// Connection deadline.
    pub connect_timeout_ms: u64,
    /// Per-message publish deadline.
    pub publish_timeout_ms: u64,
}
impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".to_owned(),
            stream: "LOOMERY_OUTBOX".to_owned(),
            subjects: "loomery.>".to_owned(),
            filter_subject: None,
            durable: "loomery-sagas".to_owned(),
            ack_wait_ms: 30_000,
            duplicate_window_secs: 120,
            deliver_all: true,
            connect_timeout_ms: 5_000,
            publish_timeout_ms: 5_000,
        }
    }
}
impl NatsConfig {
    /// Checks the connection and consumer settings.
    /// # Errors
    /// Returns an error for an empty URL/stream/subject/durable name or a zero
    /// timeout.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.url.trim().is_empty(), "nats.url must not be empty");
        anyhow::ensure!(
            !self.stream.trim().is_empty(),
            "nats.stream must not be empty"
        );
        anyhow::ensure!(
            !self.subjects.trim().is_empty(),
            "nats.subjects must not be empty"
        );
        anyhow::ensure!(
            !self.durable.trim().is_empty(),
            "nats.durable must not be empty"
        );
        anyhow::ensure!(
            self.filter_subject
                .as_ref()
                .is_none_or(|filter| !filter.trim().is_empty()),
            "nats.filter_subject must not be empty when set"
        );
        anyhow::ensure!(
            self.ack_wait_ms > 0 && self.connect_timeout_ms > 0 && self.publish_timeout_ms > 0,
            "nats timeouts must be positive"
        );
        anyhow::ensure!(
            self.duplicate_window_secs > 0,
            "nats.duplicate_window_secs must be positive"
        );
        Ok(())
    }
}

/// Provider-agnostic OIDC settings.
///
/// Nothing here is provider-specific: the issuer drives discovery, the keys come
/// from the discovered JWKS (or an explicit [`OidcConfig::jwks_uri`]), and the
/// claim names are configurable so Keycloak, Entra ID, Auth0, Okta or a
/// self-hosted provider all work through the same adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OidcConfig {
    /// The issuer URL; discovery reads `{issuer}/.well-known/openid-configuration`.
    pub issuer: String,
    /// Use this JWKS URL instead of discovery (providers without it).
    pub jwks_uri: Option<String>,
    /// The `aud` claim a token must carry, when the provider sets one.
    pub audience: Option<String>,
    /// The claim carrying the subject: the user id a token authenticates as.
    ///
    /// `sub` per `OpenID` Connect; configurable because not every provider puts the
    /// subject there (some issue it only in the id token, some use a custom
    /// claim).
    pub subject_claim: String,
    /// The claim carrying the caller's email address.
    pub email_claim: String,
    /// Whether an email must be *verified* before it is trusted.
    ///
    /// `true` (the default) requires the provider's `email_verified` claim to be
    /// true; a provider that omits it must set this to `false`, which means any
    /// address the provider issues is taken at face value.
    pub require_verified_email: bool,
    /// The claim carrying group/role membership; dot paths address nested claims.
    pub groups_claim: String,
    /// The value in that claim that grants the system-admin claim.
    pub admin_group: String,
    /// When set, a `sub` that is not a canonical UUID is derived into one in
    /// this namespace (`UUIDv5`), so any provider's subject becomes an [`Id`].
    pub subject_namespace: Option<String>,
    /// Clock-skew allowance when checking `exp`/`nbf`.
    pub leeway_seconds: u64,
    /// How long a fetched key set is trusted before it is refetched.
    pub jwks_ttl_seconds: u64,
    /// HTTP deadline for discovery and JWKS requests.
    pub timeout_ms: u64,
}
impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            issuer: String::new(),
            jwks_uri: None,
            audience: None,
            subject_claim: "sub".to_owned(),
            email_claim: "email".to_owned(),
            require_verified_email: true,
            groups_claim: "groups".to_owned(),
            admin_group: "admins".to_owned(),
            subject_namespace: None,
            leeway_seconds: 30,
            jwks_ttl_seconds: 600,
            timeout_ms: 5_000,
        }
    }
}
impl OidcConfig {
    /// Checks the issuer, key source and claim settings.
    /// # Errors
    /// Returns an error for an issuer without an `http(s)://` scheme, an empty
    /// claim name, a malformed namespace or a zero timeout.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.issuer.starts_with("http://") || self.issuer.starts_with("https://"),
            "oidc.issuer must start with http:// or https://"
        );
        anyhow::ensure!(
            self.jwks_uri
                .as_ref()
                .is_none_or(|uri| !uri.trim().is_empty()),
            "oidc.jwks_uri must not be empty when set"
        );
        anyhow::ensure!(
            !self.subject_claim.trim().is_empty(),
            "oidc.subject_claim must not be empty"
        );
        anyhow::ensure!(
            !self.email_claim.trim().is_empty(),
            "oidc.email_claim must not be empty"
        );
        anyhow::ensure!(
            !self.groups_claim.trim().is_empty(),
            "oidc.groups_claim must not be empty"
        );
        anyhow::ensure!(
            !self.admin_group.trim().is_empty(),
            "oidc.admin_group must not be empty"
        );
        if let Some(namespace) = &self.subject_namespace {
            loomery_core::Uuid::parse_str(namespace)
                .map_err(|_| anyhow::anyhow!("oidc.subject_namespace must be a UUID"))?;
        }
        anyhow::ensure!(
            self.timeout_ms > 0 && self.jwks_ttl_seconds > 0,
            "oidc timeouts must be positive"
        );
        Ok(())
    }
}

/// The runtime host's configuration: consensus, storage, HTTP, broker and `IdP`.
///
/// This is what `loomery-server` loads (JSON, via `LOOMERY_CONFIG` or `--config`)
/// before booting the control group, the tenant groups and the workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostConfig {
    /// This replica's Raft node id (unique within every group it hosts).
    pub node_id: u64,
    /// Where group databases and outbox cursors live.
    pub data_dir: PathBuf,
    /// The control group's id, which is also its database directory name.
    pub control_group: String,
    /// This node's address as its peers dial it.
    ///
    /// Every placement this host writes records it (D1). A single-node host needs
    /// it only for the record; a multi-node placement is the control plane's.
    pub node_address: String,
    /// The HTTP gateway.
    pub http: HttpConfig,
    /// Consensus, transport, storage and proposal settings for every group.
    pub group: GroupConfig,
    /// The NATS runtime; absent disables the outbox and saga workers.
    pub nats: Option<NatsConfig>,
    /// The identity provider; required to serve HTTP (the host fails closed).
    pub oidc: Option<OidcConfig>,
}
impl Default for HostConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            data_dir: PathBuf::from("data"),
            control_group: "control".to_owned(),
            node_address: "http://127.0.0.1:7001".to_owned(),
            http: HttpConfig::default(),
            group: GroupConfig::default(),
            nats: None,
            oidc: None,
        }
    }
}
impl HostConfig {
    /// The host's shape is valid, ignoring whether the optional runtimes are on.
    /// # Errors
    /// Returns an error for a zero node id, an empty path/group id, or any
    /// invalid nested section.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.node_id > 0, "node_id must be positive");
        anyhow::ensure!(
            !self.data_dir.as_os_str().is_empty(),
            "data_dir must not be empty"
        );
        anyhow::ensure!(
            !self.control_group.trim().is_empty(),
            "control_group must not be empty"
        );
        anyhow::ensure!(
            !self.control_group.contains('.'),
            "control_group is a NATS subject segment (D11): it must not contain a dot"
        );
        anyhow::ensure!(
            !self.node_address.trim().is_empty()
                && self.node_address.len() <= loomery_core::tenant::MAX_ADDRESS_BYTES,
            "node_address must be a non-empty address within the placement bounds"
        );
        self.http.validate()?;
        self.group.validate()?;
        if let Some(nats) = &self.nats {
            nats.validate()?;
        }
        if let Some(oidc) = &self.oidc {
            oidc.validate()?;
        }
        Ok(())
    }

    /// Parses a JSON configuration document.
    /// # Errors
    /// Returns an error for malformed JSON or an unknown field.
    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    /// Loads `path` and applies the environment overrides.
    /// # Errors
    /// Returns an error when the file cannot be read, parsed or overridden.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let json = std::fs::read_to_string(path)
            .map_err(|error| anyhow::anyhow!("reading {}: {error}", path.display()))?;
        Self::from_json(&json)?.with_env_from(|name| std::env::var(name).ok())
    }

    /// Applies the `LOOMERY_*` environment overrides.
    /// # Errors
    /// Returns an error when an override cannot be parsed.
    pub fn with_env(self) -> anyhow::Result<Self> {
        self.with_env_from(|name| std::env::var(name).ok())
    }

    /// Applies the `LOOMERY_*` overrides from `lookup`.
    ///
    /// Split out from [`HostConfig::with_env`] so the behaviour is testable
    /// without mutating the process environment.
    ///
    /// # Errors
    /// Returns an error when a numeric override cannot be parsed.
    pub fn with_env_from(
        mut self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        if let Some(node_id) = lookup("LOOMERY_NODE_ID") {
            self.node_id = node_id
                .parse()
                .map_err(|_| anyhow::anyhow!("LOOMERY_NODE_ID must be a number"))?;
        }
        if let Some(dir) = lookup("LOOMERY_DATA_DIR") {
            self.data_dir = PathBuf::from(dir);
        }
        if let Some(bind) = lookup("LOOMERY_HTTP_BIND") {
            self.http.bind = bind;
        }
        // Setting any broker value turns the runtime on, so a deployment needs
        // one variable rather than a whole section.
        if let Some(url) = lookup("LOOMERY_NATS_URL") {
            self.nats.get_or_insert_with(NatsConfig::default).url = url;
        }
        if let Some(issuer) = lookup("LOOMERY_OIDC_ISSUER") {
            self.oidc.get_or_insert_with(OidcConfig::default).issuer = issuer;
        }
        if let Some(jwks) = lookup("LOOMERY_OIDC_JWKS_URI") {
            self.oidc.get_or_insert_with(OidcConfig::default).jwks_uri = Some(jwks);
        }
        if let Some(audience) = lookup("LOOMERY_OIDC_AUDIENCE") {
            self.oidc.get_or_insert_with(OidcConfig::default).audience = Some(audience);
        }
        if let Some(admin) = lookup("LOOMERY_OIDC_ADMIN_GROUP") {
            self.oidc
                .get_or_insert_with(OidcConfig::default)
                .admin_group = admin;
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lookup with no environment at all.
    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn a_minimal_document_uses_the_documented_defaults() {
        let config = HostConfig::from_json("{}").unwrap();

        assert_eq!(config.node_id, 1);
        assert_eq!(config.data_dir, PathBuf::from("data"));
        assert_eq!(config.control_group, "control");
        assert_eq!(config.http.bind, "127.0.0.1:8080");
        assert_eq!(config.http.ryw_hold_ms, 50);
        assert!(config.nats.is_none(), "the broker runtime is opt-in");
        assert!(config.oidc.is_none(), "the provider is opt-in");
        config.validate().unwrap();
    }

    #[test]
    fn a_full_document_parses_and_round_trips() {
        let json = r#"{
            "node_id": 3,
            "data_dir": "/var/lib/loomery",
            "control_group": "control",
            "http": { "bind": "0.0.0.0:9000", "ryw_hold_ms": 120 },
            "nats": {
                "url": "nats://broker:4222",
                "stream": "OUTBOX",
                "subjects": "loomery.>",
                "durable": "sagas",
                "ack_wait_ms": 15000
            },
            "oidc": {
                "issuer": "https://idp.example.com/realms/main",
                "audience": "loomery-gateway",
                "groups_claim": "realm_access.roles",
                "admin_group": "platform-admins",
                "subject_namespace": "5c1a9f43-2e78-4b06-8d17-a2c3b4d5e6f7",
                "leeway_seconds": 5
            }
        }"#;
        let config = HostConfig::from_json(json).unwrap();
        config.validate().unwrap();

        assert_eq!(config.node_id, 3);
        assert_eq!(config.http.bind, "0.0.0.0:9000");
        let nats = config.nats.as_ref().unwrap();
        assert_eq!(nats.url, "nats://broker:4222");
        assert_eq!(nats.ack_wait_ms, 15_000);
        assert_eq!(nats.connect_timeout_ms, 5_000, "unspecified fields default");
        let oidc = config.oidc.as_ref().unwrap();
        assert_eq!(oidc.groups_claim, "realm_access.roles");
        assert_eq!(oidc.admin_group, "platform-admins");
        assert_eq!(oidc.leeway_seconds, 5);
        assert_eq!(oidc.jwks_ttl_seconds, 600);

        // Serializing the parsed config and parsing it again is stable.
        let again = HostConfig::from_json(&serde_json::to_string(&config).unwrap()).unwrap();
        assert_eq!(again.node_id, config.node_id);
        assert_eq!(
            again.oidc.unwrap().issuer,
            "https://idp.example.com/realms/main"
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(HostConfig::from_json(r#"{"node_idd": 1}"#).is_err());
        assert!(HostConfig::from_json(r#"{"http": {"bindd": "x"}}"#).is_err());
        assert!(HostConfig::from_json("not json").is_err());
    }

    #[test]
    fn environment_overrides_apply_and_can_enable_a_runtime() {
        let config = HostConfig::default()
            .with_env_from(|name| match name {
                "LOOMERY_NODE_ID" => Some("7".to_owned()),
                "LOOMERY_DATA_DIR" => Some("/srv/loomery".to_owned()),
                "LOOMERY_HTTP_BIND" => Some("0.0.0.0:9100".to_owned()),
                "LOOMERY_NATS_URL" => Some("nats://from-env:4222".to_owned()),
                "LOOMERY_OIDC_ISSUER" => Some("https://idp/realms/x".to_owned()),
                "LOOMERY_OIDC_ADMIN_GROUP" => Some("ops".to_owned()),
                _ => None,
            })
            .unwrap();

        assert_eq!(config.node_id, 7);
        assert_eq!(config.data_dir, PathBuf::from("/srv/loomery"));
        assert_eq!(config.http.bind, "0.0.0.0:9100");
        assert_eq!(config.nats.as_ref().unwrap().url, "nats://from-env:4222");
        let oidc = config.oidc.as_ref().unwrap();
        assert_eq!(oidc.issuer, "https://idp/realms/x");
        assert_eq!(oidc.admin_group, "ops");
        assert_eq!(oidc.groups_claim, "groups", "untouched defaults survive");
        config.validate().unwrap();
    }

    #[test]
    fn the_environment_is_optional() {
        let config = HostConfig::default().with_env_from(no_env).unwrap();
        assert!(config.nats.is_none());
    }

    #[test]
    fn a_malformed_override_is_an_error() {
        let error = HostConfig::default()
            .with_env_from(|name| (name == "LOOMERY_NODE_ID").then(|| "seven".to_owned()))
            .unwrap_err();
        assert!(error.to_string().contains("LOOMERY_NODE_ID"));
    }

    #[test]
    fn validation_rejects_bad_shapes() {
        /// A named mutation applied to a default configuration.
        type Mutation = Box<dyn Fn(&mut HostConfig)>;

        let cases: Vec<(&str, Mutation)> = vec![
            ("node_id", Box::new(|c: &mut HostConfig| c.node_id = 0)),
            (
                "data_dir",
                Box::new(|c: &mut HostConfig| c.data_dir = PathBuf::new()),
            ),
            (
                "control_group",
                Box::new(|c: &mut HostConfig| c.control_group = "  ".to_owned()),
            ),
            (
                "control_group segment",
                Box::new(|c: &mut HostConfig| c.control_group = "control.group".to_owned()),
            ),
            (
                "bind",
                Box::new(|c: &mut HostConfig| c.http.bind = "127.0.0.1".to_owned()),
            ),
            (
                "port",
                Box::new(|c: &mut HostConfig| c.http.bind = "127.0.0.1:not-a-port".to_owned()),
            ),
            (
                "url",
                Box::new(|c: &mut HostConfig| c.nats = Some(NatsConfig::default())),
            ),
            (
                "issuer",
                Box::new(|c: &mut HostConfig| {
                    c.oidc = Some(OidcConfig {
                        issuer: "idp.example.com".to_owned(),
                        ..OidcConfig::default()
                    });
                }),
            ),
            (
                "subject_namespace",
                Box::new(|c: &mut HostConfig| {
                    c.oidc = Some(OidcConfig {
                        issuer: "https://idp".to_owned(),
                        subject_namespace: Some("not-a-uuid".to_owned()),
                        ..OidcConfig::default()
                    });
                }),
            ),
        ];

        for (label, mutate) in cases {
            let mut config = HostConfig::default();
            mutate(&mut config);
            if label == "url" {
                config.nats = Some(NatsConfig {
                    url: String::new(),
                    ..NatsConfig::default()
                });
            }
            assert!(config.validate().is_err(), "{label} should not validate");
        }
    }

    #[test]
    fn an_issuer_without_a_scheme_is_rejected() {
        let config = HostConfig {
            oidc: Some(OidcConfig {
                issuer: "idp.example.com/realms/main".to_owned(),
                ..OidcConfig::default()
            }),
            ..HostConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
