// SPDX-License-Identifier: MPL-2.0
//! Benchmark control protocol (stdin/stdout, separate from Raft's gRPC traffic).
use loomery_shell::config::GroupConfig;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadPattern {
    Repeated,
    #[default]
    Pseudorandom,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub nodes: u64,
    pub trials: usize,
    pub warmup: usize,
    pub operations: usize,
    pub concurrency: usize,
    pub name_bytes: usize,
    pub payload_pattern: PayloadPattern,
    pub worker_threads: usize,
    pub operation_timeout_ms: u64,
    pub phase_timeout_ms: u64,
    pub failover: bool,
    pub snapshot_before_restart: bool,
    pub crash_restart: bool,
    pub group: GroupConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            nodes: 3,
            trials: 3,
            warmup: 100,
            operations: 1000,
            concurrency: 8,
            name_bytes: 256,
            payload_pattern: PayloadPattern::Pseudorandom,
            worker_threads: 2,
            operation_timeout_ms: 10000,
            phase_timeout_ms: 120_000,
            failover: true,
            snapshot_before_restart: true,
            crash_restart: false,
            group: GroupConfig::default(),
        }
    }
}
impl Config {
    pub fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut defaults = serde_json::to_value(Self::default())?;
        merge(&mut defaults, serde_json::from_slice(bytes)?)?;
        let config: Self = serde_json::from_value(defaults)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> anyhow::Result<()> {
        self.group.validate()?;
        anyhow::ensure!(matches!(self.nodes, 1 | 3 | 5), "nodes must be 1, 3 or 5");
        anyhow::ensure!(
            self.trials > 0
                && self.operations > 0
                && self.concurrency > 0
                && self.worker_threads > 0,
            "trials, operations, concurrency and worker_threads must be positive"
        );
        anyhow::ensure!(
            self.operation_timeout_ms > 0 && self.phase_timeout_ms >= self.operation_timeout_ms,
            "phase timeout must cover a positive operation timeout"
        );
        anyhow::ensure!(
            self.name_bytes > 0 && self.name_bytes <= 1024 * 1024,
            "name_bytes must be between 1 and 1048576"
        );
        anyhow::ensure!(
            self.group.transport.server_tls.is_some() == self.group.transport.client_tls.is_some(),
            "benchmark requires both server_tls and client_tls, or neither"
        );
        Ok(())
    }
}
fn merge(base: &mut serde_json::Value, update: serde_json::Value) -> anyhow::Result<()> {
    if let (serde_json::Value::Object(base), serde_json::Value::Object(update)) =
        (&mut *base, &update)
    {
        for (key, value) in update {
            let target = base
                .get_mut(key)
                .ok_or_else(|| anyhow::anyhow!("unknown configuration field: {key}"))?;
            merge(target, value.clone())?;
        }
    } else {
        *base = update;
    }
    Ok(())
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: u64,
    pub path: PathBuf,
    pub listen: Option<std::net::SocketAddr>,
    pub config: Config,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Initialize { members: BTreeMap<u64, String> },
    Batch { phase: String, count: usize },
    Status,
    Check { index: u64, events: usize },
    Snapshot { index: u64 },
    Stop,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub sequence: usize,
    pub latency_us: u64,
    pub command_json_bytes: usize,
    pub log_index: Option<u64>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub elapsed_us: u64,
    pub samples: Vec<Sample>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub id: u64,
    pub term: u64,
    pub leader: Option<u64>,
    pub is_leader: bool,
    pub last_log: Option<u64>,
    pub applied: Option<u64>,
    pub snapshot: Option<u64>,
    pub purged: Option<u64>,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Reply {
    Ready {
        id: u64,
        pid: u32,
        address: String,
        boot_us: u64,
    },
    Initialized {
        elapsed_us: u64,
        log_index: u64,
    },
    Batch(Batch),
    Status(Status),
    Checked {
        elapsed_us: u64,
        events: usize,
        status: Status,
    },
    Snapshot {
        elapsed_us: u64,
        already_current: bool,
    },
    Stopped,
}
pub fn micros(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::Config;
    #[test]
    fn partial_configuration_preserves_defaults_and_rejects_typos() {
        let value = Config::from_json(br#"{"group":{"raft":{"heartbeat_interval":75,"election_timeout_min":200,"election_timeout_max":400}}}"#);
        assert!(value.is_ok());
        assert!(Config::from_json(br#"{"group":{"raft":{"heartbeet_interval":75}}}"#).is_err());
        assert!(Config::from_json(br#"{"concurrency":0}"#).is_err());
        assert!(Config::from_json(br#"{"nodes":2}"#).is_err());
    }
}
