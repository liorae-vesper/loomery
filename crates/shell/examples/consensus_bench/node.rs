// SPDX-License-Identifier: MPL-2.0
//! One replica process, real tonic listener and persistent production group.
use super::protocol::{
    Batch, Config, NodeSpec, PayloadPattern, Reply, Request, Sample, Status, micros,
};
use loomery_core::{
    Uuid,
    actor::Actor,
    envelope::{Command, Payload},
    id::Id,
    key::Key,
    timestamp::Timestamp,
    workspace::{CREATE, CreateWorkspace},
};
use loomery_shell::{
    group::GroupOps,
    raft::{AppData, Applied, RaftGroup, transport::TonicTransport},
};
use openraft::{BasicNode, Raft};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    task::JoinSet,
};
type Consensus = Raft<loomery_shell::raft::TypeConfig>;

pub fn organization() -> Id {
    Id::from(Key::new(
        &Uuid::NAMESPACE_OID,
        "loomery:consensus-benchmark:organization",
    ))
}
fn command(
    phase: &str,
    sequence: usize,
    bytes: usize,
    pattern: PayloadPattern,
) -> anyhow::Result<Command> {
    let key = Key::new(
        &Uuid::NAMESPACE_OID,
        &format!("loomery:bench:{phase}:{sequence}"),
    );
    let workspace = Id::from(Key::new(&Uuid::NAMESPACE_OID, &format!("workspace:{key}")));
    Ok(Command {
        envelope_version: 1,
        id: Id::from(key.clone()),
        aggregate_id: workspace.clone(),
        organization_id: organization(),
        workspace_id: Some(workspace.clone()),
        occurred_at: Timestamp::from(1_700_000_000_000),
        causation_key: key.clone(),
        correlation_key: key,
        actor: Actor::System,
        command_type: CREATE.into(),
        payload: Payload {
            version: 1,
            data: serde_json::to_string(&CreateWorkspace {
                workspace_id: workspace,
                name: name(sequence, bytes, pattern),
            })?,
        },
    })
}
fn name(sequence: usize, bytes: usize, pattern: PayloadPattern) -> String {
    if matches!(pattern, PayloadPattern::Repeated) {
        return "x".repeat(bytes);
    }
    // Deterministic printable ASCII, intended to exercise compression without entropy I/O.
    let mut state = u64::try_from(sequence)
        .unwrap_or(0)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    (0..bytes)
        .map(|_| {
            state ^= state.wrapping_shl(13);
            state ^= state.wrapping_shr(7);
            state ^= state.wrapping_shl(17);
            char::from(b'!'.saturating_add(u8::try_from(state % 90).unwrap_or(0)))
        })
        .collect()
}
async fn reply(value: anyhow::Result<Reply>) -> anyhow::Result<()> {
    let value: Result<Reply, String> = value.map_err(|e| format!("{e:#}"));
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    let mut output = tokio::io::stdout();
    output.write_all(&bytes).await?;
    output.flush().await?;
    Ok(())
}
pub async fn run(spec: NodeSpec) -> anyhow::Result<()> {
    spec.config.validate()?;
    let started = Instant::now();
    let listener = tokio::net::TcpListener::bind(
        spec.listen
            .unwrap_or_else(|| std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
    )
    .await?;
    let scheme = if spec.config.group.transport.server_tls.is_some() {
        "https"
    } else {
        "http"
    };
    let address = format!("{scheme}://{}", listener.local_addr()?);
    let group = RaftGroup::boot_persistent(
        spec.id,
        "benchmark".into(),
        &spec.path,
        spec.config.group.clone(),
    )
    .await?;
    let transport = TonicTransport::default();
    transport.register("benchmark".into(), group.raft()).await?;
    let server_config = spec.config.group.transport.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        transport
            .serve(listener, server_config, async {
                let _ = stopped.await;
            })
            .await
    });
    reply(Ok(Reply::Ready {
        id: spec.id,
        pid: std::process::id(),
        address,
        boot_us: micros(started.elapsed()),
    }))
    .await?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let request: Request = serde_json::from_str(&line)?;
        let stopping = matches!(request, Request::Stop);
        let result = handle(&group, request, &spec.config).await;
        reply(result).await?;
        if stopping {
            break;
        }
    }
    group.shutdown().await?;
    let _ = stop.send(());
    server.await??;
    Ok(())
}
fn status(raft: &Consensus) -> Status {
    let metrics = raft.metrics();
    let metrics = metrics.borrow();
    Status {
        id: metrics.id,
        term: metrics.current_term,
        leader: metrics.current_leader,
        is_leader: metrics.state == openraft::ServerState::Leader,
        last_log: metrics.last_log_index,
        applied: metrics.last_applied.map(|id| id.index),
        snapshot: metrics.snapshot.map(|id| id.index),
        purged: metrics.purged.map(|id| id.index),
    }
}
async fn handle(group: &RaftGroup, request: Request, config: &Config) -> anyhow::Result<Reply> {
    let raft = group.raft();
    let timeout = Duration::from_millis(config.phase_timeout_ms);
    match request {
        Request::Initialize { members } => {
            let started = Instant::now();
            let id = status(&raft).id;
            let local = members
                .get(&id)
                .ok_or_else(|| anyhow::anyhow!("bootstrap node missing"))?;
            raft.initialize(BTreeMap::from([(id, BasicNode::new(local))]))
                .await?;
            raft.wait(Some(timeout))
                .current_leader(id, "benchmark leader")
                .await?;
            for (peer, address) in &members {
                if *peer != id {
                    raft.add_learner(*peer, BasicNode::new(address), true)
                        .await?;
                }
            }
            raft.change_membership(members.keys().copied().collect::<BTreeSet<_>>(), false)
                .await?;
            Ok(Reply::Initialized {
                elapsed_us: micros(started.elapsed()),
                log_index: status(&raft)
                    .applied
                    .ok_or_else(|| anyhow::anyhow!("membership not applied"))?,
            })
        }
        Request::Batch { phase, count } => {
            Ok(Reply::Batch(batch(raft, &phase, count, config).await?))
        }
        Request::Status => Ok(Reply::Status(status(&raft))),
        Request::Check { index, events } => {
            let started = Instant::now();
            raft.wait(Some(timeout))
                .applied_index_at_least(Some(index), "benchmark barrier")
                .await?;
            let actual = group.committed_events(&organization()).await?.len();
            anyhow::ensure!(
                actual == events,
                "event count mismatch: expected {events}, got {actual}"
            );
            Ok(Reply::Checked {
                elapsed_us: micros(started.elapsed()),
                events: actual,
                status: status(&raft),
            })
        }
        Request::Snapshot { index } => {
            let already_current = status(&raft).snapshot.is_some_and(|n| n >= index);
            let started = Instant::now();
            raft.trigger().snapshot().await?;
            raft.wait(Some(timeout))
                .metrics(
                    |m| m.snapshot.is_some_and(|id| id.index >= index),
                    "snapshot barrier",
                )
                .await?;
            Ok(Reply::Snapshot {
                elapsed_us: micros(started.elapsed()),
                already_current,
            })
        }
        Request::Stop => Ok(Reply::Stopped),
    }
}
async fn sample(
    raft: Consensus,
    command: Command,
    sequence: usize,
    timeout: Duration,
    command_json_bytes: usize,
) -> Sample {
    let started = Instant::now();
    let result = tokio::time::timeout(timeout, raft.client_write(AppData::Command(command))).await;
    let (index, error) = match result {
        Ok(Ok(response)) => match response.data {
            Applied::Appended { first_log_index } => (Some(first_log_index), None),
            other => (None, Some(format!("unexpected outcome: {other:?}"))),
        },
        Ok(Err(error)) => (None, Some(error.to_string())),
        Err(error) => (None, Some(format!("unknown outcome: {error}"))),
    };
    Sample {
        sequence,
        latency_us: micros(started.elapsed()),
        command_json_bytes,
        log_index: index,
        error,
    }
}
async fn batch(
    raft: Consensus,
    phase: &str,
    count: usize,
    config: &Config,
) -> anyhow::Result<Batch> {
    // Prepare outside the measured interval. Keep exactly concurrency requests in flight.
    let mut commands = (0..count)
        .map(|i| {
            command(phase, i, config.name_bytes, config.payload_pattern)
                .and_then(|c| Ok((i, serde_json::to_vec(&c)?.len(), c)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter();
    let timeout = Duration::from_millis(config.operation_timeout_ms);
    let mut pending = JoinSet::new();
    let mut samples = Vec::with_capacity(count);
    let started = Instant::now();
    loop {
        while pending.len() < config.concurrency {
            let Some((sequence, bytes, command)) = commands.next() else {
                break;
            };
            pending.spawn(sample(raft.clone(), command, sequence, timeout, bytes));
        }
        let Some(result) = pending.join_next().await else {
            break;
        };
        samples.push(result?);
    }
    let elapsed_us = micros(started.elapsed());
    samples.sort_by_key(|s| s.sequence);
    Ok(Batch {
        elapsed_us,
        samples,
    })
}
