// SPDX-License-Identifier: MPL-2.0
//! Trial orchestration. Every replica has its own process and Tokio runtime.
use super::{
    protocol::{Batch, Config, NodeSpec, Reply, Request, Status, micros},
    stats::{Summary, summarize},
};
use anyhow::Context;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

pub fn arguments(args: &[String]) -> anyhow::Result<Option<(Config, PathBuf)>> {
    if args.iter().any(|s| s == "--help") {
        println!(
            "consensus_bench [--config FILE.json] [--output NEW_DIRECTORY]\nSeparate replica processes; release builds recommended. See docs/benchmarks.md."
        );
        return Ok(None);
    }
    let mut config = Config::default();
    let mut output = None;
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing value for {flag}"))?;
        match flag.as_str() {
            "--config" => config = Config::from_json(&std::fs::read(value)?)?,
            "--output" => output = Some(PathBuf::from(value)),
            _ => anyhow::bail!("unknown argument: {flag}"),
        }
    }
    config.validate()?;
    let default = || -> anyhow::Result<PathBuf> {
        Ok(PathBuf::from(format!(
            "benchmark-results/{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
        )))
    };
    Ok(Some((
        config,
        match output {
            Some(path) => path,
            None => default()?,
        },
    )))
}
struct Process {
    spec: NodeSpec,
    address: String,
    pid: u32,
    boot_us: u64,
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    alive: bool,
}
impl Process {
    async fn spawn(spec: NodeSpec) -> anyhow::Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .arg("--node")
            .arg(serde_json::to_string(&spec)?)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().context("child stdin unavailable")?;
        let output = BufReader::new(child.stdout.take().context("child stdout unavailable")?);
        let mut process = Self {
            spec,
            address: String::new(),
            pid: 0,
            boot_us: 0,
            child,
            input,
            output,
            alive: true,
        };
        match process.receive().await? {
            Reply::Ready {
                id,
                pid,
                address,
                boot_us,
                ..
            } => {
                anyhow::ensure!(id == process.spec.id, "wrong child identity");
                process.spec.listen = Some(
                    address
                        .split_once("://")
                        .context("invalid node URI")?
                        .1
                        .parse()?,
                );
                process.address = address;
                process.pid = pid;
                process.boot_us = boot_us;
            }
            other => anyhow::bail!("expected ready, got {other:?}"),
        }
        Ok(process)
    }
    async fn receive(&mut self) -> anyhow::Result<Reply> {
        let mut line = String::new();
        let timeout = Duration::from_millis(self.spec.config.phase_timeout_ms);
        let read = tokio::time::timeout(timeout, self.output.read_line(&mut line)).await??;
        anyhow::ensure!(
            read > 0,
            "replica {} closed its control channel",
            self.spec.id
        );
        let response: Result<Reply, String> = serde_json::from_str(&line)?;
        response.map_err(|e| anyhow::anyhow!("replica {}: {e}", self.spec.id))
    }
    async fn request(&mut self, request: &Request) -> anyhow::Result<Reply> {
        let mut bytes = serde_json::to_vec(request)?;
        bytes.push(b'\n');
        self.input.write_all(&bytes).await?;
        self.input.flush().await?;
        self.receive().await
    }
    async fn stop(&mut self) -> anyhow::Result<()> {
        if self.alive {
            self.request(&Request::Stop).await?;
            let exit = tokio::time::timeout(
                Duration::from_millis(self.spec.config.phase_timeout_ms),
                self.child.wait(),
            )
            .await??;
            anyhow::ensure!(exit.success(), "replica shutdown failed: {exit}");
            self.alive = false;
        }
        Ok(())
    }
}
#[derive(Serialize)]
struct Replica {
    id: u64,
    pid: u32,
    boot_us: u64,
    address: String,
}
#[derive(Serialize)]
struct Check {
    id: u64,
    barrier_us: u64,
    events: usize,
    status: Status,
}
#[derive(Serialize)]
struct Measurement {
    summary: Summary,
    batch: Batch,
}
#[derive(Serialize)]
struct Failover {
    crashed_node: u64,
    elected_node: u64,
    election_us: u64,
    probe: Measurement,
}
#[derive(Serialize)]
struct Trial {
    number: usize,
    schema_version: u32,
    replicas: Vec<Replica>,
    initialization_us: u64,
    warmup: Measurement,
    writes: Measurement,
    checks: Vec<Check>,
    snapshot_us: Option<u64>,
    snapshot_already_current: Option<bool>,
    failover: Option<Failover>,
    restart_us: u64,
    recovered: Vec<Check>,
}
async fn batch(
    process: &mut Process,
    phase: &str,
    count: usize,
    duration_ms: Option<u64>,
) -> anyhow::Result<Measurement> {
    match process
        .request(&Request::Batch {
            phase: phase.into(),
            count,
            duration_ms,
        })
        .await?
    {
        Reply::Batch(batch) => Ok(Measurement {
            summary: summarize(&batch),
            batch,
        }),
        other => anyhow::bail!("expected batch, got {other:?}"),
    }
}
/// Prints each replica's apply-phase breakdown, and zeroes its counters.
///
/// Silent — and free — unless `LOOMERY_APPLY_TIMINGS` is set, because collection is
/// off without it. The node resets when it answers, so each call reports exactly the
/// phase that just ended.
async fn report_timings(processes: &mut [Process], phase: &str) -> anyhow::Result<()> {
    if !loomery_shell::raft::timings::enabled() {
        return Ok(());
    }
    for process in processes.iter_mut().filter(|p| p.alive) {
        match process.request(&Request::Timings).await? {
            Reply::Timings { report } => {
                println!("timings {phase} node {}: {report}", process.spec.id);
            }
            other => anyhow::bail!("expected timings, got {other:?}"),
        }
    }
    Ok(())
}

async fn check(processes: &mut [Process], index: u64, events: usize) -> anyhow::Result<Vec<Check>> {
    let mut result = Vec::new();
    for process in processes.iter_mut().filter(|p| p.alive) {
        match process.request(&Request::Check { index, events }).await? {
            Reply::Checked {
                elapsed_us,
                events,
                status,
            } => result.push(Check {
                id: process.spec.id,
                barrier_us: elapsed_us,
                events,
                status,
            }),
            other => anyhow::bail!("expected check, got {other:?}"),
        }
    }
    Ok(result)
}
fn last_index(batch: &Measurement) -> anyhow::Result<u64> {
    batch
        .batch
        .samples
        .iter()
        .filter_map(|s| s.log_index)
        .max()
        .context("no successful writes")
}
async fn elect(processes: &mut [Process], timeout_ms: u64) -> anyhow::Result<u64> {
    let started = Instant::now();
    loop {
        for process in processes.iter_mut().filter(|p| p.alive) {
            if let Reply::Status(status) = process.request(&Request::Status).await?
                && status.is_leader
            {
                return Ok(status.id);
            }
        }
        anyhow::ensure!(
            started.elapsed() < Duration::from_millis(timeout_ms),
            "leader election timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn leader(processes: &mut [Process], id: u64) -> anyhow::Result<&mut Process> {
    processes
        .iter_mut()
        .find(|p| p.spec.id == id && p.alive)
        .context("leader process unavailable")
}
async fn failover(processes: &mut [Process], timeout_ms: u64) -> anyhow::Result<Failover> {
    let id = elect(processes, timeout_ms).await?;
    let started = Instant::now();
    let process = leader(processes, id)?;
    process.child.kill().await?;
    process.alive = false;
    let elected = elect(processes, timeout_ms).await?;
    let election_us = micros(started.elapsed());
    let probe = batch(leader(processes, elected)?, "post-failover", 1, None).await?;
    anyhow::ensure!(probe.summary.failures == 0, "post-failover probe failed");
    Ok(Failover {
        crashed_node: id,
        elected_node: elected,
        election_us,
        probe,
    })
}
async fn restart(
    processes: &mut [Process],
    index: u64,
    events: usize,
) -> anyhow::Result<(u64, Vec<Check>)> {
    for process in processes.iter_mut() {
        if process.spec.config.crash_restart {
            if process.alive {
                process.child.kill().await?;
                process.alive = false;
            }
        } else {
            process.stop().await?;
        }
    }
    let started = Instant::now();
    for process in processes.iter_mut() {
        *process = Process::spawn(process.spec.clone()).await?;
    }
    let timeout = processes
        .first()
        .context("empty cluster")?
        .spec
        .config
        .phase_timeout_ms;
    elect(processes, timeout).await?;
    let checks = check(processes, index, events).await?;
    Ok((micros(started.elapsed()), checks))
}
async fn snapshot(
    process: &mut Process,
    index: u64,
) -> anyhow::Result<(Option<u64>, Option<bool>)> {
    if !process.spec.config.snapshot_before_restart {
        return Ok((None, None));
    }
    match process.request(&Request::Snapshot { index }).await? {
        Reply::Snapshot {
            elapsed_us,
            already_current,
        } => Ok((Some(elapsed_us), Some(already_current))),
        other => anyhow::bail!("expected snapshot, got {other:?}"),
    }
}
/// The measured write phase, and how many events every replica must show for it.
///
/// A time-boxed run's command count is only known once it has stopped, so the count
/// comes back from the run and the caller verifies zero failures before trusting it.
async fn measure_writes(
    processes: &mut [Process],
    leader_id: u64,
    config: &Config,
) -> anyhow::Result<(Measurement, usize)> {
    let writes = batch(
        leader(processes, leader_id)?,
        "measured",
        config.operations,
        config.duration_ms,
    )
    .await?;
    let commands = match config.duration_ms {
        Some(_) => writes.summary.successes,
        None => config.operations,
    };
    let events = config
        .warmup
        .checked_add(commands)
        .context("event count overflow")?;
    Ok((writes, events))
}
async fn trial(config: &Config, root: &Path, number: usize) -> anyhow::Result<Trial> {
    let mut processes = Vec::new();
    for id in 1..=config.nodes {
        processes.push(
            Process::spawn(NodeSpec {
                id,
                path: root.join(id.to_string()),
                listen: None,
                config: config.clone(),
            })
            .await?,
        );
    }
    let replicas = processes
        .iter()
        .map(|p| Replica {
            id: p.spec.id,
            pid: p.pid,
            boot_us: p.boot_us,
            address: p.address.clone(),
        })
        .collect();
    let members = processes
        .iter()
        .map(|p| (p.spec.id, p.address.clone()))
        .collect::<BTreeMap<_, _>>();
    let (initialization_us, membership_index) = match leader(&mut processes, 1)?
        .request(&Request::Initialize { members })
        .await?
    {
        Reply::Initialized {
            elapsed_us,
            log_index,
        } => (elapsed_us, log_index),
        other => anyhow::bail!("expected initialization, got {other:?}"),
    };
    check(&mut processes, membership_index, 0).await?;
    let leader_id = elect(&mut processes, config.phase_timeout_ms).await?;
    let warmup = batch(
        leader(&mut processes, leader_id)?,
        "warmup",
        config.warmup,
        None,
    )
    .await?;
    save(&root.join("warmup-samples.json"), &warmup).await?;
    anyhow::ensure!(warmup.summary.failures == 0, "warmup failed");
    if config.warmup > 0 {
        check(&mut processes, last_index(&warmup)?, config.warmup).await?;
    }
    // Drop the warmup's numbers so the measured phase reports only itself.
    report_timings(&mut processes, "warmup").await?;
    let (writes, mut events) = measure_writes(&mut processes, leader_id, config).await?;
    report_timings(&mut processes, "measured").await?;
    save(&root.join("write-samples.json"), &writes).await?;
    anyhow::ensure!(
        writes.summary.failures == 0,
        "measured writes failed; raw samples retained"
    );
    let mut index = last_index(&writes)?;
    let checks = check(&mut processes, index, events).await?;
    let (snapshot_us, snapshot_already_current) =
        snapshot(leader(&mut processes, leader_id)?, index).await?;
    let failover = if config.failover && config.nodes > 1 {
        let failover = failover(&mut processes, config.phase_timeout_ms).await?;
        index = last_index(&failover.probe)?;
        events = events.checked_add(1).context("event count overflow")?;
        check(&mut processes, index, events).await?;
        Some(failover)
    } else {
        None
    };
    let (restart_us, recovered) = restart(&mut processes, index, events).await?;
    for process in &mut processes {
        process.stop().await?;
    }
    Ok(Trial {
        number,
        schema_version: 3,
        replicas,
        initialization_us,
        warmup,
        writes,
        checks,
        snapshot_us,
        snapshot_already_current,
        failover,
        restart_us,
        recovered,
    })
}
#[derive(Serialize)]
struct Environment {
    timestamp_ms: u128,
    build_profile: &'static str,
    host: String,
    cpu: String,
    memory: String,
    filesystem: String,
    available_cpus: usize,
    rustc: String,
    git_commit: String,
    git_changes: String,
}
async fn output(program: &str, args: &[&str], directory: &Path) -> String {
    match Command::new(program)
        .args(args)
        .current_dir(directory)
        .output()
        .await
    {
        Ok(result) if result.status.success() => {
            String::from_utf8_lossy(&result.stdout).trim().to_owned()
        }
        _ => "unavailable".into(),
    }
}
async fn environment(root: &Path) -> anyhow::Result<Environment> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let cpu = tokio::fs::read_to_string("/proc/cpuinfo")
        .await
        .unwrap_or_default()
        .lines()
        .find(|s| s.starts_with("model name"))
        .unwrap_or("unavailable")
        .to_owned();
    let memory = tokio::fs::read_to_string("/proc/meminfo")
        .await
        .unwrap_or_default()
        .lines()
        .find(|s| s.starts_with("MemTotal"))
        .unwrap_or("unavailable")
        .to_owned();
    Ok(Environment {
        timestamp_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        host: output("uname", &["-a"], &repo).await,
        cpu,
        memory,
        filesystem: output("df", &["-T", &root.to_string_lossy()], &repo).await,
        available_cpus: std::thread::available_parallelism()?.get(),
        rustc: output("rustc", &["--version"], &repo).await,
        git_commit: output("git", &["rev-parse", "HEAD"], &repo).await,
        git_changes: output("git", &["status", "--porcelain"], &repo).await,
    })
}
async fn save(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    file.write_all(&serde_json::to_vec_pretty(value)?).await?;
    file.sync_all().await?;
    Ok(())
}
pub async fn run(config: Config, root: PathBuf) -> anyhow::Result<()> {
    if let Some(parent) = root.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::create_dir(&root)
        .await
        .context("output directory must be new")?;
    let root = tokio::fs::canonicalize(root).await?;
    save(&root.join("config.json"), &config).await?;
    save(&root.join("environment.json"), &environment(&root).await?).await?;
    tokio::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
        root.join("Cargo.lock"),
    )
    .await?;
    for number in 1..=config.trials {
        let path = root.join(format!("trial-{number:03}"));
        tokio::fs::create_dir(&path).await?;
        match trial(&config, &path, number).await {
            Ok(result) => {
                eprintln!(
                    "trial {number}: {} writes/s, p50 {:?} us, p99 {:?} us, {} failures",
                    result.writes.summary.throughput_per_second,
                    result.writes.summary.p50_us,
                    result.writes.summary.p99_us,
                    result.writes.summary.failures
                );
                save(&path.join("report.json"), &result).await?;
            }
            Err(error) => {
                save(&path.join("error.json"), &format!("{error:#}")).await?;
                return Err(error);
            }
        }
    }
    eprintln!("results: {}", root.display());
    Ok(())
}
