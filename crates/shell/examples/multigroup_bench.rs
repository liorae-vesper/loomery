// SPDX-License-Identifier: MPL-2.0

//! Co-resident multi-group mixed read/write capacity probe.
//!
//! One process hosts `GROUPS` independent single-node Raft groups (the way a
//! deployment hosts one group per tenant). Each group gets `WRITERS` proposers
//! and `READERS` readers; the probe runs for `DURATION_SECS` and prints write
//! and read throughput per group and in total.
//!
//! ```sh
//! GROUP_COUNT=4 WRITERS=2 READERS=2 DURATION_SECS=3 \
//!   cargo run --release --example multigroup_bench
//! ```
//!
//! Results, methodology and limits are recorded in
//! `docs/benchmarks/multigroup.md`. This is an **in-process, in-memory** probe:
//! it measures the shell's group/port overhead, not disk or network capacity.
#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use loomery_core::Uuid;
use loomery_core::actor::Actor;
use loomery_core::envelope::Command;
use loomery_core::envelope::Payload;
use loomery_core::id::Id;
use loomery_core::key::Key;
use loomery_core::timestamp::Timestamp;
use loomery_shell::group::GroupOps;
use loomery_shell::raft::RaftGroup;

/// The namespace of the probe's causation keys.
const NS: Uuid = Uuid::from_u128(0x018f_2c3d_4e5f_6071_8293_a4b5_c6d7_e8f9);

/// The organization every probe group is faked against.
const ORGANIZATION: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9";

/// Reads an integer environment variable.
fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A distinct `task.create` command for `(worker, counter)`.
fn task_command(worker: u64, counter: u64) -> Command {
    let id = format!("{worker}-{counter}");
    Command {
        envelope_version: 1,
        id: Id::new(),
        aggregate_id: Id::from(format!("task-{id}")),
        organization_id: Id::from(ORGANIZATION),
        workspace_id: Some(Id::from("ws-1")),
        occurred_at: Timestamp::from(1_700_000_000_000),
        causation_key: Key::new(&NS, &id),
        correlation_key: Key::new(&NS, "multigroup-bench"),
        actor: Actor::System,
        command_type: "task.create".to_owned(),
        payload: Payload {
            version: 1,
            data: format!(r#"{{"title":"task {id}"}}"#),
        },
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let groups = env("GROUP_COUNT", 4);
    let writers = env("WRITERS", 2);
    let readers = env("READERS", 2);
    let seconds = env("DURATION_SECS", 3) as u64;

    let write_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();

    for group_index in 0..groups {
        let group = RaftGroup::boot_single_node((group_index as u64).saturating_add(1)).await?;

        for worker in 0..writers {
            let mut group = group.clone();
            let write_count = Arc::clone(&write_count);
            let base = (group_index as u64)
                .saturating_mul(1_000)
                .saturating_add(worker as u64);
            tasks.push(tokio::spawn(async move {
                let mut counter: u64 = 0;
                loop {
                    if group.propose(task_command(base, counter)).await.is_ok() {
                        write_count.fetch_add(1, Ordering::Relaxed);
                    }
                    counter = counter.saturating_add(1);
                }
            }));
        }

        for _ in 0..readers {
            let group = group.clone();
            let read_count = Arc::clone(&read_count);
            tasks.push(tokio::spawn(async move {
                loop {
                    let _ = group.committed_events(&Id::from(ORGANIZATION)).await;
                    read_count.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
    }

    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    for task in &tasks {
        task.abort();
    }

    let elapsed = started.elapsed().as_secs_f64();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_reads = read_count.load(Ordering::Relaxed);

    println!("groups={groups} writers={writers} readers={readers} duration={elapsed:.2}s");
    println!(
        "writes={total_writes} total ({:.1}/s across all groups, {:.1}/s per group)",
        total_writes as f64 / elapsed,
        total_writes as f64 / elapsed / groups as f64,
    );
    println!(
        "reads={total_reads} total ({:.1}/s across all groups, {:.1}/s per group)",
        total_reads as f64 / elapsed,
        total_reads as f64 / elapsed / groups as f64,
    );

    Ok(())
}
