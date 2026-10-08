// SPDX-License-Identifier: MPL-2.0
//! Bounded command batching above the pinned, serialized Raft append path.
use super::{
    AppData, Applied, ProposeError, RaftHandle,
    port::{classify, outcome},
};
use crate::{config::ProposalConfig, group::ProposeOutcome};
use loomery_core::envelope::Command;
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::{AbortHandle, JoinHandle},
    time::Instant,
};

/// What ended a batch: the limit that bound it, or the input running out.
///
/// `Supply` is the interesting one. A batch limit can only be reached if at least
/// that many commands are in flight, so a writer configured for 256 commands per
/// entry behind eight concurrent callers reports `Supply` every time — the limit
/// is dead configuration, and the counters are what make that visible instead of
/// letting a reader assume the configured number is the effective one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum BatchBound {
    /// The batch reached `max_batch_commands`.
    Count,
    /// One more command would have passed `max_batch_bytes`.
    Bytes,
    /// The queue ran dry: the collection delay expired, or nothing was queued.
    Supply,
}

/// A snapshot of one writer's batching, so a limit that never binds is visible.
///
/// Counters, deliberately: the effective batch is `commands / batches`, and which
/// limit bound it is a histogram. Read it as "what happened", never as "what was
/// configured" — `configured_commands` is here only so the two can be compared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct BatchStats {
    /// Whether batching is on at all; with it off, every command is its own entry.
    pub enabled: bool,
    /// `max_batch_commands`.
    pub configured_commands: usize,
    /// `max_batch_bytes`.
    pub configured_bytes: usize,
    /// Entries assembled.
    pub batches: u64,
    /// Commands across those entries.
    pub commands: u64,
    /// The largest batch observed.
    pub largest: usize,
    /// The largest single encoded command seen.
    pub largest_frame_bytes: usize,
    /// Batches ended by each limit.
    pub count_bound: u64,
    /// Batches ended by the byte budget.
    pub bytes_bound: u64,
    /// Batches that ended because no more commands arrived.
    pub supply_bound: u64,
}

impl BatchStats {
    /// The mean commands per entry, truncated, or `None` before any batch.
    #[must_use]
    pub fn mean_depth(&self) -> Option<u64> {
        self.commands.checked_div(self.batches)
    }

    /// How many of the largest command seen the byte budget can hold, or `None`
    /// before any command has been seen.
    ///
    /// This is the number that decides whether the byte budget binds before the
    /// count limit: if it is below `configured_commands`, the count limit is
    /// unreachable however many writers there are.
    #[must_use]
    pub fn byte_capacity(&self) -> Option<usize> {
        let frame = self.largest_frame_bytes;
        if frame == 0 {
            None
        } else {
            self.configured_bytes.checked_div(frame)
        }
    }
}

impl fmt::Display for BatchStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.enabled {
            return f.write_str("batching disabled: one command per entry");
        }
        write!(
            f,
            "{} commands in {} entries (mean {}, largest {}); bound by count {} / bytes {} / supply {}; \
             limits {} commands / {} bytes",
            self.commands,
            self.batches,
            self.mean_depth().unwrap_or(0),
            self.largest,
            self.count_bound,
            self.bytes_bound,
            self.supply_bound,
            self.configured_commands,
            self.configured_bytes,
        )?;
        match self.byte_capacity() {
            Some(capacity) => write!(
                f,
                ", and the byte budget holds {capacity} of the {}-byte largest command",
                self.largest_frame_bytes
            ),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Default)]
struct BatchCounters {
    batches: AtomicU64,
    commands: AtomicU64,
    largest: AtomicUsize,
    largest_frame_bytes: AtomicUsize,
    count_bound: AtomicU64,
    bytes_bound: AtomicU64,
    supply_bound: AtomicU64,
}

impl BatchCounters {
    /// Records one assembled batch. Observability only, so relaxed ordering: the
    /// numbers are a snapshot, not a synchronisation point.
    fn record(&self, depth: usize, frame_bytes: usize, bound: BatchBound) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.commands
            .fetch_add(u64::try_from(depth).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.largest.fetch_max(depth, Ordering::Relaxed);
        self.largest_frame_bytes
            .fetch_max(frame_bytes, Ordering::Relaxed);
        match bound {
            BatchBound::Count => &self.count_bound,
            BatchBound::Bytes => &self.bytes_bound,
            BatchBound::Supply => &self.supply_bound,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, configured_commands: usize, configured_bytes: usize) -> BatchStats {
        BatchStats {
            enabled: true,
            configured_commands,
            configured_bytes,
            batches: self.batches.load(Ordering::Relaxed),
            commands: self.commands.load(Ordering::Relaxed),
            largest: self.largest.load(Ordering::Relaxed),
            largest_frame_bytes: self.largest_frame_bytes.load(Ordering::Relaxed),
            count_bound: self.count_bound.load(Ordering::Relaxed),
            bytes_bound: self.bytes_bound.load(Ordering::Relaxed),
            supply_bound: self.supply_bound.load(Ordering::Relaxed),
        }
    }
}

type Reply = oneshot::Sender<anyhow::Result<ProposeOutcome>>;
struct Request {
    command: Command,
    bytes: usize,
    reply: Reply,
}
struct Writer {
    raft: RaftHandle,
    queue: Option<mpsc::Sender<Request>>,
    max_bytes: usize,
    max_commands: usize,
    counters: Arc<BatchCounters>,
    stopped: AtomicBool,
    task: Mutex<Option<JoinHandle<()>>>,
    abort: Option<AbortHandle>,
}
impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(abort) = &self.abort {
            abort.abort();
        }
    }
}

/// Cloneable writer shared by every producer of one group. A successful reply
/// means quorum commit and application, including checkpoint durability when
/// configured. Commands in one batch share a Raft log index and apply in order.
#[derive(Clone)]
pub struct ProposalWriter(Arc<Writer>);
impl ProposalWriter {
    pub(super) fn new(raft: RaftHandle, config: ProposalConfig) -> Self {
        let counters = Arc::new(BatchCounters::default());
        let batching = config.max_batch_commands > 1;
        let (queue, task) = if batching {
            let (tx, rx) = mpsc::channel(config.queue_capacity);
            let raft = raft.clone();
            let counters = Arc::clone(&counters);
            let task = tokio::spawn(run(raft, rx, config, counters));
            (Some(tx), Some(task))
        } else {
            (None, None)
        };
        let abort = task.as_ref().map(JoinHandle::abort_handle);
        Self(Arc::new(Writer {
            raft,
            queue,
            max_bytes: config.max_batch_bytes,
            max_commands: config.max_batch_commands,
            counters,
            stopped: AtomicBool::new(false),
            task: Mutex::new(task),
            abort,
        }))
    }

    /// Propose a command through the shared bounded queue, or directly when
    /// batching is disabled. Queue waiting is part of the operation's latency.
    /// Dropping this future after enqueue does not cancel the command.
    /// # Errors
    /// Returns consensus/application errors, a local size rejection, or an
    /// unknown outcome on shutdown. Re-read applied state before retrying an
    /// unknown outcome using the same causation key.
    pub async fn propose(&self, command: Command) -> anyhow::Result<ProposeOutcome> {
        if self.0.stopped.load(Ordering::Acquire) {
            return Err(unknown("proposal writer stopped"));
        }
        let Some(queue) = &self.0.queue else {
            let response = self
                .0
                .raft
                .client_write(AppData::Command(command))
                .await
                .map_err(classify)?;
            return outcome(response.data);
        };
        let bytes = serde_json::to_vec(&command)?.len();
        anyhow::ensure!(
            bytes <= self.0.max_bytes,
            "command exceeds proposal batch byte limit"
        );
        let (reply, received) = oneshot::channel();
        queue
            .send(Request {
                command,
                bytes,
                reply,
            })
            .await
            .map_err(|_| unknown("proposal writer stopped"))?;
        received
            .await
            .map_err(|_| unknown("proposal interrupted; outcome unknown"))?
    }

    /// What this writer's batching has actually done, not what it was configured
    /// to do.
    ///
    /// This is the number to look at when a batch limit seems not to be working:
    /// a writer configured for 256 commands per entry behind eight concurrent
    /// callers reports `supply_bound` on every batch and `largest` 8, which says
    /// the limit is dead configuration rather than making the reader guess.
    #[must_use]
    pub fn batch_stats(&self) -> BatchStats {
        let mut stats = self
            .0
            .counters
            .snapshot(self.0.max_commands, self.0.max_bytes);
        stats.enabled = self.0.queue.is_some();
        stats
    }

    pub(super) async fn stop(&self) {
        self.0.stopped.store(true, Ordering::Release);
        if let Some(abort) = &self.0.abort {
            abort.abort();
        }
        let mut task = self.0.task.lock().await;
        if let Some(task) = task.as_mut() {
            let _ = task.await;
        }
        task.take();
    }
}

fn unknown(message: &str) -> anyhow::Error {
    anyhow::Error::new(ProposeError::Unknown(anyhow::anyhow!(message.to_owned())))
}

async fn run(
    raft: RaftHandle,
    mut queue: mpsc::Receiver<Request>,
    config: ProposalConfig,
    counters: Arc<BatchCounters>,
) {
    let mut pending = None;
    loop {
        let first = if let Some(request) = pending.take() {
            request
        } else {
            let Some(request) = queue.recv().await else {
                break;
            };
            request
        };
        let mut bytes = first.bytes;
        let mut requests = vec![first];
        // Which limit ends this batch; `Supply` unless one of the other two fires.
        let mut bound = BatchBound::Supply;
        let deadline = Instant::now().checked_add(Duration::from_millis(config.max_delay_ms));
        while requests.len() < config.max_batch_commands {
            let next = match queue.try_recv() {
                Ok(request) => Some(request),
                Err(mpsc::error::TryRecvError::Disconnected) => None,
                Err(mpsc::error::TryRecvError::Empty) => {
                    if config.max_delay_ms == 0 {
                        None
                    } else if let Some(deadline) = deadline {
                        tokio::time::timeout_at(deadline, queue.recv())
                            .await
                            .ok()
                            .flatten()
                    } else {
                        None
                    }
                }
            };
            let Some(next) = next else {
                break;
            };
            let next_bytes = bytes.saturating_add(next.bytes);
            if next_bytes > config.max_batch_bytes {
                pending = Some(next);
                bound = BatchBound::Bytes;
                break;
            }
            bytes = next_bytes;
            requests.push(next);
        }
        if requests.len() == config.max_batch_commands {
            // Reached here only by exhausting the count limit: a byte-limited batch
            // breaks out of the loop below it.
            bound = BatchBound::Count;
        }
        let largest_frame = requests
            .iter()
            .map(|request| request.bytes)
            .max()
            .unwrap_or(0);
        counters.record(requests.len(), largest_frame, bound);
        submit(&raft, requests).await;
    }
}

async fn submit(raft: &RaftHandle, requests: Vec<Request>) {
    let (commands, replies): (Vec<_>, Vec<_>) =
        requests.into_iter().map(|r| (r.command, r.reply)).unzip();
    let batched = commands.len() > 1;
    let data = if batched {
        AppData::Batch(commands)
    } else {
        let Some(command) = commands.into_iter().next() else {
            return;
        };
        AppData::Command(command)
    };
    match raft.client_write(data).await {
        Err(error) => {
            for reply in replies {
                let _ = reply.send(Err(classify(error.clone())));
            }
        }
        Ok(response) => {
            let results = match (batched, response.data) {
                (true, Applied::Batch(results)) if results.len() == replies.len() => results,
                (false, result) => vec![result],
                _ => {
                    for reply in replies {
                        let _ = reply.send(Err(unknown("invalid batch response; outcome unknown")));
                    }
                    return;
                }
            };
            for (reply, result) in replies.into_iter().zip(results) {
                let _ = reply.send(outcome(result));
            }
        }
    }
}
