// SPDX-License-Identifier: MPL-2.0
//! Bounded command batching above the pinned, serialized Raft append path.
use super::{
    AppData, Applied, ProposeError, RaftHandle,
    port::{classify, outcome},
};
use crate::{config::ProposalConfig, group::ProposeOutcome};
use loomery_core::envelope::Command;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::{AbortHandle, JoinHandle},
    time::Instant,
};

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
        let (queue, task) = if config.max_batch_commands > 1 {
            let (tx, rx) = mpsc::channel(config.queue_capacity);
            let raft = raft.clone();
            let task = tokio::spawn(run(raft, rx, config));
            (Some(tx), Some(task))
        } else {
            (None, None)
        };
        let abort = task.as_ref().map(JoinHandle::abort_handle);
        Self(Arc::new(Writer {
            raft,
            queue,
            max_bytes: config.max_batch_bytes,
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

async fn run(raft: RaftHandle, mut queue: mpsc::Receiver<Request>, config: ProposalConfig) {
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
                break;
            }
            bytes = next_bytes;
            requests.push(next);
        }
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
