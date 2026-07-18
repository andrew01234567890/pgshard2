//! File-backed topology source: a JSON file re-read on a poll interval.
//! Used by every integration test and by non-Kubernetes development; the
//! Kubernetes watcher (kube-rs, separate module later) feeds the same
//! channel with the same epoch-ordering rule.
//!
//! Writers must publish atomically (write a temp file, then rename over the
//! path) so the poller never parses a half-written file: a partial write is
//! rejected as invalid JSON and the current snapshot is kept, but the epoch
//! rule assumes each observed file is internally consistent.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::warn;

use crate::model::Topology;
use crate::{Freshness, TopologyError, TopologyWatcher, validate};

pub struct FileWatcher {
    sender: Arc<watch::Sender<Arc<Topology>>>,
    path: PathBuf,
    freshness: Freshness,
}

impl FileWatcher {
    /// Loads the initial snapshot (which must be valid) and starts polling
    /// for changes every `interval`.
    pub async fn start(
        path: impl Into<PathBuf>,
        interval: Duration,
    ) -> Result<Self, TopologyError> {
        if interval.is_zero() {
            // tokio::time::interval panics on a zero period; fail loudly at
            // construction rather than spawn a task that dies on first tick and
            // leaves a silently frozen watcher.
            return Err(TopologyError::Invalid(
                "poll interval must be non-zero".into(),
            ));
        }
        let path = path.into();
        let initial = load(&path).await?;
        let (sender, _) = watch::channel(Arc::new(initial));
        let sender = Arc::new(sender);
        // The initial load just validated the source.
        let freshness = Freshness::new();

        let poller = Arc::clone(&sender);
        let poll_freshness = freshness.clone();
        let poll_path = path.clone();
        tokio::spawn(async move {
            // First tick after a full interval, not immediately: the initial
            // snapshot is already loaded, and an immediate tick would race a
            // caller's manual reload().
            let mut ticker =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if poller.receiver_count() == 0 && Arc::strong_count(&poller) == 1 {
                    return; // watcher dropped, no consumers
                }
                match load(&poll_path).await {
                    Ok(candidate) => {
                        // A successful read+validate confirms the view is
                        // current even when the epoch is unchanged; an error
                        // deliberately leaves the freshness clock running so
                        // consumers (the router's write lease) can bound how
                        // stale their view might be.
                        poll_freshness.bump();
                        apply(&poller, candidate);
                    }
                    Err(err) => {
                        warn!(path = %poll_path.display(), error = %err,
                            "topology reload failed; keeping current snapshot");
                    }
                }
            }
        });

        Ok(FileWatcher {
            sender,
            path,
            freshness,
        })
    }

    /// Re-reads the file immediately (tests use this instead of waiting for
    /// the poll tick). Returns whether the snapshot was applied.
    pub async fn reload(&self) -> Result<bool, TopologyError> {
        let candidate = load(&self.path).await?;
        self.freshness.bump();
        Ok(apply(&self.sender, candidate))
    }

    /// The stamp of this watcher's last successful source validation — feed it
    /// to the router so its write lease can bound staleness.
    pub fn freshness(&self) -> Freshness {
        self.freshness.clone()
    }
}

impl TopologyWatcher for FileWatcher {
    fn subscribe(&self) -> watch::Receiver<Arc<Topology>> {
        self.sender.subscribe()
    }
}

async fn load(path: &PathBuf) -> Result<Topology, TopologyError> {
    let bytes = tokio::fs::read(path).await?;
    let topology: Topology = serde_json::from_slice(&bytes)?;
    validate(&topology)?;
    Ok(topology)
}

/// The single application rule: strictly increasing epochs only. Returns
/// whether the candidate was applied (send_if_modified reports the same, so no
/// separate flag is needed).
fn apply(sender: &watch::Sender<Arc<Topology>>, candidate: Topology) -> bool {
    sender.send_if_modified(|current| {
        if candidate.epoch > current.epoch {
            *current = Arc::new(candidate);
            true
        } else {
            false
        }
    })
}
