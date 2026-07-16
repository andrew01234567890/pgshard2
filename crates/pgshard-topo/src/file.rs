//! File-backed topology source: a JSON file re-read on a poll interval.
//! Used by every integration test and by non-Kubernetes development; the
//! Kubernetes watcher (kube-rs, separate module later) feeds the same
//! channel with the same epoch-ordering rule.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::warn;

use crate::model::Topology;
use crate::{TopologyError, TopologyWatcher, validate};

pub struct FileWatcher {
    sender: Arc<watch::Sender<Arc<Topology>>>,
    path: PathBuf,
}

impl FileWatcher {
    /// Loads the initial snapshot (which must be valid) and starts polling
    /// for changes every `interval`.
    pub async fn start(
        path: impl Into<PathBuf>,
        interval: Duration,
    ) -> Result<Self, TopologyError> {
        let path = path.into();
        let initial = load(&path).await?;
        let (sender, _) = watch::channel(Arc::new(initial));
        let sender = Arc::new(sender);

        let poller = Arc::clone(&sender);
        let poll_path = path.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if poller.receiver_count() == 0 && Arc::strong_count(&poller) == 1 {
                    return; // watcher dropped, no consumers
                }
                match load(&poll_path).await {
                    Ok(candidate) => {
                        apply(&poller, candidate);
                    }
                    Err(err) => {
                        warn!(path = %poll_path.display(), error = %err,
                            "topology reload failed; keeping current snapshot");
                    }
                }
            }
        });

        Ok(FileWatcher { sender, path })
    }

    /// Re-reads the file immediately (tests use this instead of waiting for
    /// the poll tick). Returns whether the snapshot was applied.
    pub async fn reload(&self) -> Result<bool, TopologyError> {
        let candidate = load(&self.path).await?;
        Ok(apply(&self.sender, candidate))
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

/// The single application rule: strictly increasing epochs only.
fn apply(sender: &watch::Sender<Arc<Topology>>, candidate: Topology) -> bool {
    let mut applied = false;
    sender.send_if_modified(|current| {
        if candidate.epoch > current.epoch {
            *current = Arc::new(candidate);
            applied = true;
            true
        } else {
            false
        }
    });
    applied
}
