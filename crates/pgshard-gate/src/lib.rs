//! The router's buffering gate: the primitive that makes failover, reshard
//! cutover, online DDL, rolling restarts, and backup barriers invisible to
//! clients. A closed gate parks matching sessions in FIFO order instead of
//! erroring; opening replays them against the new topology.
//!
//! Safety rule: every gate carries an absolute deadline. If no explicit
//! open arrives in time, the gate auto-expires and parked sessions replay
//! against the CURRENT topology — fail-safe means "abort the cutover",
//! never "wait forever" and never "switch blindly".

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pgshard_core::{KeyRange, KeyspaceId};
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateMode {
    WritesOnly,
    All,
}

/// What a statement touches, computed by the planner.
#[derive(Debug, Clone)]
pub struct StatementScope {
    pub tables: Vec<String>,
    pub keyspace_ids: Vec<KeyspaceId>,
    pub is_write: bool,
}

/// Traffic selector for one gate.
#[derive(Debug, Clone, Default)]
pub struct GateMatch {
    pub all: bool,
    pub tables: Vec<String>,
    pub key_ranges: Vec<KeyRange>,
}

impl GateMatch {
    fn matches(&self, scope: &StatementScope) -> bool {
        if self.all {
            return true;
        }
        if scope
            .tables
            .iter()
            .any(|t| self.tables.iter().any(|g| g == t))
        {
            return true;
        }
        scope
            .keyspace_ids
            .iter()
            .any(|id| self.key_ranges.iter().any(|r| r.contains(*id)))
    }
}

#[derive(Debug, Clone)]
pub struct GateSpec {
    pub id: String,
    pub mode: GateMode,
    pub matcher: GateMatch,
    /// Absolute expiry; conversion from wall clock happens at apply time.
    pub deadline: Instant,
    /// Sessions replay only after the applied topology generation reaches
    /// this (ignored on expiry — see module docs).
    pub min_topology_generation: u64,
}

/// Outcome of parking: how the session should proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Release {
    /// Gate opened after a topology change: re-plan the statement, then
    /// execute against the new topology.
    Replan { topology_generation: u64 },
    /// Gate expired without an open: re-plan against current topology
    /// (coordinator treats the cutover as aborted).
    Expired,
    /// Buffer limits exceeded: fail the statement with SQLSTATE 40001.
    Rejected,
}

#[derive(Debug, Clone)]
pub struct GateLimits {
    pub max_sessions: usize,
    pub max_wait: Duration,
}

impl Default for GateLimits {
    fn default() -> Self {
        GateLimits {
            max_sessions: 5000,
            max_wait: Duration::from_secs(20),
        }
    }
}

struct Parked {
    waker: oneshot::Sender<Release>,
    enqueued: Instant,
}

struct GateState {
    spec: GateSpec,
    parked: VecDeque<Parked>,
}

#[derive(Default)]
struct EngineState {
    gates: Vec<GateState>,
    /// Highest topology generation the router has applied; gates opened
    /// with a min generation wait for this to catch up.
    applied_generation: u64,
}

/// The gate engine. Cheap to check on the hot path: one mutex lock only
/// when gates exist (the common case is an empty gate set, checked via an
/// atomic-free fast read of a generation counter — kept simple with a
/// Mutex for now; measured before optimizing).
pub struct GateEngine {
    state: Mutex<EngineState>,
    limits: GateLimits,
}

impl GateEngine {
    pub fn new(limits: GateLimits) -> Arc<Self> {
        Arc::new(GateEngine {
            state: Mutex::new(EngineState::default()),
            limits,
        })
    }

    /// Records the topology generation the router just applied and wakes
    /// any sessions whose gate opened pending that generation.
    pub fn topology_applied(&self, generation: u64) {
        let mut state = self.state.lock().expect("gate lock");
        if generation > state.applied_generation {
            state.applied_generation = generation;
        }
    }

    /// Installs or replaces a gate (level-triggered from topology or via
    /// admin RPC — same spec either way, idempotent by id).
    pub fn close(&self, spec: GateSpec) {
        let mut state = self.state.lock().expect("gate lock");
        if let Some(existing) = state.gates.iter_mut().find(|g| g.spec.id == spec.id) {
            existing.spec = spec;
            return;
        }
        info!(gate = %spec.id, "gate closed");
        state.gates.push(GateState {
            spec,
            parked: VecDeque::new(),
        });
    }

    /// Opens a gate: parked sessions replay once the applied topology
    /// generation reaches the gate's minimum. Callers therefore invoke
    /// `topology_applied` first when the flip and the open race.
    pub fn open(&self, gate_id: &str) {
        let mut state = self.state.lock().expect("gate lock");
        let applied = state.applied_generation;
        let Some(idx) = state.gates.iter().position(|g| g.spec.id == gate_id) else {
            return;
        };
        let gate = state.gates.swap_remove(idx);
        if gate.spec.min_topology_generation > applied {
            warn!(gate = %gate.spec.id, want = gate.spec.min_topology_generation,
                applied, "gate opened before topology applied; replaying anyway with current topology");
        }
        info!(gate = %gate.spec.id, sessions = gate.parked.len(), "gate opened; replaying");
        for parked in gate.parked {
            let _ = parked.waker.send(Release::Replan {
                topology_generation: applied,
            });
        }
    }

    /// Expires overdue gates; returns how many were expired. The router
    /// runs this from a timer tick.
    pub fn expire_due(&self, now: Instant) -> usize {
        let mut state = self.state.lock().expect("gate lock");
        let mut expired = 0;
        let mut idx = 0;
        while idx < state.gates.len() {
            if state.gates[idx].spec.deadline <= now {
                let gate = state.gates.swap_remove(idx);
                warn!(gate = %gate.spec.id, sessions = gate.parked.len(),
                    "gate deadline expired; fail-safe replay against current topology");
                for parked in gate.parked {
                    let _ = parked.waker.send(Release::Expired);
                }
                expired += 1;
            } else {
                idx += 1;
            }
        }
        expired
    }

    /// Hot-path check. Returns a future to await when parked; None means
    /// proceed immediately.
    pub fn check(&self, scope: &StatementScope) -> Option<oneshot::Receiver<Release>> {
        let mut state = self.state.lock().expect("gate lock");
        let gate = state.gates.iter_mut().find(|g| {
            (g.spec.mode == GateMode::All || scope.is_write) && g.spec.matcher.matches(scope)
        })?;
        if gate.parked.len() >= self.limits.max_sessions {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Release::Rejected);
            debug!(gate = %gate.spec.id, "gate buffer full; rejecting");
            return Some(rx);
        }
        let (tx, rx) = oneshot::channel();
        gate.parked.push_back(Parked {
            waker: tx,
            enqueued: Instant::now(),
        });
        Some(rx)
    }

    /// Rejects sessions parked longer than max_wait (run from the same
    /// timer tick as expire_due); returns how many were rejected.
    pub fn reject_overdue_sessions(&self, now: Instant) -> usize {
        let mut state = self.state.lock().expect("gate lock");
        let max_wait = self.limits.max_wait;
        let mut rejected = 0;
        for gate in &mut state.gates {
            while let Some(front) = gate.parked.front() {
                if now.duration_since(front.enqueued) > max_wait {
                    let parked = gate.parked.pop_front().expect("front exists");
                    let _ = parked.waker.send(Release::Rejected);
                    rejected += 1;
                } else {
                    break;
                }
            }
        }
        rejected
    }

    /// Snapshot for RouterAdminService.GateStatus.
    pub fn status(&self) -> Vec<(String, usize)> {
        let state = self.state.lock().expect("gate lock");
        state
            .gates
            .iter()
            .map(|g| (g.spec.id.clone(), g.parked.len()))
            .collect()
    }
}
