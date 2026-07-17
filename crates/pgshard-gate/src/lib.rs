//! The router's buffering gate: the primitive that makes failover, reshard
//! cutover, online DDL, rolling restarts, and backup barriers invisible to
//! clients. A closed gate parks matching sessions in FIFO order instead of
//! erroring; opening replays them against the new topology.
//!
//! Safety rules:
//!   - Every gate carries an ABSOLUTE deadline. If no explicit open arrives in
//!     time, the gate auto-expires and parked sessions replay against the
//!     CURRENT topology — fail-safe means "abort the cutover", never "wait
//!     forever" and never "switch blindly".
//!   - A gate opens only once the router has applied a topology at or beyond
//!     its `min_topology_generation`; opening earlier would replay buffered
//!     writes against the stale (pre-cutover) shard map. `open` before the
//!     generation lands just arms the gate; `topology_applied` releases it.
//!
//! Recheck contract: a session may match more than one gate (e.g. an overlap of
//! a failover gate and a DDL gate). `check` parks a session behind ONE matching
//! gate at a time; a `Release` (Replan or Expired) means "re-plan the statement
//! and check again", not "proceed". Callers therefore loop `check` until it
//! returns `None`, so a session transitively waits for every gate that matches
//! it. `Rejected` is terminal.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
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
    /// re-check gates (it may match another) before executing.
    Replan { topology_generation: u64 },
    /// Gate expired without an open: re-plan against current topology
    /// (coordinator treats the cutover as aborted).
    Expired,
    /// Buffer limits exceeded: fail the statement with SQLSTATE 40001.
    Rejected,
}

#[derive(Debug, Clone)]
pub struct GateLimits {
    /// Sessions parked behind a single gate before new arrivals are rejected.
    pub max_sessions: usize,
    /// Sessions parked across ALL gates before new arrivals are rejected,
    /// bounding total buffer memory independent of how many gates exist.
    pub max_total_sessions: usize,
    pub max_wait: Duration,
}

impl Default for GateLimits {
    fn default() -> Self {
        GateLimits {
            max_sessions: 5000,
            max_total_sessions: 50000,
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
    /// Set once `open` is requested but the applied topology generation has not
    /// yet reached `min_topology_generation`; `topology_applied` releases it.
    open_requested: bool,
}

impl GateState {
    /// Drops parked sessions whose client already disconnected (the receiver
    /// was dropped), so they neither hold memory nor count toward capacity.
    fn prune_dead(&mut self) {
        self.parked.retain(|p| !p.waker.is_closed());
    }
}

#[derive(Default)]
struct EngineState {
    gates: Vec<GateState>,
    /// Highest topology generation the router has applied; gates opened
    /// with a min generation wait for this to catch up.
    applied_generation: u64,
}

impl EngineState {
    fn total_parked(&self) -> usize {
        self.gates.iter().map(|g| g.parked.len()).sum()
    }

    /// Removes `gates[idx]` and releases its parked sessions with `release`.
    fn drain_gate(&mut self, idx: usize, release: Release) {
        let gate = self.gates.swap_remove(idx);
        for parked in gate.parked {
            let _ = parked.waker.send(release);
        }
    }

    /// Releases `gates[idx]` as an opened gate. The absolute deadline is
    /// authoritative: a gate whose deadline has already passed is an aborted
    /// cutover (Expired), even if the generation that would have opened it lands
    /// in the same instant — never a blind Replan after the deadline.
    fn open_gate(&mut self, idx: usize, applied: u64, now: Instant) {
        let release = if self.gates[idx].spec.deadline <= now {
            Release::Expired
        } else {
            Release::Replan {
                topology_generation: applied,
            }
        };
        self.drain_gate(idx, release);
    }
}

/// The gate engine. `check` acquires the mutex on every statement; there is no
/// lock-free fast path yet (measured before optimizing). The common case is an
/// empty gate set, so the critical section is a single vector scan.
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

    /// Locks the state, recovering from a poisoned mutex: a panic elsewhere
    /// must not wedge the gate engine and strand every parked session forever.
    fn lock(&self) -> MutexGuard<'_, EngineState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Records the highest topology generation the router has applied and
    /// releases any gate whose open was requested before its
    /// `min_topology_generation` had landed.
    pub fn topology_applied(&self, generation: u64) {
        let mut state = self.lock();
        if generation > state.applied_generation {
            state.applied_generation = generation;
        }
        let applied = state.applied_generation;
        let now = Instant::now();
        let mut idx = 0;
        while idx < state.gates.len() {
            let gate = &state.gates[idx];
            if gate.open_requested && gate.spec.min_topology_generation <= applied {
                info!(gate = %gate.spec.id, sessions = gate.parked.len(),
                    "gate open now satisfied by applied topology; releasing");
                // Deadline-authoritative: past the deadline this is Expired.
                state.open_gate(idx, applied, now);
            } else {
                idx += 1;
            }
        }
    }

    /// Installs or replaces a gate (level-triggered from topology or via
    /// admin RPC — same spec either way, idempotent by id). Re-closing keeps
    /// parked sessions and preserves any pending open: a level-triggered
    /// re-assertion of the same gate must not silently cancel an in-flight open
    /// (which would strand the parked sessions until the deadline).
    pub fn close(&self, spec: GateSpec) {
        let mut state = self.lock();
        if let Some(existing) = state.gates.iter_mut().find(|g| g.spec.id == spec.id) {
            existing.spec = spec;
            return;
        }
        info!(gate = %spec.id, "gate closed");
        state.gates.push(GateState {
            spec,
            parked: VecDeque::new(),
            open_requested: false,
        });
    }

    /// Opens a gate. If the applied topology generation has not yet reached the
    /// gate's minimum, the open is armed and the gate keeps parking until
    /// `topology_applied` catches up — replaying earlier would route buffered
    /// writes against the pre-cutover shard map. A gate whose deadline has
    /// already passed is expired instead (an aborted cutover, not a switch).
    pub fn open(&self, gate_id: &str) {
        let mut state = self.lock();
        let applied = state.applied_generation;
        let Some(idx) = state.gates.iter().position(|g| g.spec.id == gate_id) else {
            return;
        };
        let spec = &state.gates[idx].spec;
        if spec.deadline <= Instant::now() {
            warn!(gate = %spec.id, "open arrived after the deadline; expiring (aborted cutover)");
            state.drain_gate(idx, Release::Expired);
            return;
        }
        if spec.min_topology_generation > applied {
            warn!(gate = %spec.id, want = spec.min_topology_generation, applied,
                "gate open requested before topology applied; holding until it lands");
            state.gates[idx].open_requested = true;
            return;
        }
        info!(gate = %spec.id, sessions = state.gates[idx].parked.len(), "gate opened; replaying");
        state.drain_gate(
            idx,
            Release::Replan {
                topology_generation: applied,
            },
        );
    }

    /// Expires overdue gates; returns how many were expired. The router
    /// runs this from a timer tick.
    pub fn expire_due(&self, now: Instant) -> usize {
        let mut state = self.lock();
        let mut expired = 0;
        let mut idx = 0;
        while idx < state.gates.len() {
            if state.gates[idx].spec.deadline <= now {
                warn!(gate = %state.gates[idx].spec.id, sessions = state.gates[idx].parked.len(),
                    "gate deadline expired; fail-safe replay against current topology");
                state.drain_gate(idx, Release::Expired);
                expired += 1;
            } else {
                idx += 1;
            }
        }
        expired
    }

    /// Hot-path check. Returns a future to await when parked; None means
    /// proceed immediately. On release, callers re-plan and call `check` again
    /// (see the recheck contract in the module docs).
    pub fn check(&self, scope: &StatementScope) -> Option<oneshot::Receiver<Release>> {
        let mut state = self.lock();
        let idx = state.gates.iter().position(|g| {
            (g.spec.mode == GateMode::All || scope.is_write) && g.spec.matcher.matches(scope)
        })?;
        // Prune disconnected sessions across all gates BEFORE measuring capacity,
        // so dead clients neither hold a per-gate nor an engine-wide slot.
        for gate in &mut state.gates {
            gate.prune_dead();
        }
        let over_total = state.total_parked() >= self.limits.max_total_sessions;
        let gate = &mut state.gates[idx];
        if over_total || gate.parked.len() >= self.limits.max_sessions {
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
        let mut state = self.lock();
        let max_wait = self.limits.max_wait;
        let mut rejected = 0;
        for gate in &mut state.gates {
            gate.prune_dead();
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
        let state = self.lock();
        state
            .gates
            .iter()
            .map(|g| (g.spec.id.clone(), g.parked.len()))
            .collect()
    }
}
