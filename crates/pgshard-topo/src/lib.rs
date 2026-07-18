//! Topology watching for the router and agents.
//!
//! The compiled routing view (PgShardRouting in Kubernetes, a JSON file in
//! tests and non-k8s development) is applied through a single rule:
//! an update is accepted iff its epoch is strictly greater than the last
//! applied epoch. Consumers hold a `tokio::sync::watch` receiver and always
//! see the newest accepted snapshot.

pub mod file;
pub mod model;

use std::sync::Arc;

use tokio::sync::watch;

pub use file::FileWatcher;
pub use model::{
    GateMatch, GateMode, GateSpec, Instance, Sequence, ShardEntry, ShardKeyType, ShardState,
    TableEntry, TableType, Topology,
};

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("invalid topology: {0}")]
    Invalid(String),
}

/// A source of topology snapshots. Implementations push validated,
/// epoch-ordered snapshots into the watch channel.
pub trait TopologyWatcher {
    fn subscribe(&self) -> watch::Receiver<Arc<Topology>>;
}

/// The instant a watcher last successfully read AND validated its source —
/// bumped on every successful poll, including one whose epoch was unchanged
/// (an unchanged-but-readable source still confirms the view is current), and
/// never on an error. Distinct from receiving a new snapshot: this is what
/// lets a consumer bound how stale its view can possibly be — the router's
/// write lease rejects writes once the age exceeds the topology's
/// `write_lease_seconds`, so a router cut off from its source cannot keep
/// writing against a routing world that may have moved on.
#[derive(Clone)]
pub struct Freshness(Arc<std::sync::RwLock<(std::time::Duration, std::time::SystemTime)>>);

/// A suspend-inclusive monotonic reading: Linux `CLOCK_BOOTTIME` keeps counting
/// through system suspend, which `Instant` (CLOCK_MONOTONIC) does not — and a
/// suspend paired with a partial backward wall step can make BOTH ordinary
/// clocks under-report. Elsewhere this falls back to the platform monotonic
/// clock (those platforms' `Instant` generally includes suspend).
pub fn boot_now() -> Option<std::time::Duration> {
    #[cfg(target_os = "linux")]
    {
        // CLOCK_BOOTTIME cannot fail on a running kernel (both arguments are
        // statically valid), but if it somehow did, no caller may substitute a
        // guess: a validation refuses to stamp and an age reads as maximal —
        // both fail closed.
        nix::time::clock_gettime(nix::time::ClockId::CLOCK_BOOTTIME)
            .ok()
            .map(|ts| std::time::Duration::new(ts.tv_sec() as u64, ts.tv_nsec() as u32))
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::sync::OnceLock;
        static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
        Some(EPOCH.get_or_init(std::time::Instant::now).elapsed())
    }
}

impl Freshness {
    /// A stamp of "confirmed just now" — for direct construction (tests, or a
    /// consumer that treats its own construction as a confirmation). A caller
    /// with a real initial validation should use [`Freshness::seeded`]: the
    /// ordering guard in [`Freshness::install`] would silently ignore a stamp
    /// older than construction time.
    pub fn new() -> Self {
        Self(Arc::new(std::sync::RwLock::new((
            boot_now().unwrap_or_default(),
            std::time::SystemTime::now(),
        ))))
    }

    /// A freshness seeded from an existing validation's own stamps —
    /// unconditionally, unlike [`Freshness::install`], because the seed IS the
    /// first confirmation and construction time is irrelevant to it.
    pub fn seeded(v: &SourceValidation) -> Self {
        Self(Arc::new(std::sync::RwLock::new((v.boot, v.wall))))
    }

    /// Install a source-captured confirmation: the stamp is the moment the
    /// source read BEGAN, not the moment this call runs — a delivery delayed
    /// across a suspend must not launder a pre-suspend validation into a fresh
    /// one. A stamp older than the one already installed is ignored (a
    /// concurrent out-of-order publish may deliver stamps backwards; moving
    /// freshness backwards would only shrink availability, but there is no
    /// reason to allow it).
    pub fn install(&self, v: &SourceValidation) {
        let mut guard = self.0.write().unwrap_or_else(|e| e.into_inner());
        if v.boot > guard.0 {
            *guard = (v.boot, v.wall);
        }
    }

    /// How long ago the view was last confirmed. The suspend-inclusive
    /// monotonic clock is authoritative; the wall clock is a second witness
    /// whose larger reading wins (an NTP forward step can only inflate the
    /// age), and a wall clock that stepped backward past the stamp reports
    /// maximal age — a false expiry fails closed and the next renewal clears
    /// it within a poll interval.
    pub fn age(&self) -> std::time::Duration {
        let (boot, wall) = *self.0.read().unwrap_or_else(|e| e.into_inner());
        // Wall first, boot second: a suspend between the two samples then
        // lands inside the boot reading (age looks larger — fail closed); the
        // reverse order could exclude it from both witnesses when paired with
        // a partial backward wall step.
        let wall_elapsed = match std::time::SystemTime::now().duration_since(wall) {
            Ok(d) => d,
            Err(_) => return std::time::Duration::MAX,
        };
        let Some(now) = boot_now() else {
            return std::time::Duration::MAX;
        };
        now.saturating_sub(boot).max(wall_elapsed)
    }

    /// Test hook: pretend the last confirmation happened `by` ago.
    #[doc(hidden)]
    pub fn backdate(&self, by: std::time::Duration) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = (
            boot_now().unwrap_or_default().saturating_sub(by),
            std::time::SystemTime::now() - by,
        );
    }
}

impl Default for Freshness {
    fn default() -> Self {
        Self::new()
    }
}

/// One successful source read+validate: the epoch it observed and the clocks
/// captured AT VALIDATION TIME (both, for the same suspend-awareness as
/// [`Freshness::age`]). Consumers install these stamps rather than stamping
/// delivery time, so a delayed delivery never inflates freshness.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceValidation {
    pub epoch: u64,
    /// Suspend-inclusive monotonic stamp ([`boot_now`]).
    pub boot: std::time::Duration,
    pub wall: std::time::SystemTime,
}

/// The clocks of a [`SourceValidation`], captured BEFORE the source read
/// begins — a suspend between the read and a post-read capture would stamp
/// post-resume time onto a pre-suspend view. Wall is sampled first, so a
/// suspend between the two samples inflates the boot reading (older-looking
/// stamp: fail closed), never the reverse.
#[derive(Debug, Clone, Copy)]
pub struct ValidationClocks {
    boot: std::time::Duration,
    wall: std::time::SystemTime,
}

impl ValidationClocks {
    /// `None` when the boot clock is unavailable: a validation that cannot be
    /// stamped must not be announced — consumers then age out (fail closed).
    pub fn before_read() -> Option<Self> {
        let wall = std::time::SystemTime::now();
        boot_now().map(|boot| Self { boot, wall })
    }

    pub fn stamp(self, epoch: u64) -> SourceValidation {
        SourceValidation {
            epoch,
            boot: self.boot,
            wall: self.wall,
        }
    }
}

/// Validates a candidate snapshot: the epoch and topology generation are the
/// CRD's 1-based counters, the shard function must be known, and serving shards
/// must partition the full keyspace. Serving shards are sorted before the
/// partition check so a snapshot whose shard list is not already start-ordered
/// is not spuriously rejected.
pub fn validate(topology: &Topology) -> Result<(), TopologyError> {
    if topology.epoch == 0 {
        return Err(TopologyError::Invalid("epoch must be >= 1".into()));
    }
    // Mirrors the cluster CRD's bounds: 0 would refuse every write, and an
    // enormous value effectively disables stale-write fencing.
    if !(1..=60).contains(&topology.write_lease_seconds) {
        return Err(TopologyError::Invalid(format!(
            "writeLeaseSeconds {} must be between 1 and 60",
            topology.write_lease_seconds
        )));
    }
    if topology.topology_generation == 0 {
        return Err(TopologyError::Invalid(
            "topologyGeneration must be >= 1".into(),
        ));
    }
    pgshard_core::shard_function(&topology.hash_function)
        .map_err(|e| TopologyError::Invalid(e.to_string()))?;
    let mut serving: Vec<_> = topology
        .shards
        .iter()
        .filter(|s| s.state == ShardState::Serving)
        .map(|s| s.key_range)
        .collect();
    serving.sort_by_key(|kr| kr.start());
    pgshard_core::validate_partition(&serving)
        .map_err(|e| TopologyError::Invalid(format!("serving shards: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod freshness_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn install_never_moves_freshness_backwards() {
        let f = Freshness::new();
        let newer = ValidationClocks::before_read().unwrap().stamp(1);
        f.install(&newer);
        let young = f.age();
        // An older stamp (out-of-order concurrent publish) is ignored.
        let older = SourceValidation {
            epoch: 1,
            boot: newer.boot.saturating_sub(Duration::from_secs(30)),
            wall: newer.wall - Duration::from_secs(30),
        };
        f.install(&older);
        assert!(
            f.age() < young + Duration::from_secs(1),
            "an older stamp must not regress freshness"
        );
    }

    #[test]
    fn backdate_ages_and_a_fresh_install_recovers() {
        let f = Freshness::new();
        f.backdate(Duration::from_secs(120));
        assert!(f.age() >= Duration::from_secs(119));
        f.install(&ValidationClocks::before_read().unwrap().stamp(1));
        assert!(f.age() < Duration::from_secs(2));
    }

    #[test]
    fn boot_clock_is_monotonic_nonzero() {
        let a = boot_now().unwrap();
        let b = boot_now().unwrap();
        assert!(b >= a);
        assert!(a > Duration::ZERO);
    }

    #[test]
    fn seeded_accepts_an_older_stamp_that_install_would_ignore() {
        let old = SourceValidation {
            epoch: 1,
            boot: boot_now().unwrap().saturating_sub(Duration::from_secs(30)),
            wall: std::time::SystemTime::now() - Duration::from_secs(30),
        };
        // install() on a fresh Freshness ignores the older stamp...
        let f = Freshness::new();
        f.install(&old);
        assert!(f.age() < Duration::from_secs(2));
        // ...seeded() takes it as the first confirmation.
        let f = Freshness::seeded(&old);
        assert!(f.age() >= Duration::from_secs(29));
    }
}
