//! Global sequence block cache for the router.
//!
//! Sharded auto-increment columns can't use per-shard Postgres sequences —
//! the values would collide across shards. Instead each sequence lives once
//! in the unsharded system database, and routers hand out ids from blocks
//! they reserve there in bulk (Vitess-style). A block reservation is a
//! single `UPDATE pgshard.sequences SET next_id = next_id + cache_size ...
//! RETURNING` that grants a half-open range `[start, start+size)`; the
//! router then hands ids out of that range without touching the database
//! until it drains. Ids are monotonic-ish (gaps on router restart or
//! over-reservation are expected and documented) and never duplicated across
//! routers.
//!
//! This crate is the in-memory allocator over reserved blocks plus the
//! trait for the reservation backend; the concrete Postgres backend lives
//! in the router where the connection pool is. Concurrent callers that drain
//! a block are single-flighted: exactly one performs the reservation while the
//! others wait for it, so a burst never fans out into N database round-trips.
//! Reserving synchronously on full drain still blocks that one caller on the
//! database; a router avoids even that by watching `needs_refill` and
//! reserving the next block in the background before the current one drains.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex, MutexGuard};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SeqError {
    #[error("sequence {0:?} is not registered")]
    UnknownSequence(String),
    #[error("reservation backend error: {0}")]
    Backend(String),
    #[error("sequence {0:?} exhausted the i64 space")]
    Exhausted(String),
}

/// A reserved, still-usable id range `[next, end)` for one sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Block {
    next: i64,
    end: i64,
}

impl Block {
    /// Builds a block over `[start, start + size)`. Returns `None` if the range
    /// would overflow `i64` (a malformed reservation near the top of the space),
    /// so a bad backend result is rejected rather than silently truncated.
    pub fn new(start: i64, size: i64) -> Option<Block> {
        let end = start.checked_add(size)?;
        Some(Block { next: start, end })
    }

    /// An already-drained block, used as a placeholder while a first
    /// reservation is in flight.
    const fn empty() -> Block {
        Block { next: 0, end: 0 }
    }

    fn take(&mut self) -> Option<i64> {
        if self.next < self.end {
            let id = self.next;
            self.next += 1;
            Some(id)
        } else {
            None
        }
    }

    /// Remaining ids in the block.
    pub fn remaining(&self) -> i64 {
        (self.end - self.next).max(0)
    }
}

/// Reserves the next block for a sequence from the authoritative store.
/// Implementations run the `UPDATE ... RETURNING` against the system DB.
///
/// Correctness contract — the store is the ONLY thing that enforces global
/// uniqueness, so an implementation MUST return, for each `sequence`, ranges
/// that are:
///   - positive (`size > 0`) and representable (`start + size` fits in i64),
///   - globally DISJOINT across every router and every call — two reservations
///     that overlap immediately duplicate ids, the one thing sequences must
///     never do. The atomic `UPDATE ... SET next_id = next_id + size RETURNING`
///     against the single system row is what guarantees this.
pub trait BlockReserver: Send + Sync {
    /// Returns the reserved range's start and size for `sequence`.
    fn reserve(&self, sequence: &str) -> Result<(i64, i64), SeqError>;
}

struct SeqState {
    block: Block,
    /// Once the current block drops to this many ids, `needs_refill` reports
    /// true so a router can reserve the next block in the background before the
    /// current one drains (this crate itself only reserves on full drain).
    refill_at: i64,
    /// True while one caller holds the single-flight reservation claim for this
    /// sequence; others wait on the condvar rather than reserving in parallel.
    reserving: bool,
}

/// Hands out ids for many sequences, reserving fresh blocks as they drain.
/// Cheap and lock-guarded; the steady-state hot path is a single mutex + integer
/// bump. Sequence names are expected to come from the bounded, registered
/// sequence catalog (the vschema), so the per-name map does not grow unbounded.
pub struct SequenceCache<R: BlockReserver> {
    reserver: R,
    state: Mutex<HashMap<String, SeqState>>,
    /// Notified whenever a reservation completes (success or failure), so
    /// callers waiting on a single-flighted reservation re-check their block.
    reserved: Condvar,
}

/// Holds the single-flight reservation claim for one sequence and releases it
/// on drop — INCLUDING when `BlockReserver::reserve` panics and unwinds. Without
/// this, a panicking backend would leave `reserving = true` set with no notify,
/// and every future caller of that sequence would block on the condvar forever.
struct ReservationClaim<'a, R: BlockReserver> {
    cache: &'a SequenceCache<R>,
    sequence: &'a str,
    armed: bool,
}

impl<R: BlockReserver> ReservationClaim<'_, R> {
    /// The happy/error path already cleared the claim under the lock; make the
    /// drop a no-op so it does not re-lock and re-notify.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<R: BlockReserver> Drop for ReservationClaim<'_, R> {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.cache.lock();
            if let Some(entry) = state.get_mut(self.sequence) {
                entry.reserving = false;
            }
            self.cache.reserved.notify_all();
        }
    }
}

impl<R: BlockReserver> SequenceCache<R> {
    pub fn new(reserver: R) -> SequenceCache<R> {
        SequenceCache {
            reserver,
            state: Mutex::new(HashMap::new()),
            reserved: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, SeqState>> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Returns the next id for `sequence`, reserving a new block if the current
    /// one is exhausted (or was never reserved). Concurrent callers on a drained
    /// sequence single-flight the reservation: one reserves, the rest wait.
    ///
    /// On a reservation FAILURE the error is not fanned out to the waiters; each
    /// retries in turn, so a persistently failing backend serializes retries.
    /// That is fine when `reserve` fails fast (a down system DB refuses quickly);
    /// sharing one fast-fail across waiters is a deferred optimization.
    pub fn next_id(&self, sequence: &str) -> Result<i64, SeqError> {
        let mut state = self.lock();
        loop {
            match state.get_mut(sequence) {
                Some(entry) => {
                    if let Some(id) = entry.block.take() {
                        return Ok(id);
                    }
                    if entry.reserving {
                        // Another caller is reserving the next block; wait for it
                        // rather than issue a second reservation, then re-check.
                        state = self.reserved.wait(state).unwrap_or_else(|p| p.into_inner());
                        continue;
                    }
                    entry.reserving = true;
                }
                None => {
                    // First use: claim the reservation with a drained placeholder
                    // so concurrent first-callers single-flight too.
                    state.insert(
                        sequence.to_string(),
                        SeqState {
                            block: Block::empty(),
                            refill_at: 0,
                            reserving: true,
                        },
                    );
                }
            }

            // We hold the single-flight claim. Reserve without the lock, under an
            // RAII guard so a panic out of reserve() still clears the claim and
            // wakes waiters (otherwise the sequence would deadlock forever).
            drop(state);
            let mut claim = ReservationClaim {
                cache: self,
                sequence,
                armed: true,
            };
            let result = self.reserver.reserve(sequence);
            state = self.lock();
            let entry = state.get_mut(sequence).expect("reservation claim present");
            entry.reserving = false;
            self.reserved.notify_all();
            claim.disarm();

            let (start, size) = result?;
            if size <= 0 {
                return Err(SeqError::Backend(format!(
                    "sequence {sequence:?} reserved a non-positive block size {size}"
                )));
            }
            let mut block =
                Block::new(start, size).ok_or_else(|| SeqError::Exhausted(sequence.to_string()))?;
            let id = block
                .take()
                .ok_or_else(|| SeqError::Exhausted(sequence.to_string()))?;
            entry.block = block;
            entry.refill_at = (size / 5).max(1);
            return Ok(id);
        }
    }

    /// Whether the sequence's cached block has drained past its refill
    /// threshold (a router uses this to trigger a background reservation).
    pub fn needs_refill(&self, sequence: &str) -> bool {
        let state = self.lock();
        state
            .get(sequence)
            .is_some_and(|s| !s.reserving && s.block.remaining() <= s.refill_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    /// A reserver that grants consecutive blocks of a fixed size, mimicking
    /// `UPDATE ... SET next_id = next_id + size RETURNING`.
    struct FakeReserver {
        size: i64,
        next_start: AtomicI64,
        reservations: AtomicI64,
    }

    impl FakeReserver {
        fn new(size: i64) -> FakeReserver {
            FakeReserver {
                size,
                next_start: AtomicI64::new(1),
                reservations: AtomicI64::new(0),
            }
        }
    }

    impl BlockReserver for FakeReserver {
        fn reserve(&self, _sequence: &str) -> Result<(i64, i64), SeqError> {
            self.reservations.fetch_add(1, Ordering::SeqCst);
            let start = self.next_start.fetch_add(self.size, Ordering::SeqCst);
            Ok((start, self.size))
        }
    }

    #[test]
    fn hands_out_contiguous_ids_reserving_once_per_block() {
        let cache = SequenceCache::new(FakeReserver::new(10));
        let ids: Vec<i64> = (0..25)
            .map(|_| cache.next_id("orders_id").unwrap())
            .collect();
        assert_eq!(ids, (1..=25).collect::<Vec<_>>());
        // 25 ids from blocks of 10 => 3 reservations ([1,11),[11,21),[21,31)).
        assert_eq!(cache.reserver.reservations.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn drained_block_reserves_on_next_request() {
        let cache = SequenceCache::new(FakeReserver::new(4));
        for _ in 0..4 {
            cache.next_id("s").unwrap();
        }
        assert!(cache.needs_refill("s"));
        assert_eq!(cache.next_id("s").unwrap(), 5);
        assert_eq!(cache.reserver.reservations.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn independent_sequences_do_not_interfere() {
        let cache = SequenceCache::new(FakeReserver::new(100));
        let a1 = cache.next_id("a").unwrap();
        let b1 = cache.next_id("b").unwrap();
        let a2 = cache.next_id("a").unwrap();
        assert_eq!((a1, a2), (1, 2));
        // b reserved the second block since a reserved first.
        assert_eq!(b1, 101);
    }

    #[test]
    fn needs_refill_tracks_the_threshold() {
        let cache = SequenceCache::new(FakeReserver::new(10)); // refill_at = 2
        for _ in 0..7 {
            cache.next_id("s").unwrap();
        }
        assert!(!cache.needs_refill("s")); // 3 remaining, above threshold
        cache.next_id("s").unwrap();
        assert!(cache.needs_refill("s")); // 2 remaining <= 2
    }

    struct FailingReserver;
    impl BlockReserver for FailingReserver {
        fn reserve(&self, _: &str) -> Result<(i64, i64), SeqError> {
            Err(SeqError::Backend("system db unreachable".into()))
        }
    }

    #[test]
    fn backend_errors_propagate() {
        let cache = SequenceCache::new(FailingReserver);
        assert!(matches!(cache.next_id("s"), Err(SeqError::Backend(_))));
    }

    struct BadSizeReserver;
    impl BlockReserver for BadSizeReserver {
        fn reserve(&self, _: &str) -> Result<(i64, i64), SeqError> {
            Ok((1, 0))
        }
    }

    #[test]
    fn non_positive_block_size_is_rejected() {
        let cache = SequenceCache::new(BadSizeReserver);
        assert!(cache.next_id("s").is_err());
    }

    struct OverflowReserver;
    impl BlockReserver for OverflowReserver {
        fn reserve(&self, _: &str) -> Result<(i64, i64), SeqError> {
            Ok((i64::MAX - 1, 10))
        }
    }

    #[test]
    fn overflowing_reservation_is_rejected_not_truncated() {
        let cache = SequenceCache::new(OverflowReserver);
        assert!(matches!(cache.next_id("s"), Err(SeqError::Exhausted(_))));
    }

    /// Records the peak number of `reserve` calls in flight at once, so a test
    /// can prove they are actually single-flighted (peak 1), not merely unique.
    struct SingleFlightProbe {
        size: i64,
        next_start: AtomicI64,
        in_flight: AtomicI64,
        max_in_flight: AtomicI64,
    }

    impl BlockReserver for SingleFlightProbe {
        fn reserve(&self, _: &str) -> Result<(i64, i64), SeqError> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(1)); // widen the window
            let start = self.next_start.fetch_add(self.size, Ordering::SeqCst);
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok((start, self.size))
        }
    }

    #[test]
    fn concurrent_drainers_single_flight_the_reservation() {
        use std::sync::Arc;
        // Block of 1: every id past the first drains and must reserve, so eight
        // threads race the same drained sequence continuously.
        let cache = Arc::new(SequenceCache::new(SingleFlightProbe {
            size: 1,
            next_start: AtomicI64::new(1),
            in_flight: AtomicI64::new(0),
            max_in_flight: AtomicI64::new(0),
        }));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                (0..50).map(|_| c.next_id("s").unwrap()).collect::<Vec<_>>()
            }));
        }
        let mut all: Vec<i64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let n = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(
            all.len(),
            n,
            "ids must be unique across concurrent drainers"
        );
        // The point of single-flighting: never two reservations at once.
        assert_eq!(cache.reserver.max_in_flight.load(Ordering::SeqCst), 1);
    }

    struct PanicOnceReserver {
        calls: AtomicI64,
        next_start: AtomicI64,
    }

    impl BlockReserver for PanicOnceReserver {
        fn reserve(&self, _: &str) -> Result<(i64, i64), SeqError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("backend panicked");
            }
            Ok((self.next_start.fetch_add(10, Ordering::SeqCst), 10))
        }
    }

    #[test]
    fn panic_in_reserve_does_not_strand_the_sequence() {
        let cache = SequenceCache::new(PanicOnceReserver {
            calls: AtomicI64::new(0),
            next_start: AtomicI64::new(1),
        });
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {})); // keep the expected panic quiet
        let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cache.next_id("s")));
        std::panic::set_hook(prev);
        assert!(first.is_err(), "the backend panic propagated");
        // The claim guard cleared `reserving`, so the sequence recovers instead
        // of deadlocking every future caller.
        assert_eq!(cache.next_id("s").unwrap(), 1);
    }
}
