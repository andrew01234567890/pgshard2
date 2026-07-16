//! Global sequence block cache for the router.
//!
//! Sharded auto-increment columns can't use per-shard Postgres sequences —
//! the values would collide across shards. Instead each sequence lives once
//! in the unsharded system database, and routers hand out ids from blocks
//! they reserve there in bulk (Vitess-style). A block reservation is a
//! single `UPDATE pgshard.sequences SET next_id = next_id + cache_size ...
//! RETURNING` that grants a half-open range `[start, start+size)`; the
//! router then hands ids out of that range without touching the database
//! until it drains. Ids are monotonic-ish (gaps on router restart are
//! expected and documented) and never duplicated across routers.
//!
//! This crate is the in-memory allocator over reserved blocks plus the
//! trait for the reservation backend; the concrete Postgres backend lives
//! in the router where the connection pool is.

use std::collections::HashMap;
use std::sync::Mutex;

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
    pub fn new(start: i64, size: i64) -> Block {
        Block {
            next: start,
            end: start.saturating_add(size),
        }
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
pub trait BlockReserver: Send + Sync {
    /// Returns the reserved range's start and size for `sequence`.
    fn reserve(&self, sequence: &str) -> Result<(i64, i64), SeqError>;
}

struct SeqState {
    block: Block,
    /// Reserve the next block once the current one drops to this many ids,
    /// so steady-state traffic never blocks on the database.
    refill_at: i64,
    reserving: bool,
}

/// Hands out ids for many sequences, reserving fresh blocks as they drain.
/// Cheap and lock-guarded; the hot path is a single mutex + integer bump.
pub struct SequenceCache<R: BlockReserver> {
    reserver: R,
    state: Mutex<HashMap<String, SeqState>>,
}

impl<R: BlockReserver> SequenceCache<R> {
    pub fn new(reserver: R) -> SequenceCache<R> {
        SequenceCache {
            reserver,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the next id for `sequence`, reserving a new block if the
    /// current one is exhausted (or was never reserved).
    pub fn next_id(&self, sequence: &str) -> Result<i64, SeqError> {
        // Fast path under the lock; on a drained block we reserve while
        // holding no lock, then re-acquire to install the block (the
        // double-buffered async refill is a router-side optimization on
        // top of this).
        {
            let mut state = self.state.lock().expect("sequence cache lock");
            if let Some(entry) = state.get_mut(sequence)
                && let Some(id) = entry.block.take()
            {
                return Ok(id);
            }
        }

        let (start, size) = self.reserver.reserve(sequence)?;
        if size <= 0 {
            return Err(SeqError::Backend(format!(
                "sequence {sequence:?} reserved a non-positive block size {size}"
            )));
        }
        let mut block = Block::new(start, size);
        let id = block
            .take()
            .ok_or_else(|| SeqError::Exhausted(sequence.to_string()))?;
        let refill_at = (size / 5).max(1);

        let mut state = self.state.lock().expect("sequence cache lock");
        state.insert(
            sequence.to_string(),
            SeqState {
                block,
                refill_at,
                reserving: false,
            },
        );
        Ok(id)
    }

    /// Whether the sequence's cached block has drained past its refill
    /// threshold (a router uses this to trigger a background reservation).
    pub fn needs_refill(&self, sequence: &str) -> bool {
        let state = self.state.lock().expect("sequence cache lock");
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
}
