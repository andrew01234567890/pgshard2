//! The shards of one sharded database, in keyspace order.

use pgshard_core::{KeyRange, KeyspaceId, PartitionError, validate_partition};

/// A shard's stable identity — its keyrange name, e.g. `mycl-40-80`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub String);

impl ShardId {
    pub fn new(id: impl Into<String>) -> Self {
        ShardId(id.into())
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The shards that together partition the full keyspace of one database, in
/// keyspace order. Built once from the topology; every keyspace id routes to
/// exactly one shard.
#[derive(Debug, Clone)]
pub struct ShardCatalog {
    shards: Vec<(KeyRange, ShardId)>,
}

impl ShardCatalog {
    /// Build from `(range, id)` pairs. The ranges must exactly partition the
    /// keyspace (start at 0, contiguous, open-ended last) — the same invariant
    /// the operator enforces on the shard set.
    pub fn new(shards: Vec<(KeyRange, ShardId)>) -> Result<Self, PartitionError> {
        let ranges: Vec<KeyRange> = shards.iter().map(|(r, _)| *r).collect();
        validate_partition(&ranges)?;
        Ok(Self { shards })
    }

    /// The shard owning `id`. Infallible: a validated partition covers the whole
    /// keyspace, so exactly one shard contains any id.
    pub fn route(&self, id: KeyspaceId) -> &ShardId {
        &self
            .shards
            .iter()
            .find(|(range, _)| range.contains(id))
            .expect("a validated partition covers the whole keyspace")
            .1
    }

    /// Every shard, in keyspace order.
    pub fn all(&self) -> Vec<ShardId> {
        self.shards.iter().map(|(_, id)| id.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_core::PartitionError;

    fn shards() -> Vec<(KeyRange, ShardId)> {
        KeyRange::FULL
            .split_evenly(4)
            .unwrap()
            .into_iter()
            .map(|r| (r, ShardId::new(r.to_string())))
            .collect()
    }

    #[test]
    fn routes_each_id_to_its_owning_shard() {
        let cat = ShardCatalog::new(shards()).unwrap();
        assert_eq!(cat.len(), 4);
        assert_eq!(cat.route(KeyspaceId(0)), &ShardId::new("-40"));
        assert_eq!(cat.route(KeyspaceId(0x40 << 56)), &ShardId::new("40-80"));
        assert_eq!(cat.route(KeyspaceId(u64::MAX)), &ShardId::new("c0-"));
        assert_eq!(
            cat.all(),
            vec![
                ShardId::new("-40"),
                ShardId::new("40-80"),
                ShardId::new("80-c0"),
                ShardId::new("c0-"),
            ]
        );
    }

    #[test]
    fn rejects_a_non_partitioning_shard_set() {
        // A gap in the middle is not a valid partition.
        let lo: KeyRange = "-40".parse().unwrap();
        let hi: KeyRange = "80-".parse().unwrap();
        assert!(matches!(
            ShardCatalog::new(vec![(lo, ShardId::new("lo")), (hi, ShardId::new("hi"))]),
            Err(PartitionError::GapOrOverlap { .. })
        ));
        assert_eq!(
            ShardCatalog::new(vec![]).unwrap_err(),
            PartitionError::Empty
        );
    }
}
