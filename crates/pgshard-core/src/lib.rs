//! Core domain types for pgshard: keyspace ids, key ranges, shard functions,
//! and the topology model shared by the router, agent, and CLI.

pub const KEYSPACE_ID_BITS: u32 = 64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_id_width_is_64() {
        assert_eq!(KEYSPACE_ID_BITS, 64);
    }
}
