//! Generated gRPC contract shared by the router, agent, CLI, and (via the
//! same .proto sources) the Go operator.

pub mod pgshard {
    pub mod v1 {
        tonic::include_proto!("pgshard.v1");
    }
}

pub use pgshard::v1;

#[cfg(test)]
mod tests {
    use super::v1;

    #[test]
    fn generated_types_are_usable() {
        let range = v1::KeyRange {
            start: 0x40 << 56,
            end: Some(0x80 << 56),
        };
        assert!(range.end.unwrap() > range.start);

        let vgtid = v1::VGtid {
            shard_gtids: vec![v1::ShardGtid {
                shard: "mycl-x40-x80".into(),
                lsn: Some(v1::Lsn { value: 42 }),
                tables_copied: vec![],
            }],
        };
        assert_eq!(vgtid.shard_gtids[0].lsn.as_ref().unwrap().value, 42);
    }
}
