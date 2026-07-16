use pgshard_core::keyspace::{KeyRange, format_bound, parse_bound};
use pgshard_core::{KeyspaceId, validate_partition};
use proptest::prelude::*;

fn key_range() -> impl Strategy<Value = KeyRange> {
    (any::<u64>(), proptest::option::of(any::<u64>()))
        .prop_filter_map("empty range", |(start, end)| KeyRange::new(start, end).ok())
}

proptest! {
    #[test]
    fn bound_format_round_trips(v in any::<u64>()) {
        prop_assert_eq!(parse_bound(&format_bound(v)).unwrap(), v);
    }

    #[test]
    fn range_display_round_trips(range in key_range()) {
        let parsed: KeyRange = range.to_string().parse().unwrap();
        prop_assert_eq!(parsed, range);
    }

    #[test]
    fn split_partitions_and_preserves_bounds(range in key_range(), parts in 1u32..=64) {
        prop_assume!(range.end().is_none_or(|e| e - range.start() >= u64::from(parts)));
        let split = range.split_evenly(parts).unwrap();
        prop_assert_eq!(split.len(), parts as usize);
        prop_assert_eq!(split.first().unwrap().start(), range.start());
        prop_assert_eq!(split.last().unwrap().end(), range.end());
        for pair in split.windows(2) {
            prop_assert_eq!(pair[0].end(), Some(pair[1].start()));
        }
    }

    #[test]
    fn full_split_validates_as_partition(parts in 1u32..=64) {
        let split = KeyRange::FULL.split_evenly(parts).unwrap();
        prop_assert!(validate_partition(&split).is_ok());
    }

    #[test]
    fn exactly_one_partition_member_contains_any_id(id in any::<u64>(), parts in 1u32..=64) {
        let split = KeyRange::FULL.split_evenly(parts).unwrap();
        let owners = split.iter().filter(|r| r.contains(KeyspaceId(id))).count();
        prop_assert_eq!(owners, 1);
    }

    #[test]
    fn contains_implies_intersects_point_range(range in key_range(), id in any::<u64>()) {
        if range.contains(KeyspaceId(id)) {
            let point = KeyRange::new(id, id.checked_add(1)).unwrap_or(
                KeyRange::new(id, None).unwrap(),
            );
            prop_assert!(range.intersects(&point));
        }
    }
}
