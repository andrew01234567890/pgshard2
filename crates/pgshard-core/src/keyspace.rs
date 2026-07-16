use std::fmt;

use thiserror::Error;

/// Position in the 64-bit keyspace: `keyspace_id = shard_function(shard key)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyspaceId(pub u64);

/// Half-open range `[start, end)` over keyspace ids. `end == None` means the
/// range extends to the top of the space (2^64).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyRange {
    start: u64,
    end: Option<u64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeyRangeError {
    #[error("empty range: start {start:#018x} >= end {end:#018x}")]
    Empty { start: u64, end: u64 },
    #[error("invalid bound {0:?}: expected lowercase hex, even number of digits, at most 16")]
    InvalidBound(String),
    #[error("invalid range syntax {0:?}: expected \"<start>-<end>\"")]
    InvalidSyntax(String),
    #[error("cannot split into {0} parts: range too narrow or parts == 0")]
    InvalidSplit(u32),
}

impl KeyRange {
    pub const FULL: KeyRange = KeyRange {
        start: 0,
        end: None,
    };

    pub fn new(start: u64, end: Option<u64>) -> Result<Self, KeyRangeError> {
        if let Some(end) = end
            && start >= end
        {
            return Err(KeyRangeError::Empty { start, end });
        }
        Ok(KeyRange { start, end })
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> Option<u64> {
        self.end
    }

    fn end_exclusive(&self) -> u128 {
        self.end.map_or(1u128 << 64, u128::from)
    }

    pub fn is_full(&self) -> bool {
        self.start == 0 && self.end.is_none()
    }

    pub fn contains(&self, id: KeyspaceId) -> bool {
        id.0 >= self.start && u128::from(id.0) < self.end_exclusive()
    }

    pub fn intersects(&self, other: &KeyRange) -> bool {
        u128::from(self.start) < other.end_exclusive()
            && u128::from(other.start) < self.end_exclusive()
    }

    /// Splits into `parts` contiguous sub-ranges of near-equal width
    /// (boundaries at `start + round(i * width / parts)`).
    pub fn split_evenly(&self, parts: u32) -> Result<Vec<KeyRange>, KeyRangeError> {
        if parts == 0 {
            return Err(KeyRangeError::InvalidSplit(parts));
        }
        let start = u128::from(self.start);
        let width = self.end_exclusive() - start;
        if width < u128::from(parts) {
            return Err(KeyRangeError::InvalidSplit(parts));
        }
        let bound = |i: u128| start + (i * width) / u128::from(parts);
        let mut ranges = Vec::with_capacity(parts as usize);
        for i in 0..u128::from(parts) {
            let lo = bound(i) as u64;
            let hi = bound(i + 1);
            let end = if hi == self.end_exclusive() {
                self.end
            } else {
                Some(hi as u64)
            };
            ranges.push(KeyRange { start: lo, end });
        }
        Ok(ranges)
    }
}

/// Formats a range bound in the canonical CRD/topology syntax: big-endian hex
/// with trailing zero bytes trimmed; zero formats as the empty string
/// (`"40"` means `0x4000_0000_0000_0000`).
pub fn format_bound(v: u64) -> String {
    if v == 0 {
        return String::new();
    }
    let full = format!("{v:016x}");
    let trimmed = full.trim_end_matches("00");
    // An odd number of remaining digits would change the byte alignment.
    if trimmed.len() % 2 == 1 {
        format!("{trimmed}0")
    } else {
        trimmed.to_string()
    }
}

pub fn parse_bound(s: &str) -> Result<u64, KeyRangeError> {
    if s.is_empty() {
        return Ok(0);
    }
    let invalid = || KeyRangeError::InvalidBound(s.to_string());
    if !s.len().is_multiple_of(2)
        || s.len() > 16
        || !s
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
        return Err(invalid());
    }
    let parsed = u64::from_str_radix(s, 16).map_err(|_| invalid())?;
    Ok(parsed << (4 * (16 - s.len())))
}

impl fmt::Display for KeyRange {
    /// `"<start>-<end>"` with bounds per [`format_bound`]; an open end is the
    /// empty string, so the full range renders as `"-"`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}",
            format_bound(self.start),
            self.end.map(format_bound).unwrap_or_default()
        )
    }
}

impl std::str::FromStr for KeyRange {
    type Err = KeyRangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (start, end) = s
            .split_once('-')
            .ok_or_else(|| KeyRangeError::InvalidSyntax(s.to_string()))?;
        let start = parse_bound(start)?;
        let end = if end.is_empty() {
            None
        } else {
            Some(parse_bound(end)?)
        };
        KeyRange::new(start, end)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PartitionError {
    #[error("no ranges")]
    Empty,
    #[error("first range starts at {0:#018x}, not 0")]
    DoesNotStartAtZero(u64),
    #[error("range {index} starts at {next_start:#018x} but previous range ends at {prev_end:?}")]
    GapOrOverlap {
        index: usize,
        prev_end: Option<u64>,
        next_start: u64,
    },
    #[error("last range ends at {0:#018x} instead of the top of the keyspace")]
    DoesNotEndAtMax(u64),
}

/// Checks that `ranges`, in order, exactly partition the full keyspace:
/// start at 0, each range begins where the previous ended, open end last.
pub fn validate_partition(ranges: &[KeyRange]) -> Result<(), PartitionError> {
    let Some(first) = ranges.first() else {
        return Err(PartitionError::Empty);
    };
    if first.start != 0 {
        return Err(PartitionError::DoesNotStartAtZero(first.start));
    }
    for (index, pair) in ranges.windows(2).enumerate() {
        if pair[0].end != Some(pair[1].start) {
            return Err(PartitionError::GapOrOverlap {
                index: index + 1,
                prev_end: pair[0].end,
                next_start: pair[1].start,
            });
        }
    }
    match ranges.last().unwrap().end {
        None => Ok(()),
        Some(end) => Err(PartitionError::DoesNotEndAtMax(end)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_round_trips() {
        for v in [
            0u64,
            1,
            0x40 << 56,
            0x80 << 56,
            0xc0de_0000_0000_0000,
            u64::MAX,
        ] {
            assert_eq!(parse_bound(&format_bound(v)).unwrap(), v, "{v:#x}");
        }
    }

    #[test]
    fn bound_formats_are_trimmed_and_left_aligned() {
        assert_eq!(format_bound(0), "");
        assert_eq!(format_bound(0x40 << 56), "40");
        assert_eq!(format_bound(0x4080 << 48), "4080");
        assert_eq!(parse_bound("40").unwrap(), 0x40 << 56);
        assert!(parse_bound("4").is_err());
        assert!(parse_bound("GG").is_err());
        assert!(parse_bound("40000000000000000000").is_err());
    }

    #[test]
    fn range_display_and_parse() {
        let range: KeyRange = "40-80".parse().unwrap();
        assert_eq!(range.start(), 0x40 << 56);
        assert_eq!(range.end(), Some(0x80 << 56));
        assert_eq!(range.to_string(), "40-80");
        assert_eq!(KeyRange::FULL.to_string(), "-");
        assert_eq!("-".parse::<KeyRange>().unwrap(), KeyRange::FULL);
        assert!("80-40".parse::<KeyRange>().is_err());
        assert!("40".parse::<KeyRange>().is_err());
    }

    #[test]
    fn contains_and_intersects() {
        let range: KeyRange = "40-80".parse().unwrap();
        assert!(!range.contains(KeyspaceId((0x40 << 56) - 1)));
        assert!(range.contains(KeyspaceId(0x40 << 56)));
        assert!(!range.contains(KeyspaceId(0x80 << 56)));

        let top: KeyRange = "c0-".parse().unwrap();
        assert!(top.contains(KeyspaceId(u64::MAX)));
        assert!(!top.intersects(&range));
        assert!(top.intersects(&KeyRange::FULL));
    }

    #[test]
    fn split_partitions_the_input() {
        let parts = KeyRange::FULL.split_evenly(4).unwrap();
        assert_eq!(parts.len(), 4);
        validate_partition(&parts).unwrap();
        assert_eq!(parts[1].to_string(), "40-80");

        let sub: KeyRange = "40-80".parse().unwrap();
        let sub_parts = sub.split_evenly(3).unwrap();
        assert_eq!(sub_parts.first().unwrap().start(), sub.start());
        assert_eq!(sub_parts.last().unwrap().end(), sub.end());
        for pair in sub_parts.windows(2) {
            assert_eq!(pair[0].end(), Some(pair[1].start()));
        }
    }

    #[test]
    fn partition_validation_rejects_gaps_and_overlaps() {
        assert_eq!(validate_partition(&[]), Err(PartitionError::Empty));

        let lo: KeyRange = "-40".parse().unwrap();
        let hi: KeyRange = "80-".parse().unwrap();
        assert!(matches!(
            validate_partition(&[lo, hi]),
            Err(PartitionError::GapOrOverlap { index: 1, .. })
        ));

        let mid: KeyRange = "40-80".parse().unwrap();
        validate_partition(&[lo, mid, hi]).unwrap();
    }
}
