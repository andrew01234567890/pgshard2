// Package topology implements keyspace math shared with the Rust data plane:
// key ranges, canonical bound syntax, partition validation, and the
// xxhash64_v1 shard function. Semantics must stay in lockstep with
// crates/pgshard-core (golden vectors enforce the hash side).
package topology

import (
	"fmt"
	"math/bits"
	"strings"
)

// KeyRange is a half-open range [start, end) over the 64-bit keyspace.
// A range without a closed end extends to the top of the space (2^64).
// The zero value is the full range.
type KeyRange struct {
	start  uint64
	end    uint64
	closed bool
}

// FullRange covers the entire keyspace.
var FullRange = KeyRange{}

func NewKeyRange(start, end uint64) (KeyRange, error) {
	if start >= end {
		return KeyRange{}, fmt.Errorf("empty range: start %#016x >= end %#016x", start, end)
	}
	return KeyRange{start: start, end: end, closed: true}, nil
}

func NewOpenKeyRange(start uint64) KeyRange {
	return KeyRange{start: start}
}

func (r KeyRange) Start() uint64 { return r.start }

// End returns the exclusive end bound; ok is false for an open range.
func (r KeyRange) End() (end uint64, ok bool) { return r.end, r.closed }

func (r KeyRange) IsFull() bool { return !r.closed && r.start == 0 }

func (r KeyRange) Contains(id KeyspaceID) bool {
	return uint64(id) >= r.start && (!r.closed || uint64(id) < r.end)
}

func (r KeyRange) Intersects(other KeyRange) bool {
	return (!other.closed || r.start < other.end) && (!r.closed || other.start < r.end)
}

// SplitEvenly cuts the range into parts contiguous sub-ranges of near-equal
// width, with boundaries at start + floor(i*width/parts).
func (r KeyRange) SplitEvenly(parts uint32) ([]KeyRange, error) {
	if parts == 0 {
		return nil, fmt.Errorf("cannot split into 0 parts")
	}
	// bound(i) computed with 128-bit intermediates; width = 2^64 - start for
	// open ranges (the full 2^64 width is the hi=i, lo=0 dividend case).
	bound := func(i uint64) uint64 {
		if !r.closed && r.start == 0 {
			q, _ := bits.Div64(i, 0, uint64(parts))
			return q
		}
		width := r.end - r.start // wraps to 2^64-start for open ranges
		if !r.closed {
			width = -r.start
		}
		hi, lo := bits.Mul64(i, width)
		q, _ := bits.Div64(hi, lo, uint64(parts))
		return r.start + q
	}
	if !r.IsFull() {
		width := r.end - r.start
		if !r.closed {
			width = -r.start
		}
		if width < uint64(parts) {
			return nil, fmt.Errorf("cannot split %s into %d parts: too narrow", r, parts)
		}
	}
	ranges := make([]KeyRange, 0, parts)
	for i := uint64(0); i < uint64(parts); i++ {
		sub := KeyRange{start: bound(i)}
		if i == uint64(parts)-1 {
			sub.end, sub.closed = r.end, r.closed
		} else {
			sub.end, sub.closed = bound(i+1), true
		}
		ranges = append(ranges, sub)
	}
	return ranges, nil
}

// FormatBound renders a bound in the canonical syntax: big-endian hex with
// trailing zero bytes trimmed; zero is the empty string ("40" means
// 0x4000000000000000).
func FormatBound(v uint64) string {
	if v == 0 {
		return ""
	}
	s := fmt.Sprintf("%016x", v)
	s = strings.TrimRight(s, "0")
	if len(s)%2 == 1 {
		s += "0"
	}
	return s
}

// ParseBound is the inverse of FormatBound: lowercase hex, even number of
// digits, at most 16, left-aligned into the 64-bit space.
func ParseBound(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	if len(s)%2 != 0 || len(s) > 16 {
		return 0, fmt.Errorf("invalid bound %q: expected even number of hex digits, at most 16", s)
	}
	var v uint64
	for _, c := range []byte(s) {
		var d uint64
		switch {
		case c >= '0' && c <= '9':
			d = uint64(c - '0')
		case c >= 'a' && c <= 'f':
			d = uint64(c-'a') + 10
		default:
			return 0, fmt.Errorf("invalid bound %q: expected lowercase hex", s)
		}
		v = v<<4 | d
	}
	return v << (4 * (16 - len(s))), nil
}

// String renders "<start>-<end>"; an open end is empty, so the full range is "-".
func (r KeyRange) String() string {
	end := ""
	if r.closed {
		end = FormatBound(r.end)
	}
	return FormatBound(r.start) + "-" + end
}

func ParseKeyRange(s string) (KeyRange, error) {
	startStr, endStr, found := strings.Cut(s, "-")
	if !found {
		return KeyRange{}, fmt.Errorf("invalid range %q: expected \"<start>-<end>\"", s)
	}
	start, err := ParseBound(startStr)
	if err != nil {
		return KeyRange{}, err
	}
	if endStr == "" {
		return NewOpenKeyRange(start), nil
	}
	end, err := ParseBound(endStr)
	if err != nil {
		return KeyRange{}, err
	}
	return NewKeyRange(start, end)
}

// ValidatePartition checks that ranges, in order, exactly partition the full
// keyspace: start at 0, each range begins where the previous ended, open end
// last.
func ValidatePartition(ranges []KeyRange) error {
	if len(ranges) == 0 {
		return fmt.Errorf("no ranges")
	}
	if ranges[0].start != 0 {
		return fmt.Errorf("first range starts at %#016x, not 0", ranges[0].start)
	}
	for i := 1; i < len(ranges); i++ {
		prev, next := ranges[i-1], ranges[i]
		if !prev.closed || prev.end != next.start {
			return fmt.Errorf("range %d starts at %#016x but previous range ends at %s", i, next.start, prev)
		}
	}
	if last := ranges[len(ranges)-1]; last.closed {
		return fmt.Errorf("last range ends at %#016x instead of the top of the keyspace", last.end)
	}
	return nil
}
