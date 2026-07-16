package topology

import (
	"math"
	"math/rand"
	"testing"
)

func mustParse(t *testing.T, s string) KeyRange {
	t.Helper()
	r, err := ParseKeyRange(s)
	if err != nil {
		t.Fatalf("ParseKeyRange(%q): %v", s, err)
	}
	return r
}

func TestBoundRoundTrips(t *testing.T) {
	fixed := make([]uint64, 0, 10006)
	fixed = append(fixed, 0, 1, 0x40<<56, 0x80<<56, 0xc0de000000000000, math.MaxUint64)
	rng := rand.New(rand.NewSource(1))
	for range 10000 {
		fixed = append(fixed, rng.Uint64())
	}
	for _, v := range fixed {
		got, err := ParseBound(FormatBound(v))
		if err != nil || got != v {
			t.Fatalf("round trip %#016x -> %q -> %#016x, err=%v", v, FormatBound(v), got, err)
		}
	}
}

func TestBoundFormatting(t *testing.T) {
	if FormatBound(0) != "" || FormatBound(0x40<<56) != "40" || FormatBound(0x4080<<48) != "4080" {
		t.Fatalf("unexpected canonical formats: %q %q %q",
			FormatBound(0), FormatBound(0x40<<56), FormatBound(0x4080<<48))
	}
	for _, bad := range []string{"4", "GG", "4A", "40000000000000000000"} {
		if _, err := ParseBound(bad); err == nil {
			t.Errorf("ParseBound(%q) should fail", bad)
		}
	}
}

func TestRangeParseAndDisplay(t *testing.T) {
	r := mustParse(t, "40-80")
	if r.Start() != 0x40<<56 {
		t.Fatalf("start = %#x", r.Start())
	}
	if end, ok := r.End(); !ok || end != 0x80<<56 {
		t.Fatalf("end = %#x ok=%v", end, ok)
	}
	if r.String() != "40-80" || FullRange.String() != "-" {
		t.Fatalf("display: %q %q", r.String(), FullRange.String())
	}
	if full := mustParse(t, "-"); full != FullRange {
		t.Fatalf("parsed full range mismatch: %+v", full)
	}
	for _, bad := range []string{"80-40", "40", "40-40"} {
		if _, err := ParseKeyRange(bad); err == nil {
			t.Errorf("ParseKeyRange(%q) should fail", bad)
		}
	}
}

func TestContainsAndIntersects(t *testing.T) {
	mid := mustParse(t, "40-80")
	if mid.Contains(KeyspaceID(0x40<<56-1)) || !mid.Contains(KeyspaceID(0x40<<56)) ||
		mid.Contains(KeyspaceID(0x80<<56)) {
		t.Fatal("contains boundary behavior wrong")
	}
	top := mustParse(t, "c0-")
	if !top.Contains(KeyspaceID(math.MaxUint64)) {
		t.Fatal("open range must contain MaxUint64")
	}
	if top.Intersects(mid) || !top.Intersects(FullRange) {
		t.Fatal("intersects wrong")
	}
}

func TestSplitEvenlyPartitions(t *testing.T) {
	for _, parts := range []uint32{1, 2, 3, 4, 7, 64} {
		split, err := FullRange.SplitEvenly(parts)
		if err != nil {
			t.Fatalf("split %d: %v", parts, err)
		}
		if len(split) != int(parts) {
			t.Fatalf("split %d: got %d ranges", parts, len(split))
		}
		if err := ValidatePartition(split); err != nil {
			t.Fatalf("split %d: %v", parts, err)
		}
	}

	quarters, _ := FullRange.SplitEvenly(4)
	if quarters[1].String() != "40-80" {
		t.Fatalf("second quarter = %s, want 40-80 (Rust parity)", quarters[1])
	}

	sub := mustParse(t, "40-80")
	subSplit, err := sub.SplitEvenly(3)
	if err != nil {
		t.Fatal(err)
	}
	if subSplit[0].Start() != sub.Start() {
		t.Fatal("first sub-range must keep the start")
	}
	if end, ok := subSplit[2].End(); !ok {
		t.Fatal("closed input must produce closed output")
	} else if wantEnd, _ := sub.End(); end != wantEnd {
		t.Fatalf("last end = %#x", end)
	}
	for i := 1; i < len(subSplit); i++ {
		prevEnd, _ := subSplit[i-1].End()
		if prevEnd != subSplit[i].Start() {
			t.Fatal("sub-ranges must be adjacent")
		}
	}
}

func TestSplitSingleOwner(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	split, _ := FullRange.SplitEvenly(7)
	for range 10000 {
		id := KeyspaceID(rng.Uint64())
		owners := 0
		for _, r := range split {
			if r.Contains(id) {
				owners++
			}
		}
		if owners != 1 {
			t.Fatalf("id %#x owned by %d ranges", id, owners)
		}
	}
}

func TestValidatePartitionRejectsGapsAndOverlaps(t *testing.T) {
	if err := ValidatePartition(nil); err == nil {
		t.Fatal("empty partition must fail")
	}
	lo := mustParse(t, "-40")
	mid := mustParse(t, "40-80")
	hi := mustParse(t, "80-")
	if err := ValidatePartition([]KeyRange{lo, mid, hi}); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePartition([]KeyRange{lo, hi}); err == nil {
		t.Fatal("gap must fail")
	}
	if err := ValidatePartition([]KeyRange{lo, mid}); err == nil {
		t.Fatal("missing open tail must fail")
	}
	if err := ValidatePartition([]KeyRange{mid, hi}); err == nil {
		t.Fatal("nonzero start must fail")
	}
}
