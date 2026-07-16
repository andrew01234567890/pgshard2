package topology

import (
	"math"
	"testing"
)

// These vectors are frozen and mirrored in
// crates/pgshard-core/src/shardfn.rs (golden_vectors_are_frozen).
// xxhash64_v1 is versioned by name: never change them.
func TestGoldenVectorsMatchRust(t *testing.T) {
	intCases := map[int64]KeyspaceID{
		0:             0x34c96acdcadb1bbb,
		1:             0x9f1ffc793b8a47da,
		42:            0xa2c396223f8bdbdf,
		-1:            0x85d136adb773c6c9,
		math.MaxInt64: 0x043de1bbaf341994,
	}
	for in, want := range intCases {
		if got := HashInt64(in); got != want {
			t.Errorf("HashInt64(%d) = %#x, want %#x", in, got, want)
		}
	}

	textCases := map[string]KeyspaceID{
		"":               0xef46db3751d8e999,
		"hello":          0x26c7827d889f6da3,
		"customer-12345": 0xab540c504509fb51,
	}
	for in, want := range textCases {
		if got := HashText(in); got != want {
			t.Errorf("HashText(%q) = %#x, want %#x", in, got, want)
		}
	}

	uuid := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if got := HashUUID(uuid); got != 0x44b6ef2fb84169f7 {
		t.Errorf("HashUUID = %#x, want 0x44b6ef2fb84169f7", got)
	}
	if got := HashBytes([]byte{0xde, 0xad, 0xbe, 0xef}); got != 0x2ff5cfb6af9aaf68 {
		t.Errorf("HashBytes = %#x, want 0x2ff5cfb6af9aaf68", got)
	}
}
