package topology

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// KeyspaceID is a position in the 64-bit keyspace:
// keyspace_id = shard_function(shard key).
type KeyspaceID uint64

// ShardFunctionXxHash64V1 is the versioned name recorded in table configs.
// The mapping is frozen; see the golden-vector tests shared with Rust.
const ShardFunctionXxHash64V1 = "xxhash64_v1"

// HashInt64 hashes an integer shard key (all Postgres integer widths widen
// to int64 first): xxh64 seed 0 over 8-byte big-endian two's complement.
func HashInt64(v int64) KeyspaceID {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return KeyspaceID(xxhash.Sum64(b[:]))
}

// HashText hashes a text shard key: xxh64 seed 0 over the UTF-8 bytes.
func HashText(s string) KeyspaceID {
	return KeyspaceID(xxhash.Sum64String(s))
}

// HashUUID hashes a uuid shard key: xxh64 seed 0 over its 16 bytes.
func HashUUID(u [16]byte) KeyspaceID {
	return KeyspaceID(xxhash.Sum64(u[:]))
}

// HashBytes hashes a bytea shard key: xxh64 seed 0 over the raw bytes.
func HashBytes(b []byte) KeyspaceID {
	return KeyspaceID(xxhash.Sum64(b))
}
