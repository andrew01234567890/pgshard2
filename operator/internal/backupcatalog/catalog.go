// Package backupcatalog defines the object-storage catalog manifests and
// resolves restore targets into per-shard pgBackRest restore plans.
//
// The catalog in object storage is authoritative — a restore into a fresh
// cluster reads only the bucket (CRD status is a cache). Topology snapshots
// give a total function time -> topology, which is what makes PITR across
// topology drift work: a restore always materializes the shard set that was
// live at the target point, then converges to the desired layout with a
// standard online reshard.
package backupcatalog

import (
	"time"
)

// TopologySnapshot is catalog/topology/gen-<N>.json — written on every
// structural change (shard set or table catalog). Immutable. It must be the
// COMPLETE structural routing view: a restore into a fresh cluster
// reconstructs routing from the snapshot alone (CRDs may be gone with the
// cluster), so shard ranges without the hash function and table catalog
// would leave the restored data unroutable — or, worse, routable under
// different rules than the ones it was written with.
type TopologySnapshot struct {
	Generation int64     `json:"generation"`
	ValidFrom  time.Time `json:"validFrom"`
	Epoch      int64     `json:"epoch"`
	// HashFunction is the cluster's shard function (the keyspace-id hash the
	// shard ranges partition). Restoring data hashed with one function into a
	// cluster routing with another silently misplaces every row.
	HashFunction string          `json:"hashFunction"`
	Shards       []ShardTopology `json:"shards"`
	// Tables is the compiled table catalog live at this generation, projected
	// from the same source as PgShardRouting.
	Tables []TableTopology `json:"tables"`
}

type ShardTopology struct {
	Name     string      `json:"name"`
	KeyRange KeyRangeRef `json:"keyRange"`
	Stanza   string      `json:"stanza"`
	// Role: "data" or "system". Ranges alone cannot identify the sequence
	// host (the system shard has no range, like a full-range data shard),
	// and a restore that guessed would rebuild sequences on the wrong shard.
	Role string `json:"role"`
	// State is the compiled routing state at snapshot time (serving,
	// buffered, readOnly, draining, hidden). Mid-reshard, sources and hidden
	// targets hold OVERLAPPING ranges: without the state a fresh restore
	// cannot know which side may serve, and publishing both would return
	// duplicate or wrong rows.
	State string `json:"state"`
}

// TableTopology mirrors the compiled RoutingTable structurally: everything a
// restored cluster needs to route the table's data the way it was written.
type TableTopology struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	// "sharded" or "global".
	Type           string `json:"type"`
	ShardKeyColumn string `json:"shardKeyColumn,omitempty"`
	// Wire value of the shard-key column type (matches the router's
	// ShardKeyType); required for sharded tables — hashing a literal as the
	// wrong type routes it to the wrong shard.
	ShardKeyType string             `json:"shardKeyType,omitempty"`
	Sequences    []SequenceTopology `json:"sequences,omitempty"`
}

type SequenceTopology struct {
	Column   string `json:"column"`
	Sequence string `json:"sequence"`
}

type KeyRangeRef struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

// BarrierManifest is catalog/barriers/.../b-<id>.json — one coordinated
// restore point across every shard (cross-shard consistent PITR target).
type BarrierManifest struct {
	ID                 string         `json:"id"`
	Time               time.Time      `json:"time"`
	RoutingEpoch       int64          `json:"routingEpoch"`
	TopologyGeneration int64          `json:"topologyGeneration"`
	Shards             []BarrierShard `json:"shards"`
}

type BarrierShard struct {
	Name         string `json:"name"`
	Stanza       string `json:"stanza"`
	LSN          string `json:"lsn"`
	Timeline     int32  `json:"timeline"`
	RestorePoint string `json:"restorePoint"`
}

// BackupManifest is catalog/backups/bk-<ts>.json — one coordinated cluster
// backup (per-shard pgBackRest backup sets).
type BackupManifest struct {
	ID                 string        `json:"id"`
	CompletedAt        time.Time     `json:"completedAt"`
	TopologyGeneration int64         `json:"topologyGeneration"`
	RoutingEpoch       int64         `json:"routingEpoch"`
	Shards             []BackupShard `json:"shards"`
}

type BackupShard struct {
	Name     string    `json:"name"`
	Stanza   string    `json:"stanza"`
	Label    string    `json:"label"` // pgBackRest backup set label
	StopLSN  string    `json:"stopLsn"`
	Timeline int32     `json:"timeline"`
	StopTime time.Time `json:"stopTime"`
}

// Catalog is the full manifest set a resolver works over. Callers load it
// from object storage; tests construct it directly.
type Catalog struct {
	Topologies []TopologySnapshot
	Barriers   []BarrierManifest
	Backups    []BackupManifest
}
