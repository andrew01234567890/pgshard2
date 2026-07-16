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
// structural change (shard set or table catalog). Immutable.
type TopologySnapshot struct {
	Generation int64           `json:"generation"`
	ValidFrom  time.Time       `json:"validFrom"`
	Epoch      int64           `json:"epoch"`
	Shards     []ShardTopology `json:"shards"`
}

type ShardTopology struct {
	Name     string      `json:"name"`
	KeyRange KeyRangeRef `json:"keyRange"`
	Stanza   string      `json:"stanza"`
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
