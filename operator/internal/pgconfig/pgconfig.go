// Package pgconfig derives per-instance PostgreSQL configuration from a
// cluster's size class and overrides. Render is a pure function: given the
// same inputs it must produce byte-identical output (the config hash drives
// rolling restarts, so any nondeterminism would churn pods).
package pgconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

type classProfile struct {
	cpu            string
	memory         string
	replicas       int32
	syncMode       string
	syncNumber     int32
	maxConnections int
	maxWalSize     string
}

// Size classes: modest max_connections everywhere because the routers own
// pooling.
var classProfiles = map[pgshardv1alpha1.SizeClass]classProfile{
	"S":  {cpu: "1", memory: "4Gi", replicas: 2, syncMode: "off", syncNumber: 0, maxConnections: 100, maxWalSize: "2GB"},
	"M":  {cpu: "4", memory: "16Gi", replicas: 3, syncMode: "quorum", syncNumber: 1, maxConnections: 200, maxWalSize: "8GB"},
	"L":  {cpu: "8", memory: "64Gi", replicas: 3, syncMode: "quorum", syncNumber: 1, maxConnections: 300, maxWalSize: "16GB"},
	"XL": {cpu: "16", memory: "128Gi", replicas: 5, syncMode: "quorum", syncNumber: 2, maxConnections: 400, maxWalSize: "32GB"},
}

// Inputs to Render.
type Inputs struct {
	Class     pgshardv1alpha1.SizeClass
	Overrides *pgshardv1alpha1.SizeOverrides
	// User-supplied parameters, merged last (they win over derived values,
	// but never over platform-fixed ones).
	UserParameters map[string]string
	// Slots/senders headroom beyond replicas (reshard links, CDC streams).
	SlotHeadroom int32
}

// Rendered is the derived instance configuration.
type Rendered struct {
	Resources        corev1.ResourceRequirements
	ReplicasPerShard int32
	Synchronous      pgshardv1alpha1.SynchronousSpec
	Parameters       map[string]string
	// Content hash over parameters + resources; a change drives the
	// rolling-restart/reload flow.
	ConfigHash string
}

// Platform-fixed parameters: required for pgshard to function; user
// overrides are rejected silently by re-setting them last.
func platformFixed(replicas, headroom int32) map[string]string {
	slots := int(replicas) + int(headroom)
	return map[string]string{
		"wal_level":              "logical",
		"archive_mode":           "on",
		"hot_standby_feedback":   "on",
		"sync_replication_slots": "on",
		"password_encryption":    "scram-sha-256",
		"max_replication_slots":  fmt.Sprintf("%d", slots),
		"max_wal_senders":        fmt.Sprintf("%d", slots),
		"wal_log_hints":          "on", // pg_rewind without checksums assumption
	}
}

func Render(in Inputs) (Rendered, error) {
	class := in.Class
	if class == "" {
		class = "M"
	}
	profile, ok := classProfiles[class]
	if !ok {
		return Rendered{}, fmt.Errorf("unknown size class %q", class)
	}

	cpu := resource.MustParse(profile.cpu)
	memory := resource.MustParse(profile.memory)
	replicas := profile.replicas
	sync := pgshardv1alpha1.SynchronousSpec{Mode: profile.syncMode, Number: profile.syncNumber}

	if o := in.Overrides; o != nil {
		if o.Resources != nil {
			if v, ok := o.Resources.Limits[corev1.ResourceCPU]; ok {
				cpu = v
			}
			if v, ok := o.Resources.Limits[corev1.ResourceMemory]; ok {
				memory = v
			}
		}
		if o.ReplicasPerShard != nil {
			replicas = *o.ReplicasPerShard
		}
		if o.Synchronous != nil {
			sync = *o.Synchronous
		}
	}
	if sync.Mode != "off" && sync.Number >= replicas {
		return Rendered{}, fmt.Errorf(
			"synchronous.number (%d) must be smaller than replicasPerShard (%d)",
			sync.Number, replicas)
	}

	memBytes := memory.Value()
	cpuCores := max(cpu.MilliValue()/1000, 1)

	params := map[string]string{}

	// PGTune-conventional formulas off the memory limit.
	sharedBuffers := memBytes / 4
	if cap := int64(32) << 30; sharedBuffers > cap {
		sharedBuffers = cap
	}
	params["shared_buffers"] = mb(sharedBuffers)
	params["effective_cache_size"] = mb(memBytes * 70 / 100)
	maintenance := memBytes / 16
	if cap := int64(2) << 30; maintenance > cap {
		maintenance = cap
	}
	params["maintenance_work_mem"] = mb(maintenance)
	workMem := memBytes / 4 / int64(profile.maxConnections)
	if floor := int64(4) << 20; workMem < floor {
		workMem = floor
	}
	params["work_mem"] = mb(workMem)
	params["max_connections"] = fmt.Sprintf("%d", profile.maxConnections)
	params["max_wal_size"] = profile.maxWalSize
	params["checkpoint_completion_target"] = "0.9"
	params["random_page_cost"] = "1.1"
	params["effective_io_concurrency"] = "256"
	params["wal_compression"] = "zstd"
	params["max_worker_processes"] = fmt.Sprintf("%d", cpuCores)
	params["max_parallel_workers"] = fmt.Sprintf("%d", cpuCores)

	// PostgreSQL 18 asynchronous I/O.
	params["io_method"] = "worker"
	ioWorkers := max(cpuCores/2, 3)
	params["io_workers"] = fmt.Sprintf("%d", ioWorkers)

	maps.Copy(params, in.UserParameters)
	maps.Copy(params, platformFixed(replicas, in.SlotHeadroom))

	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: cpu, corev1.ResourceMemory: memory},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: cpu, corev1.ResourceMemory: memory},
	}

	return Rendered{
		Resources:        resources,
		ReplicasPerShard: replicas,
		Synchronous:      sync,
		Parameters:       params,
		ConfigHash:       hashConfig(params, cpu.String(), memory.String()),
	}, nil
}

func mb(bytes int64) string {
	return fmt.Sprintf("%dMB", bytes>>20)
}

func hashConfig(params map[string]string, cpu, memory string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "cpu=%s\nmemory=%s\n", cpu, memory)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, params[k])
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// restartParameters mirrors pg_settings.context == 'postmaster' for every
// parameter Render can emit; anything else we set is reloadable.
var restartParameters = map[string]bool{
	"shared_buffers":        true,
	"max_connections":       true,
	"max_worker_processes":  true,
	"wal_level":             true,
	"archive_mode":          true,
	"max_wal_senders":       true,
	"max_replication_slots": true,
	"io_method":             true,
	"wal_log_hints":         true,
}

// ClassifyDiff splits changed parameters into reload-safe and
// restart-requiring sets.
func ClassifyDiff(old, new map[string]string) (reload, restart []string) {
	seen := map[string]bool{}
	for k := range old {
		seen[k] = true
	}
	for k := range new {
		seen[k] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if old[k] == new[k] {
			continue
		}
		if restartParameters[k] {
			restart = append(restart, k)
		} else {
			reload = append(reload, k)
		}
	}
	return reload, restart
}
