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
	"regexp"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

// Parameter and mode names that recur across the profile table, the
// platform-fixed set, and the restart classification.
const (
	SyncQuorum          = "quorum"
	SyncOff             = "off"
	ParamWalLevel       = "wal_level"
	ParamSharedBuffers  = "shared_buffers"
	ParamWorkMem        = "work_mem"
	ParamRandomPageCost = "random_page_cost"
	ParamMaxConnections = "max_connections"
	ssdRandomPageCost   = "1.1"
	logicalWalLevel     = "logical"

	sharedBuffersCap = int64(32) << 30
	maintenanceCap   = int64(2) << 30
	workMemFloor     = int64(4) << 20
	// io_workers has a hard PostgreSQL maximum of 32; exceeding it is a FATAL
	// at startup.
	ioWorkersMax = 32
	// PostgreSQL's logical-replication launcher permanently holds one
	// max_worker_processes slot, so a pool sized to a 1-vCPU class would
	// starve the apply/tablesync workers that reshard and CDC require. Floor
	// the pool (PostgreSQL's own stock default is 8).
	minWorkerProcesses = 8
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
	"S":  {cpu: "1", memory: "4Gi", replicas: 2, syncMode: SyncOff, syncNumber: 0, maxConnections: 100, maxWalSize: "2GB"},
	"M":  {cpu: "4", memory: "16Gi", replicas: 3, syncMode: SyncQuorum, syncNumber: 1, maxConnections: 200, maxWalSize: "8GB"},
	"L":  {cpu: "8", memory: "64Gi", replicas: 3, syncMode: SyncQuorum, syncNumber: 1, maxConnections: 300, maxWalSize: "16GB"},
	"XL": {cpu: "16", memory: "128Gi", replicas: 5, syncMode: SyncQuorum, syncNumber: 2, maxConnections: 400, maxWalSize: "32GB"},
}

// Inputs to Render.
type Inputs struct {
	Class     pgshardv1alpha1.SizeClass
	Overrides *pgshardv1alpha1.SizeOverrides
	// User-supplied parameters, merged last (they win over derived values,
	// but never over platform-fixed ones). Sanitized before use.
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

var gucNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_.]*$`)

// rejectedParameters are execution- or library-loading settings a user must
// never set through spec.parameters: PostgreSQL runs them as shell commands or
// loads them as native code under the postgres OS user, and the `include*`
// directives would pull in an unsanitized file that could set any of the
// others. Platform-fixed GUCs are enforced separately by re-asserting them
// last.
var rejectedParameters = map[string]bool{
	// Shell-command execution.
	"archive_command":         true,
	"restore_command":         true,
	"archive_cleanup_command": true,
	"recovery_end_command":    true,
	"ssl_passphrase_command":  true,
	// Native-library loading.
	"shared_preload_libraries":  true,
	"session_preload_libraries": true,
	"local_preload_libraries":   true,
	"archive_library":           true,
	"oauth_validator_libraries": true,
	"jit_provider":              true,
	// Configuration-file inclusion (not GUCs, but valid postgresql.conf
	// directives that would load an unsanitized file).
	"include":           true,
	"include_if_exists": true,
	"include_dir":       true,
}

// sanitizeUserParameters normalizes keys (trim + lowercase, since PostgreSQL
// GUC names are case-insensitive and whitespace-insignificant) and rejects any
// that are malformed, carry a newline (which would inject extra
// postgresql.conf directives and defeat the platform-fixed keys), name an
// execution-capable GUC, or collide after normalization.
func sanitizeUserParameters(in map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(in))
	for k, v := range in {
		key := strings.ToLower(strings.TrimSpace(k))
		if !gucNameRe.MatchString(key) {
			return nil, fmt.Errorf("invalid parameter name %q", k)
		}
		if strings.ContainsAny(v, "\r\n") {
			return nil, fmt.Errorf("parameter %q value must not contain a newline", key)
		}
		if rejectedParameters[key] {
			return nil, fmt.Errorf("parameter %q may not be set via spec.parameters", key)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("parameter %q specified more than once (case/whitespace variants)", key)
		}
		out[key] = v
	}
	return out, nil
}

// Platform-fixed parameters: required for pgshard to function; user overrides
// are rejected by re-setting them last.
func platformFixed(replicas, headroom int32) map[string]string {
	slots := int(replicas) + int(headroom)
	return map[string]string{
		ParamWalLevel:            logicalWalLevel,
		"archive_mode":           "on",
		"hot_standby_feedback":   "on",
		"sync_replication_slots": "on",
		"password_encryption":    "scram-sha-256",
		"max_replication_slots":  fmt.Sprintf("%d", slots),
		"max_wal_senders":        fmt.Sprintf("%d", slots),
		"wal_log_hints":          "on", // pg_rewind without checksums assumption
	}
}

// pickQuantity prefers a Limits entry, falls back to Requests (a common
// requests-only override), then the class default.
func pickQuantity(rr *corev1.ResourceRequirements, name corev1.ResourceName, fallback resource.Quantity) resource.Quantity {
	if v, ok := rr.Limits[name]; ok {
		return v
	}
	if v, ok := rr.Requests[name]; ok {
		return v
	}
	return fallback
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

	userParams, err := sanitizeUserParameters(in.UserParameters)
	if err != nil {
		return Rendered{}, err
	}

	cpu := resource.MustParse(profile.cpu)
	memory := resource.MustParse(profile.memory)
	replicas := profile.replicas
	sync := pgshardv1alpha1.SynchronousSpec{Mode: profile.syncMode, Number: profile.syncNumber}

	if o := in.Overrides; o != nil {
		if o.Resources != nil {
			cpu = pickQuantity(o.Resources, corev1.ResourceCPU, cpu)
			memory = pickQuantity(o.Resources, corev1.ResourceMemory, memory)
		}
		if o.ReplicasPerShard != nil {
			replicas = *o.ReplicasPerShard
		}
		if o.Synchronous != nil {
			sync = *o.Synchronous
		}
	}
	if replicas < 1 {
		return Rendered{}, fmt.Errorf("replicasPerShard (%d) must be at least 1", replicas)
	}
	if sync.Mode != SyncOff && sync.Number >= replicas {
		return Rendered{}, fmt.Errorf(
			"synchronous.number (%d) must be smaller than replicasPerShard (%d)",
			sync.Number, replicas)
	}

	memBytes := memory.Value()
	cpuCores := max(cpu.MilliValue()/1000, 1)

	// A user-overridden max_connections must drive work_mem, or per-backend
	// memory is under-divided and the aggregate blows past the memory limit.
	maxConn := profile.maxConnections
	if v, ok := userParams[ParamMaxConnections]; ok {
		if n, err := strconv.Atoi(strings.Trim(strings.TrimSpace(v), `'"`)); err == nil && n > 0 {
			maxConn = n
		}
	}

	params := map[string]string{}

	// PGTune-conventional formulas off the memory limit.
	params[ParamSharedBuffers] = mb(min(memBytes/4, sharedBuffersCap))
	params["effective_cache_size"] = mb(memBytes * 70 / 100)
	params["maintenance_work_mem"] = mb(min(memBytes/16, maintenanceCap))
	params[ParamWorkMem] = mb(max(memBytes/4/int64(maxConn), workMemFloor))
	params[ParamMaxConnections] = strconv.Itoa(maxConn)
	params["max_wal_size"] = profile.maxWalSize
	params["checkpoint_completion_target"] = "0.9"
	params[ParamRandomPageCost] = ssdRandomPageCost
	params["effective_io_concurrency"] = "256"
	params["wal_compression"] = "zstd"
	params["max_worker_processes"] = fmt.Sprintf("%d", max(cpuCores, minWorkerProcesses))
	params["max_parallel_workers"] = fmt.Sprintf("%d", cpuCores)

	// PostgreSQL 18 asynchronous I/O.
	params["io_method"] = "worker"
	params["io_workers"] = fmt.Sprintf("%d", min(max(cpuCores/2, 3), ioWorkersMax))

	maps.Copy(params, userParams)
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

// hashConfig length-prefixes every field so no key/value content (including a
// stray '=' or delimiter) can make two distinct parameter maps collide.
func hashConfig(params map[string]string, cpu, memory string) string {
	keys := slices.Sorted(maps.Keys(params))
	h := sha256.New()
	field := func(s string) { _, _ = fmt.Fprintf(h, "%d:%s", len(s), s) }
	field("cpu")
	field(cpu)
	field("memory")
	field(memory)
	for _, k := range keys {
		field(k)
		field(params[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// reloadableParameters lists the GUCs Render emits whose pg_settings.context is
// sighup/user (applied by pg_reload_conf). Every other changed key — including
// any postmaster-context GUC and any unrecognized user parameter — requires a
// restart; defaulting unknown keys to restart is the safe choice, since
// mislabeling a restart-only change as reloadable makes it silently never
// apply.
var reloadableParameters = map[string]bool{
	"effective_cache_size":         true,
	"maintenance_work_mem":         true,
	ParamWorkMem:                   true,
	"max_wal_size":                 true,
	"checkpoint_completion_target": true,
	ParamRandomPageCost:            true,
	"effective_io_concurrency":     true,
	"wal_compression":              true,
	"max_parallel_workers":         true,
	"io_workers":                   true,
	"hot_standby_feedback":         true,
	"sync_replication_slots":       true,
	"password_encryption":          true,
}

// ClassifyDiff splits changed parameters into reload-safe and
// restart-requiring sets. Unknown keys default to restart.
func ClassifyDiff(old, new map[string]string) (reload, restart []string) {
	seen := map[string]bool{}
	for k := range old {
		seen[k] = true
	}
	for k := range new {
		seen[k] = true
	}
	keys := slices.Sorted(maps.Keys(seen))
	for _, k := range keys {
		ov, oin := old[k]
		nv, nin := new[k]
		if oin == nin && ov == nv {
			continue
		}
		if reloadableParameters[k] {
			reload = append(reload, k)
		} else {
			restart = append(restart, k)
		}
	}
	return reload, restart
}
