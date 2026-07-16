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
max_worker_processes=1
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
	if r.Parameters[ParamWalLevel] != "logical" {
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
	if r.Parameters[ParamSharedBuffers] != "2048MB" {
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
	new := map[string]string{ParamWorkMem: "20MB", ParamSharedBuffers: "2048MB", ParamRandomPageCost: ssdRandomPageCost, "io_workers": "4"}
	reload, restart := ClassifyDiff(old, new)
	if fmt.Sprint(reload) != "[io_workers work_mem]" {
		t.Errorf("reload = %v", reload)
	}
	if fmt.Sprint(restart) != "[shared_buffers]" {
		t.Errorf("restart = %v", restart)
	}
}
