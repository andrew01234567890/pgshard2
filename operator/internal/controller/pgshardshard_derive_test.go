package controller

import (
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

func TestDeriveShardStatus(t *testing.T) {
	const s0 = "s-0"
	inst := func(pod, role string, ready bool) pgshardv1alpha1.InstanceState {
		return pgshardv1alpha1.InstanceState{Pod: pod, Role: pgshardv1alpha1.InstanceRole(role), Ready: ready}
	}
	cases := []struct {
		name        string
		instances   []pgshardv1alpha1.InstanceState
		replicas    int32
		hadPrimary  bool
		wantPrimary string
		wantPhase   pgshardv1alpha1.ShardPhase
	}{
		{"all ready with a primary", []pgshardv1alpha1.InstanceState{inst(s0, "primary", true), inst("s-1", "replica", true)}, 2, true, s0, pgshardv1alpha1.ShardReady},
		{"over-provisioned is still ready", []pgshardv1alpha1.InstanceState{inst(s0, "primary", true), inst("s-1", "replica", true), inst("s-2", "replica", true)}, 2, true, s0, pgshardv1alpha1.ShardReady},
		{"excess ready must not mask a failed desired replica", []pgshardv1alpha1.InstanceState{inst(s0, "primary", true), inst("s-1", "replica", false), inst("s-2", "replica", true)}, 2, true, s0, pgshardv1alpha1.ShardDegraded},
		{"split brain withholds the primary", []pgshardv1alpha1.InstanceState{inst(s0, "primary", true), inst("s-1", "primary", true)}, 2, true, "", pgshardv1alpha1.ShardDegraded},
		{"primary gone clears and degrades", []pgshardv1alpha1.InstanceState{inst(s0, "replica", true), inst("s-1", "replica", true)}, 2, true, "", pgshardv1alpha1.ShardDegraded},
		{"initial bring-up provisions", []pgshardv1alpha1.InstanceState{inst(s0, "replica", false)}, 1, false, "", pgshardv1alpha1.ShardProvisioning},
		{"provisioning keeps a not-ready primary", []pgshardv1alpha1.InstanceState{inst(s0, "primary", false)}, 1, false, s0, pgshardv1alpha1.ShardProvisioning},
	}
	for _, c := range cases {
		gotPrimary, gotPhase := deriveShardStatus(c.instances, c.replicas, c.hadPrimary, "s-")
		if gotPrimary != c.wantPrimary || gotPhase != c.wantPhase {
			t.Errorf("%s: got (%q,%s) want (%q,%s)", c.name, gotPrimary, gotPhase, c.wantPrimary, c.wantPhase)
		}
	}
}

func TestOrdinalOf(t *testing.T) {
	const prefix = "shard-"
	cases := []struct {
		pod     string
		wantOrd int32
		wantOK  bool
	}{
		{prefix + "0", 0, true},
		{prefix + "5", 5, true},
		{prefix, 0, false},
		{"other-1", 0, false},
		{prefix + "x", 0, false},
	}
	for _, c := range cases {
		ord, ok := ordinalOf(c.pod, prefix)
		if ord != c.wantOrd || ok != c.wantOK {
			t.Errorf("ordinalOf(%q,%q) = (%d,%v) want (%d,%v)", c.pod, prefix, ord, ok, c.wantOrd, c.wantOK)
		}
	}
}
