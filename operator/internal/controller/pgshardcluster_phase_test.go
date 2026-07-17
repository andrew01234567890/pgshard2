package controller

import (
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

func TestClusterPhase(t *testing.T) {
	cases := []struct {
		name                   string
		ready, degraded, total int32
		want                   pgshardv1alpha1.ClusterPhase
	}{
		{"no shards yet", 0, 0, 0, pgshardv1alpha1.ClusterProvisioning},
		{"still coming up", 1, 0, 3, pgshardv1alpha1.ClusterProvisioning},
		{"all ready", 3, 0, 3, pgshardv1alpha1.ClusterReady},
		{"one degraded demotes", 2, 1, 3, pgshardv1alpha1.ClusterDegraded},
		{"all degraded", 0, 3, 3, pgshardv1alpha1.ClusterDegraded},
	}
	for _, c := range cases {
		if got := clusterPhase(c.ready, c.degraded, c.total); got != c.want {
			t.Errorf("%s: clusterPhase(%d,%d,%d)=%q want %q", c.name, c.ready, c.degraded, c.total, got, c.want)
		}
	}
}
