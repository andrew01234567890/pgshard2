/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

var _ = Describe("PgShardCluster placement", func() {
	const ns = "default"

	newCluster := func(name string, placement *pgshardv1alpha1.PlacementSpec) *pgshardv1alpha1.PgShardCluster {
		return &pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardClusterSpec{
				Postgres:  pgshardv1alpha1.PostgresSpec{Version: "18"},
				Shards:    pgshardv1alpha1.ShardsSpec{InitialCount: 2},
				Placement: placement,
			},
		}
	}
	reconcile := func(name string) error {
		r := &PgShardClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: ns},
		})
		return err
	}
	nodeRefOf := func(shard string) string {
		var s pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shard, Namespace: ns}, &s)).To(Succeed())
		return s.Spec.NodeRef
	}

	It("dedicatedInstance (default) points each shard at its own node", func() {
		Expect(k8sClient.Create(ctx, newCluster("dedi", nil))).To(Succeed())
		Expect(reconcile("dedi")).To(Succeed())
		for _, shard := range []string{"dedi-min-80", "dedi-80-max", "dedi-system"} {
			Expect(nodeRefOf(shard)).To(Equal(shard), "each shard is placed on its own node")
		}
	})

	It("shared mode points every shard at one node", func() {
		Expect(k8sClient.Create(ctx, newCluster("shar",
			&pgshardv1alpha1.PlacementSpec{Mode: pgshardv1alpha1.PlacementShared}))).To(Succeed())
		Expect(reconcile("shar")).To(Succeed())
		for _, shard := range []string{"shar-min-80", "shar-80-max", "shar-system"} {
			Expect(nodeRefOf(shard)).To(Equal("shar-shared"))
		}
	})

	It("colocateWith points shards at another cluster's shared node", func() {
		Expect(k8sClient.Create(ctx, newCluster("guest", &pgshardv1alpha1.PlacementSpec{
			Mode: pgshardv1alpha1.PlacementShared, ColocateWith: "host",
		}))).To(Succeed())
		Expect(reconcile("guest")).To(Succeed())
		Expect(nodeRefOf("guest-min-80")).To(Equal("host-shared"))
	})

	It("rejects changing placement (an online move, not yet supported)", func() {
		Expect(k8sClient.Create(ctx, newCluster("immut",
			&pgshardv1alpha1.PlacementSpec{Mode: pgshardv1alpha1.PlacementDedicatedInstance}))).To(Succeed())
		var got pgshardv1alpha1.PgShardCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "immut", Namespace: ns}, &got)).To(Succeed())
		got.Spec.Placement.Mode = pgshardv1alpha1.PlacementShared
		Expect(k8sClient.Update(ctx, &got)).NotTo(Succeed(), "placement is immutable")
	})
})
