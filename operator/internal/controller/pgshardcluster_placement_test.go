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
	"k8s.io/apimachinery/pkg/api/resource"
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
	getNode := func(name string) (pgshardv1alpha1.PgShardNode, bool) {
		var n pgshardv1alpha1.PgShardNode
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &n)
		return n, err == nil
	}

	It("dedicatedInstance (default) creates a node per shard and points each shard at it", func() {
		Expect(k8sClient.Create(ctx, newCluster("dedi", nil))).To(Succeed())
		Expect(reconcile("dedi")).To(Succeed())
		for _, shard := range []string{"dedi-min-80", "dedi-80-max", "dedi-system"} {
			node, ok := getNode(shard)
			Expect(ok).To(BeTrue(), "node "+shard)
			Expect(node.OwnerReferences).NotTo(BeEmpty(), "node is cluster-owned")
			Expect(nodeRefOf(shard)).To(Equal(shard))
		}
		_, ok := getNode("dedi-shared")
		Expect(ok).To(BeFalse(), "dedicated mode creates no shared node")
	})

	It("shared mode creates one node hosting every shard database", func() {
		Expect(k8sClient.Create(ctx, newCluster("shar",
			&pgshardv1alpha1.PlacementSpec{Mode: pgshardv1alpha1.PlacementShared}))).To(Succeed())
		Expect(reconcile("shar")).To(Succeed())
		_, ok := getNode("shar-shared")
		Expect(ok).To(BeTrue())
		_, perShard := getNode("shar-min-80")
		Expect(perShard).To(BeFalse(), "shared mode creates no per-shard node")
		for _, shard := range []string{"shar-min-80", "shar-80-max", "shar-system"} {
			Expect(nodeRefOf(shard)).To(Equal("shar-shared"))
		}
	})

	It("colocateWith reuses another cluster's shared node and creates none of its own", func() {
		Expect(k8sClient.Create(ctx, newCluster("host",
			&pgshardv1alpha1.PlacementSpec{Mode: pgshardv1alpha1.PlacementShared}))).To(Succeed())
		Expect(reconcile("host")).To(Succeed())
		_, ok := getNode("host-shared")
		Expect(ok).To(BeTrue())

		Expect(k8sClient.Create(ctx, newCluster("guest", &pgshardv1alpha1.PlacementSpec{
			Mode: pgshardv1alpha1.PlacementShared, ColocateWith: "host",
		}))).To(Succeed())
		Expect(reconcile("guest")).To(Succeed())
		_, own := getNode("guest-shared")
		Expect(own).To(BeFalse(), "guest owns no node; it reuses host's")
		Expect(nodeRefOf("guest-min-80")).To(Equal("host-shared"))
	})

	It("colocateWith errors until the target's shared node exists", func() {
		Expect(k8sClient.Create(ctx, newCluster("orphan", &pgshardv1alpha1.PlacementSpec{
			Mode: pgshardv1alpha1.PlacementShared, ColocateWith: "absent",
		}))).To(Succeed())
		Expect(reconcile("orphan")).NotTo(Succeed())
	})

	It("plumbs the cluster's requested storage into the nodes it creates", func() {
		cluster := newCluster("stor", nil)
		cluster.Spec.Size = &pgshardv1alpha1.SizeSpec{
			Overrides: &pgshardv1alpha1.SizeOverrides{
				Storage: &pgshardv1alpha1.StorageSpec{Size: resource.MustParse("50Gi")},
			},
		}
		cluster.Spec.System = &pgshardv1alpha1.SystemSpec{
			Storage: &pgshardv1alpha1.StorageSpec{Size: resource.MustParse("5Gi")},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		Expect(reconcile("stor")).To(Succeed())

		data, ok := getNode("stor-min-80")
		Expect(ok).To(BeTrue())
		Expect(data.Spec.Storage).NotTo(BeNil())
		Expect(data.Spec.Storage.Size.String()).To(Equal("50Gi"), "data node uses the data storage")
		system, ok := getNode("stor-system")
		Expect(ok).To(BeTrue())
		Expect(system.Spec.Storage).NotTo(BeNil())
		Expect(system.Spec.Storage.Size.String()).To(Equal("5Gi"), "system node uses the system storage")
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
