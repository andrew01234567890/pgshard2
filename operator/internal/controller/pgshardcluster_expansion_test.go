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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

var _ = Describe("PgShardCluster expansion", func() {
	const ns = "default"

	reconcile := func(name string) {
		reconciler := &PgShardClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: ns},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	newCluster := func(name string, count int32) *pgshardv1alpha1.PgShardCluster {
		return &pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardClusterSpec{
				Postgres: pgshardv1alpha1.PostgresSpec{Version: "18"},
				Size:     &pgshardv1alpha1.SizeSpec{Class: "S"},
				Shards:   pgshardv1alpha1.ShardsSpec{InitialCount: count},
			},
		}
	}

	shardsOf := func(name string) []pgshardv1alpha1.PgShardShard {
		var list pgshardv1alpha1.PgShardShardList
		Expect(k8sClient.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
		var owned []pgshardv1alpha1.PgShardShard
		for _, s := range list.Items {
			if s.Spec.ClusterRef == name {
				owned = append(owned, s)
			}
		}
		return owned
	}

	It("expands into equal-range data shards plus the system shard", func() {
		cluster := newCluster("exp4", 4)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		reconcile("exp4")

		shards := shardsOf("exp4")
		Expect(shards).To(HaveLen(5))

		ranges := map[string]bool{}
		var system *pgshardv1alpha1.PgShardShard
		for i := range shards {
			s := shards[i]
			if s.Spec.Role == pgshardv1alpha1.ShardRoleSystem {
				system = &shards[i]
				continue
			}
			Expect(s.Spec.Serving).To(BeTrue())
			Expect(s.Spec.Replicas).To(Equal(int32(2))) // size class S
			Expect(s.Spec.PostgresConfigHash).NotTo(BeEmpty())
			Expect(s.Spec.Stanza).To(HaveSuffix("-g1"))
			ranges[s.Spec.KeyRange.Start+"-"+s.Spec.KeyRange.End] = true
		}
		Expect(system).NotTo(BeNil(), "system shard must exist")
		Expect(ranges).To(Equal(map[string]bool{
			"-40": true, "40-80": true, "80-c0": true, "c0-": true,
		}))

		// Owner references make shards garbage-collect with the cluster.
		Expect(shards[0].OwnerReferences).NotTo(BeEmpty())

		// Per-shard config map carries the rendered parameters + hash.
		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: shards[0].Name + "-postgres-config", Namespace: ns,
		}, &cm)).To(Succeed())
		Expect(cm.Data["config-hash"]).To(Equal(shards[0].Spec.PostgresConfigHash))
		Expect(cm.Data["param.wal_level"]).To(Equal("logical"))

		// Status counts.
		var got pgshardv1alpha1.PgShardCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "exp4", Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Shards.Total).To(Equal(int32(5)))
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ClusterProvisioning))
	})

	It("is idempotent and converges owned fields without touching ranges", func() {
		cluster := newCluster("exp2", 2)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		reconcile("exp2")
		first := shardsOf("exp2")
		reconcile("exp2")
		second := shardsOf("exp2")
		Expect(second).To(HaveLen(len(first)))
		for i := range first {
			Expect(second[i].Spec.KeyRange).To(Equal(first[i].Spec.KeyRange))
			Expect(second[i].UID).To(Equal(first[i].UID), "no recreation on reconcile")
		}
	})

	It("skips reconcile while paused", func() {
		cluster := newCluster("exp-paused", 2)
		cluster.Spec.Pause = true
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		reconcile("exp-paused")
		Expect(shardsOf("exp-paused")).To(BeEmpty())
	})
})
