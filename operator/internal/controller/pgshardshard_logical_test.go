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

var _ = Describe("PgShardShard placed on a node", func() {
	const (
		ns      = "default"
		shardNm = "logshard"
	)

	It("mirrors its node's health and creates no physical objects", func() {
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: "gatenode", Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 2},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		node.Status.Phase = pgshardv1alpha1.NodeReady
		node.Status.CurrentPrimary = "gatenode-0"
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: shardNm, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c",
				KeyRange:   pgshardv1alpha1.KeyRange{End: "80"},
				Replicas:   2,
				NodeRef:    "gatenode",
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())

		// The reconciler needs no agent/image config: a placed shard returns via
		// the logical path before any physical reconcile.
		r := &PgShardShardReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: shardNm, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())

		var got pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardNm, Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ShardReady), "phase mirrors the node")
		Expect(got.Status.CurrentPrimary).To(Equal("gatenode-0"))

		var pods corev1.PodList
		Expect(k8sClient.List(ctx, &pods, client.InNamespace(ns),
			client.MatchingLabels{labelShard: shardNm})).To(Succeed())
		Expect(pods.Items).To(BeEmpty(), "a placed shard creates no pods; its node does")
		var svcs corev1.ServiceList
		Expect(k8sClient.List(ctx, &svcs, client.InNamespace(ns),
			client.MatchingLabels{labelShard: shardNm})).To(Succeed())
		Expect(svcs.Items).To(BeEmpty(), "and no services")

		// A shard-level fence flag is meaningless for a placed shard (fencing is
		// the node's action); it must not stop the status mirror.
		got.Spec.Fenced = true
		Expect(k8sClient.Update(ctx, &got)).To(Succeed())
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: shardNm, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardNm, Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ShardReady), "a fenced placed shard still mirrors its node")
	})
})
