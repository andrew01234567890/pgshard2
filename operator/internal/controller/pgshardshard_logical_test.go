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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
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
		// Routing reads CurrentPrimary: it stays withheld until the shard's OWN
		// database on this node is verified (no agent here, so never).
		Expect(got.Status.CurrentPrimary).To(BeEmpty(),
			"a placed shard is not routable before its database is verified")

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

	It("provisions its Postgres database on the node's primary once ready", func() {
		agent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(agent.Stop)

		const dbNode = "dbnode"
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: dbNode, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		node.Status.Phase = pgshardv1alpha1.NodeReady
		node.Status.CurrentPrimary = dbNode + "-0"
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		// The primary pod must have an address for the controller to dial.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: dbNode + "-0", Namespace: ns},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "postgres", Image: "pg"},
			}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.9"
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		const dbShard = "dbshard"
		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: dbShard, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"},
				Replicas: 1, NodeRef: dbNode,
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())

		r := &PgShardShardReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			dialAgent: func(string, int32) (pgshardv1.AgentServiceClient, error) {
				return agent.Client()
			},
		}
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: dbShard, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())

		Expect(agent.Databases()).To(HaveKey(dbShard), "the shard's database was created on the node")

		var got pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: dbShard, Namespace: ns}, &got)).To(Succeed())
		Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, shardDatabaseReadyCondition)).
			To(BeTrue(), "the DatabaseReady condition is set")
		Expect(agent.DatabaseProvenance(dbShard)).To(Equal(string(got.UID)),
			"the database is stamped with this placement's identity")

		// A second reconcile is a no-op: the recorded node identity short-circuits
		// the call.
		callsBefore := len(agent.Calls)
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: dbShard, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())
		Expect(agent.Calls).To(HaveLen(callsBefore), "no further CreateDatabase once provisioned")

		var provisioned pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: dbShard, Namespace: ns}, &provisioned)).To(Succeed())
		Expect(provisioned.Status.DatabaseNode).To(Equal(dbNode))
		Expect(provisioned.Status.CurrentPrimary).To(Equal(dbNode+"-0"),
			"a verified database makes the shard routable")

		// A recreated same-named node is a NEW incarnation: its storage has
		// never been verified, so the latch must not carry over — the shard
		// re-provisions (and re-verifies provenance) against the new node.
		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
		fresh := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: dbNode, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, fresh)).To(Succeed())
		fresh.Status.Phase = pgshardv1alpha1.NodeReady
		fresh.Status.CurrentPrimary = dbNode + "-0"
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		callsBefore = len(agent.Calls)
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: dbShard, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(agent.Calls)).To(BeNumerically(">", callsBefore),
			"a recreated node incarnation must be re-verified, not trusted by name")
	})

	It("fences a same-named database left by another placement and adopts only on explicit authorization", func() {
		agent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(agent.Stop)

		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: "stalenode", Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		node.Status.Phase = pgshardv1alpha1.NodeReady
		node.Status.CurrentPrimary = "stalenode-0"
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "stalenode-0", Namespace: ns},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "postgres", Image: "pg"},
			}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.9"
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		// A retained database from an earlier placement: same name, different
		// (stale, partially-seeded) contents.
		const staleShard = "staleshard"
		agent.SeedDatabase(staleShard, "app", "prior-placement-uid")

		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: staleShard, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"},
				Replicas: 1, NodeRef: "stalenode",
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())

		r := &PgShardShardReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			dialAgent: func(string, int32) (pgshardv1.AgentServiceClient, error) {
				return agent.Client()
			},
		}
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: staleShard, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}

		reconcile()
		var got pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: staleShard, Namespace: ns}, &got)).To(Succeed())
		cond := apimeta.FindStatusCondition(got.Status.Conditions, shardDatabaseReadyCondition)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("ForeignDatabase"))
		Expect(cond.Message).To(ContainSubstring(adoptDatabaseAnnotation))
		Expect(agent.DatabaseProvenance(staleShard)).To(Equal("prior-placement-uid"),
			"the foreign marker is untouched")
		// A refused database must not be served: the routing view reads
		// CurrentPrimary, so it is withheld and the shard reads Degraded.
		Expect(got.Status.CurrentPrimary).To(BeEmpty(),
			"a shard whose database was refused must not be routable")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ShardDegraded))

		// Explicit adoption: a deliberate human/restore action, never routine.
		got.Annotations = map[string]string{adoptDatabaseAnnotation: "true"}
		Expect(k8sClient.Update(ctx, &got)).To(Succeed())
		reconcile()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: staleShard, Namespace: ns}, &got)).To(Succeed())
		Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, shardDatabaseReadyCondition)).To(BeTrue())
		Expect(agent.DatabaseProvenance(staleShard)).To(Equal(string(got.UID)),
			"adoption re-stamps the marker with this placement's identity")
		Expect(got.Status.CurrentPrimary).To(Equal("stalenode-0"),
			"an adopted database makes the shard routable again")
		// Adoption is ONE-SHOT: left standing, the annotation would silently
		// adopt any stale same-named database on the shard's NEXT node too.
		Expect(got.Annotations).NotTo(HaveKey(adoptDatabaseAnnotation),
			"the adopt annotation is consumed by a successful adoption")
	})

	It("marks an overlong database name terminal without calling the agent or wedging the mirror", func() {
		agent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(agent.Stop)

		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: "bignode", Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		node.Status.Phase = pgshardv1alpha1.NodeReady
		node.Status.CurrentPrimary = "bignode-0"
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())

		// A shard name over PostgreSQL's 63-byte identifier limit.
		longName := strings.Repeat("a", 64)
		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: longName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"},
				Replicas: 1, NodeRef: "bignode",
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())

		r := &PgShardShardReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			dialAgent: func(string, int32) (pgshardv1.AgentServiceClient, error) {
				return agent.Client()
			},
		}
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: longName, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred(), "a bad name is terminal, not a hard error")

		Expect(agent.Calls).To(BeEmpty(), "the agent is never called for a too-long name")

		var got pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: longName, Namespace: ns}, &got)).To(Succeed())
		cond := apimeta.FindStatusCondition(got.Status.Conditions, shardDatabaseReadyCondition)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("InvalidName"))
		// A shard whose database can never be created is not routable and not
		// healthy - Degraded with no published primary is the honest signal.
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ShardDegraded))
		Expect(got.Status.CurrentPrimary).To(BeEmpty())
	})
})
