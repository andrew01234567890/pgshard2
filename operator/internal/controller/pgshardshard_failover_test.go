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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
)

const (
	testPostgresImage = "pg:test"
	testAgentImage    = "agent:test"
	primaryPodIP      = "127.0.0.2"
	replicaPodIP      = "127.0.0.3"
)

var _ = Describe("PgShardShard failover", func() {
	const ns = "default"

	It("elects the most-advanced replica and drives it to primary via the epoch-guarded agent", func() {
		// Three fake agents: a primary and two standbys, so the election
		// actually exercises most-advanced selection (not a single candidate).
		primaryAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(primaryAgent.Stop)
		laggingAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(laggingAgent.Stop)
		advancedAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(advancedAgent.Stop)

		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		laggingAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		advancedAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)

		// The reconciler dials by the address we stamp on each pod; map those
		// synthetic addresses to the three fakes.
		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			switch host {
			case primaryPodIP:
				return primaryAgent.Client()
			case replicaPodIP:
				return laggingAgent.Client()
			default:
				return advancedAgent.Client()
			}
		}
		r := &PgShardShardReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}

		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: "fo", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"}, Replicas: 3,
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())

		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "fo", Namespace: ns},
			})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardShard {
			var got pgshardv1alpha1.PgShardShard
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "fo", Namespace: ns}, &got)).To(Succeed())
			return got
		}

		// First reconcile creates the pods.
		reconcile()
		stampPodIP("fo-0", primaryPodIP)
		stampPodIP("fo-1", replicaPodIP)
		stampPodIP("fo-2", "127.0.0.4")
		laggingAgent.SetReceivedLSN(300)
		advancedAgent.SetReceivedLSN(500) // most advanced -> should be elected

		// Healthy state: reconcile records the primary and — crucially — does
		// NOT promote any replica while a ready primary exists.
		reconcile()
		Expect(get().Status.CurrentPrimary).To(Equal("fo-0"))
		Expect(advancedAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))
		Expect(advancedAgent.AppliedEpoch()).To(Equal(uint64(0)))
		Expect(laggingAgent.AppliedEpoch()).To(Equal(uint64(0)))

		// Primary relinquishes the role (clean demotion) and goes not-ready.
		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		primaryAgent.SetReady(false)

		// Election pass: the decision epoch + target are persisted, but NO agent
		// is promoted yet — the epoch must be durable before it is ever used.
		reconcile()
		elected := get()
		Expect(elected.Status.TargetPrimary).To(Equal("fo-2"))
		Expect(elected.Status.DecisionEpoch).To(BeNumerically(">=", 1))
		Expect(advancedAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))
		Expect(advancedAgent.AppliedEpoch()).To(Equal(uint64(0)))

		// Promote pass: the durable epoch drives Promote on the elected replica
		// only — the lagging replica is never promoted.
		reconcile()
		Expect(advancedAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY))
		Expect(advancedAgent.AppliedEpoch()).To(BeNumerically(">=", uint64(1)))
		Expect(laggingAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))

		// Observe pass: the poll sees fo-2 as a ready primary and records
		// currentPrimary. The election commitment is cleared now the failover
		// completed, so a later failure re-elects afresh.
		reconcile()
		got := get()
		Expect(got.Status.CurrentPrimary).To(Equal("fo-2"))
		Expect(got.Status.TargetPrimary).To(BeEmpty())

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("does not promote when the primary is merely unreachable, and recovers when it returns", func() {
		primaryAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		replicaAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(replicaAgent.Stop)

		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		replicaAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return primaryAgent.Client()
			}
			return replicaAgent.Client()
		}
		r := &PgShardShardReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		const shardName = "blip"
		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: shardName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"}, Replicas: 2,
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: shardName, Namespace: ns},
			})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardShard {
			var got pgshardv1alpha1.PgShardShard
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardName, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("blip-0", primaryPodIP)
		stampPodIP("blip-1", replicaPodIP)

		// Healthy: the primary is recorded.
		reconcile()
		Expect(get().Status.CurrentPrimary).To(Equal("blip-0"))

		// The primary's agent becomes unreachable (its pod stays present, so it
		// keeps its IP) — a transient partition, NOT a demotion. It may still be
		// serving on the far side, so no replica may be promoted.
		primaryAgent.Stop()
		reconcile()
		blipped := get()
		Expect(blipped.Status.CurrentPrimary).To(BeEmpty()) // -rw withheld (fail-safe)
		Expect(blipped.Status.Phase).To(Equal(pgshardv1alpha1.ShardDegraded))
		Expect(replicaAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))
		Expect(replicaAgent.AppliedEpoch()).To(Equal(uint64(0)))

		// The primary returns; the shard recovers without ever failing over.
		recovered, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(recovered.Stop)
		recovered.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		r.dialAgent = func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return recovered.Client()
			}
			return replicaAgent.Client()
		}
		reconcile()
		back := get()
		Expect(back.Status.CurrentPrimary).To(Equal("blip-0"))
		Expect(replicaAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))
		Expect(replicaAgent.AppliedEpoch()).To(Equal(uint64(0)))

		Expect(k8sClient.Delete(ctx, &back)).To(Succeed())
	})

	It("does not abandon an in-flight promote for a tied replica that becomes ready later", func() {
		primaryAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(primaryAgent.Stop)
		earlyName, err := fakes.NewFakeAgent() // sorts first by pod name (sticky-2)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(earlyName.Stop)
		firstElected, err := fakes.NewFakeAgent() // sorts last, but ready first
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(firstElected.Stop)

		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		earlyName.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		firstElected.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			switch host {
			case primaryPodIP:
				return primaryAgent.Client()
			case replicaPodIP: // sticky-1 (earlyName, sorts first)
				return earlyName.Client()
			default: // 127.0.0.4 sticky-2 (firstElected, sorts last)
				return firstElected.Client()
			}
		}
		r := &PgShardShardReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		const shardName = "sticky"
		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: shardName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"}, Replicas: 3,
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: shardName, Namespace: ns},
			})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardShard {
			var got pgshardv1alpha1.PgShardShard
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardName, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("sticky-0", primaryPodIP)
		stampPodIP("sticky-1", replicaPodIP)
		stampPodIP("sticky-2", "127.0.0.4")
		// Both standbys hold the same LSN; sticky-1 sorts first by name. sticky-1
		// is initially not-ready, so only sticky-2 is electable this cycle.
		earlyName.SetReceivedLSN(500)
		earlyName.SetReady(false)
		firstElected.SetReceivedLSN(500)

		reconcile() // healthy
		Expect(get().Status.CurrentPrimary).To(Equal("sticky-0"))

		// Primary relinquishes; sticky-2 is the only ready candidate, so it wins.
		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		primaryAgent.SetReady(false)
		reconcile() // election pass -> TargetPrimary=sticky-2
		Expect(get().Status.TargetPrimary).To(Equal("sticky-2"))

		// The committed target goes not-ready (its normal mid-promotion window)
		// exactly as the tied, name-earlier replica becomes ready. The elected
		// target must stay pinned — a not-ready committed target must NOT be
		// abandoned for the newly-ready tied one, or both would be promoted.
		firstElected.SetReady(false)
		earlyName.SetReady(true)
		reconcile() // promote pass
		Expect(firstElected.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY))
		Expect(earlyName.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))
		Expect(earlyName.AppliedEpoch()).To(Equal(uint64(0)))
		Expect(get().Status.TargetPrimary).To(Equal("sticky-2"))

		got := get()
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("keeps driving a mid-promotion target in a two-node shard rather than stranding it", func() {
		primaryAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(primaryAgent.Stop)
		replicaAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(replicaAgent.Stop)

		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		replicaAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return primaryAgent.Client()
			}
			return replicaAgent.Client()
		}
		r := &PgShardShardReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		const shardName = "twonode"
		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: shardName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"}, Replicas: 2,
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: shardName, Namespace: ns},
			})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardShard {
			var got pgshardv1alpha1.PgShardShard
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardName, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("twonode-0", primaryPodIP)
		stampPodIP("twonode-1", replicaPodIP)

		reconcile() // healthy
		Expect(get().Status.CurrentPrimary).To(Equal("twonode-0"))

		// Primary is gone; twonode-1 is the sole ready candidate and is elected.
		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		primaryAgent.SetReady(false)
		reconcile() // election pass
		Expect(get().Status.TargetPrimary).To(Equal("twonode-1"))
		Expect(replicaAgent.AppliedEpoch()).To(Equal(uint64(0))) // not promoted yet

		// The elected target goes not-ready before its promote lands — the only
		// other instance (the dead primary) is also not-ready, so there is no
		// fresh candidate. It must still be driven, not stranded.
		replicaAgent.SetReady(false)
		reconcile() // drive pass
		Expect(replicaAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY))
		Expect(replicaAgent.AppliedEpoch()).To(BeNumerically(">=", uint64(1)))

		got := get()
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("re-elects a fresh target when a promoted primary later fails (no stale-commit deadlock)", func() {
		n0, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(n0.Stop)
		n1, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(n1.Stop)
		n2, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(n2.Stop)
		n0.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		n1.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		n2.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			switch host {
			case primaryPodIP:
				return n0.Client()
			case replicaPodIP:
				return n1.Client()
			default:
				return n2.Client()
			}
		}
		r := &PgShardShardReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		const shardName = "refail"
		shard := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: shardName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: "c", KeyRange: pgshardv1alpha1.KeyRange{End: "80"}, Replicas: 3,
			},
		}
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: shardName, Namespace: ns},
			})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardShard {
			var got pgshardv1alpha1.PgShardShard
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardName, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("refail-0", primaryPodIP)
		stampPodIP("refail-1", replicaPodIP)
		stampPodIP("refail-2", "127.0.0.4")
		n1.SetReceivedLSN(500) // both standbys caught up; refail-1 sorts first
		n2.SetReceivedLSN(500)

		reconcile() // healthy
		Expect(get().Status.CurrentPrimary).To(Equal("refail-0"))

		// First failover: refail-0 dies, refail-1 is elected and promoted.
		n0.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		n0.SetReady(false)
		reconcile() // elect
		Expect(get().Status.TargetPrimary).To(Equal("refail-1"))
		reconcile() // promote
		Expect(n1.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY))
		reconcile() // observe: commitment cleared once refail-1 is a ready primary
		Expect(get().Status.CurrentPrimary).To(Equal("refail-1"))
		Expect(get().Status.TargetPrimary).To(BeEmpty())
		// The legacy shard path latches identity and reports it, exactly like
		// the node path.
		latched := get()
		Expect(latched.Status.SystemID).To(Equal("4242"))
		idCond := apimeta.FindStatusCondition(latched.Status.Conditions, "IdentityConsistent")
		Expect(idCond).NotTo(BeNil())
		Expect(string(idCond.Status)).To(Equal("True"))

		// Second failover: the new primary's pod is removed (node failure). The
		// caught-up refail-2 must be elected — not parked forever on the stale,
		// now-gone commitment to refail-1.
		var p1 corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "refail-1", Namespace: ns}, &p1)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &p1)).To(Succeed())

		reconcile() // recreates refail-1 (no IP), elects refail-2
		Expect(get().Status.TargetPrimary).To(Equal("refail-2"))
		reconcile() // promote refail-2
		Expect(n2.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY))

		got := get()
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})
})

func stampPodIP(name, ip string) {
	const ns = "default"
	var pod corev1.Pod
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &pod)).To(Succeed())
	pod.Status.PodIP = ip
	pod.Status.Phase = corev1.PodRunning
	Expect(k8sClient.Status().Update(ctx, &pod)).To(Succeed())
}
