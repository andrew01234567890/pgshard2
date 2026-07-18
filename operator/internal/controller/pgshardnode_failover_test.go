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

// The election itself (evaluateFailover) is a shared pure function exercised by
// the shard failover suite; these specs prove the node handshake wiring reads
// and writes the node's own status (targetPrimary/decisionEpoch/phase) and
// drives the epoch-guarded promote.
var _ = Describe("PgShardNode failover", func() {
	const ns = "default"

	newNode := func(name string, replicas int32) *pgshardv1alpha1.PgShardNode {
		return &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: replicas},
		}
	}

	It("elects the most-advanced replica and drives it to primary via the epoch-guarded agent", func() {
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
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		Expect(k8sClient.Create(ctx, newNode("fonode", 3))).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "fonode", Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "fonode", Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("fonode-0", primaryPodIP)
		stampPodIP("fonode-1", replicaPodIP)
		stampPodIP("fonode-2", "127.0.0.4")
		laggingAgent.SetReceivedLSN(300)
		advancedAgent.SetReceivedLSN(500) // most advanced -> elected

		reconcile()
		Expect(get().Status.CurrentPrimary).To(Equal("fonode-0"))
		Expect(advancedAgent.AppliedEpoch()).To(Equal(uint64(0))) // no promote while a ready primary exists

		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		primaryAgent.SetReady(false)

		reconcile() // election pass: epoch+target persisted, no promote yet
		elected := get()
		Expect(elected.Status.TargetPrimary).To(Equal("fonode-2"))
		Expect(elected.Status.DecisionEpoch).To(BeNumerically(">=", 1))
		Expect(elected.Status.Phase).To(Equal(pgshardv1alpha1.NodeFailingOver))
		Expect(advancedAgent.AppliedEpoch()).To(Equal(uint64(0)))

		reconcile() // promote pass
		Expect(advancedAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY))
		Expect(advancedAgent.AppliedEpoch()).To(BeNumerically(">=", uint64(1)))
		Expect(laggingAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))

		reconcile() // observe: currentPrimary recorded, commitment cleared
		got := get()
		Expect(got.Status.CurrentPrimary).To(Equal("fonode-2"))
		Expect(got.Status.TargetPrimary).To(BeEmpty())

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("records an unconfirmed role as empty, withholds readiness, and elects only once the role is confirmed", func() {
		agent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(agent.Stop)
		// The agent is up and ready but has not classified its role yet.
		agent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_UNSPECIFIED)

		dial := func(_ string, _ int32) (pgshardv1.AgentServiceClient, error) {
			return agent.Client()
		}
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		Expect(k8sClient.Create(ctx, newNode("rolenode", 1))).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rolenode", Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rolenode", Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("rolenode-0", primaryPodIP)

		reconcile()
		unconfirmed := get()
		// An unconfirmed role is recorded as empty, never silently as replica.
		Expect(unconfirmed.Status.Instances).To(HaveLen(1))
		Expect(string(unconfirmed.Status.Instances[0].Role)).To(BeEmpty())
		Expect(unconfirmed.Status.Instances[0].Ready).To(BeTrue())
		// A ready pod with an unconfirmed role neither makes the node Ready nor is
		// elected primary.
		Expect(unconfirmed.Status.Phase).NotTo(Equal(pgshardv1alpha1.NodeReady))
		Expect(unconfirmed.Status.CurrentPrimary).To(BeEmpty())
		Expect(unconfirmed.Status.TargetPrimary).To(BeEmpty())
		Expect(agent.AppliedEpoch()).To(Equal(uint64(0)))

		// Once the agent confirms the standby role, the instance is elected primary.
		agent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		reconcile() // election pass persists target + epoch
		Expect(get().Status.TargetPrimary).To(Equal("rolenode-0"))
		reconcile() // promote pass
		Expect(agent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY))

		got := get()
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("keeps a confirmed standby's replica label across a transient poll blip", func() {
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
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		Expect(k8sClient.Create(ctx, newNode("stickynode", 2))).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "stickynode", Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		labelOf := func(pod string) string {
			var p corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pod, Namespace: ns}, &p)).To(Succeed())
			return p.Labels[labelRole]
		}

		reconcile()
		stampPodIP("stickynode-0", primaryPodIP)
		stampPodIP("stickynode-1", replicaPodIP)
		reconcile()
		Expect(labelOf("stickynode-1")).To(Equal(roleLabelReplica)) // confirmed standby -> -ro

		// The replica's agent poll blips this cycle; its role is unconfirmed.
		replicaAgent.SetEmptyStatus(true)
		reconcile()
		// A single hiccup must not flap a healthy replica out of read routing.
		Expect(labelOf("stickynode-1")).To(Equal(roleLabelReplica))

		// Now the PRIMARY's poll blips. An unconfirmed ex-primary is a possible
		// writer: it must be pulled from -rw and never sticky-added to -ro, so it
		// keeps neither label — unlike the replica, whose label was replica.
		replicaAgent.SetEmptyStatus(false)
		primaryAgent.SetEmptyStatus(true)
		reconcile()
		Expect(labelOf("stickynode-1")).To(Equal(roleLabelReplica)) // replica still serves reads
		var exPrimary corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "stickynode-0", Namespace: ns}, &exPrimary)).To(Succeed())
		Expect(exPrimary.Labels).NotTo(HaveKey(labelRole)) // out of both -rw and -ro

		var got pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "stickynode", Namespace: ns}, &got)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("does not promote a merely-unreachable primary and recovers when it returns", func() {
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
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		Expect(k8sClient.Create(ctx, newNode("blipnode", 2))).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "blipnode", Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "blipnode", Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("blipnode-0", primaryPodIP)
		stampPodIP("blipnode-1", replicaPodIP)

		reconcile()
		Expect(get().Status.CurrentPrimary).To(Equal("blipnode-0"))

		// The primary's agent becomes unreachable but its pod keeps its IP — a
		// transient partition, not a demotion. No replica may be promoted.
		primaryAgent.Stop()
		reconcile()
		blipped := get()
		Expect(blipped.Status.CurrentPrimary).To(BeEmpty()) // -rw withheld (fail-safe)
		Expect(replicaAgent.Role()).To(Equal(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY))
		Expect(replicaAgent.AppliedEpoch()).To(Equal(uint64(0)))

		// The primary returns; the node recovers without ever failing over.
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
		Expect(back.Status.CurrentPrimary).To(Equal("blipnode-0"))
		Expect(replicaAgent.AppliedEpoch()).To(Equal(uint64(0)))

		Expect(k8sClient.Delete(ctx, &back)).To(Succeed())
	})
})

var _ = Describe("PgShardNode failover identity fencing", func() {
	const ns = "default"
	const idNode = "idnode"

	It("never elects a foreign-identity or divergent-timeline instance, whatever its LSN", func() {
		primaryAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(primaryAgent.Stop)
		matchingAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(matchingAgent.Stop)
		foreignAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(foreignAgent.Stop)

		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		matchingAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		foreignAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		// The foreign instance carries another database's data (reused PVC):
		// different system identifier, and the most advanced LSN — the exact
		// candidate raw LSN comparison would wrongly elect.
		foreignAgent.SetSystemID(9999)
		foreignAgent.SetReceivedLSN(500)
		matchingAgent.SetReceivedLSN(300)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			switch host {
			case primaryPodIP:
				return primaryAgent.Client()
			case replicaPodIP:
				return matchingAgent.Client()
			default:
				return foreignAgent.Client()
			}
		}
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: idNode, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 3},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: idNode, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: idNode, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP("idnode-0", primaryPodIP)
		stampPodIP("idnode-1", replicaPodIP)
		stampPodIP("idnode-2", "127.0.0.4")

		// The confirmed primary latches the lineage identity and timeline: one
		// poll confirms it as CurrentPrimary, the next latches from the
		// now-trusted claimant (an unsolicited claimant must never latch).
		reconcile()
		Expect(get().Status.SystemID).To(BeEmpty(),
			"an unconfirmed claimant must not latch the lineage identity")
		reconcile()
		got := get()
		Expect(got.Status.SystemID).To(Equal("4242"))
		Expect(got.Status.Timeline).To(Equal(int32(1)))

		// Primary dies: the election must skip the foreign instance despite
		// its higher LSN and choose the matching replica.
		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		primaryAgent.SetReady(false)
		reconcile() // election pass
		elected := get()
		Expect(elected.Status.TargetPrimary).To(Equal("idnode-1"),
			"the foreign-identity instance must never win an election")
		cond := apimeta.FindStatusCondition(elected.Status.Conditions, "IdentityConsistent")
		Expect(cond).NotTo(BeNil())
		Expect(string(cond.Status)).To(Equal("False"))
		Expect(cond.Message).To(ContainSubstring("idnode-2"))

		// A timeline AHEAD of the recorded one (a self-promoted split-brain
		// artifact) is fenced the same way: make the previously-foreign agent
		// matching in identity but ahead in timeline — still excluded.
		foreignAgent.SetSystemID(4242)
		foreignAgent.SetTimeline(9)
		reconcile()
		Expect(get().Status.TargetPrimary).To(Equal("idnode-1"))

		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
	})

	It("never publishes a sole foreign claimant as CurrentPrimary", func() {
		const rogueNode = "roguenode"
		primaryAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(primaryAgent.Stop)
		rogueAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(rogueAgent.Stop)

		primaryAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		rogueAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return primaryAgent.Client()
			}
			return rogueAgent.Client()
		}
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: rogueNode, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 2},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rogueNode, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rogueNode, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP(rogueNode+"-0", primaryPodIP)
		stampPodIP(rogueNode+"-1", replicaPodIP)
		reconcile() // confirm the legitimate primary
		reconcile() // latch its identity from the now-trusted claimant
		Expect(get().Status.SystemID).To(Equal("4242"))

		// The legitimate primary vanishes and the OTHER pod claims primary on a
		// foreign lineage (a reused volume that booted primary): it must never
		// be recognized — with the audit's original code it would become the
		// sole confirmed primary and take -rw.
		primaryAgent.SetEmptyStatus(true)
		rogueAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		rogueAgent.SetSystemID(9999)
		for range 3 {
			reconcile()
			got := get()
			Expect(got.Status.CurrentPrimary).NotTo(Equal(rogueNode+"-1"),
				"a foreign claimant must never be published as CurrentPrimary")
			Expect(got.Status.TargetPrimary).NotTo(Equal(rogueNode + "-1"))
		}
		cond := apimeta.FindStatusCondition(get().Status.Conditions, "IdentityConsistent")
		Expect(cond).NotTo(BeNil())
		Expect(string(cond.Status)).To(Equal("False"))
		Expect(cond.Message).To(ContainSubstring(rogueNode + "-1"))

		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
	})
})
