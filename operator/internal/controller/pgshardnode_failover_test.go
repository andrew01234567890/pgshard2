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
		// The most advanced LSN sits on the instance that will turn foreign —
		// the exact candidate raw LSN comparison would wrongly elect.
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

		// idnode-2's volume is swapped for another database's (a reused PVC):
		// different system identifier, highest LSN. Then the primary dies: the
		// election must skip the foreign instance and choose the matching
		// replica.
		foreignAgent.SetSystemID(9999)
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
		// The foreign instance also loses its replica label: its reads are
		// another database's data, so -ro must not route to it — including
		// through the sticky-label blip branch.
		var fencedPod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "idnode-2", Namespace: ns}, &fencedPod)).To(Succeed())
		Expect(fencedPod.Labels).NotTo(HaveKey(labelRole),
			"a fenced instance must be pulled out of read routing")

		// A timeline AHEAD of the recorded one (a self-promoted split-brain
		// artifact) is fenced the same way: make the previously-foreign agent
		// matching in identity but ahead in timeline — still excluded.
		foreignAgent.SetSystemID(4242)
		foreignAgent.SetTimeline(9)
		reconcile()
		Expect(get().Status.TargetPrimary).To(Equal("idnode-1"))

		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
	})

	It("neither publishes nor latches a claimant while bootstrap identities conflict", func() {
		const cfNode = "conflictnode"
		claimant, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(claimant.Stop)
		standby, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(standby.Stop)

		// A fresh node whose pod 0 booted from a reused foreign volume and
		// reports primary unsolicited, while pod 1 reports a different id.
		// Without the conflict gate the first poll would publish pod 0 as
		// CurrentPrimary and the second would trust and latch 9999 — silently
		// resolving the dispute in the intruder's favor.
		claimant.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		claimant.SetSystemID(9999)
		standby.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return claimant.Client()
			}
			return standby.Client()
		}
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: cfNode, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 2},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: cfNode, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cfNode, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP(cfNode+"-0", primaryPodIP)
		stampPodIP(cfNode+"-1", replicaPodIP)

		for range 3 {
			reconcile()
			got := get()
			Expect(got.Status.CurrentPrimary).To(BeEmpty(),
				"no primary may be published while identities conflict")
			Expect(got.Status.SystemID).To(BeEmpty(),
				"no lineage may latch out of an unresolved conflict")
			Expect(got.Status.TargetPrimary).To(BeEmpty())
		}
		cond := apimeta.FindStatusCondition(get().Status.Conditions, "IdentityConsistent")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("IdentityConflict"))

		// The intruder is rebuilt onto the right lineage: the conflict clears,
		// the claimant is published, and the identity latches from it.
		claimant.SetSystemID(4242)
		reconcile()
		Expect(get().Status.CurrentPrimary).To(Equal(cfNode + "-0"))
		reconcile()
		Expect(get().Status.SystemID).To(Equal("4242"))

		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
	})

	It("strips read routing and gates the election while bootstrap identities conflict", func() {
		const csNode = "conflictstandbys"
		a, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(a.Stop)
		b, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(b.Stop)
		// Not ready yet, so the agreeing phase confirms roles (and applies
		// replica labels) without an election committing to anything.
		a.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		b.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		a.SetReady(false)
		b.SetReady(false)
		a.SetReceivedLSN(100)
		b.SetReceivedLSN(500)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return a.Client()
			}
			return b.Client()
		}
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		Expect(k8sClient.Create(ctx, &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: csNode, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 2},
		})).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: csNode, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: csNode, Namespace: ns}, &got)).To(Succeed())
			return got
		}
		labelOf := func(pod string) string {
			var p corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pod, Namespace: ns}, &p)).To(Succeed())
			return p.Labels[labelRole]
		}

		reconcile()
		stampPodIP(csNode+"-0", primaryPodIP)
		stampPodIP(csNode+"-1", replicaPodIP)
		reconcile()
		Expect(labelOf(csNode+"-1")).To(Equal(roleLabelReplica), "agreeing standbys serve reads")

		// Pod 1's volume is swapped for another lineage's before any identity
		// latched, and both standbys become ready — the exact snapshot an
		// unfenced election would elect from. Either read is possibly wrong
		// data and electing by raw LSN could promote the intruder, so BOTH
		// routing and the election must stop.
		b.SetSystemID(9999)
		a.SetReady(true)
		b.SetReady(true)
		for range 2 {
			reconcile()
			got := get()
			Expect(got.Status.TargetPrimary).To(BeEmpty(),
				"no election may run across conflicting identities")
			Expect(got.Status.CurrentPrimary).To(BeEmpty())
		}
		Expect(labelOf(csNode+"-0")).To(BeEmpty(), "conflicting standbys lose read routing")
		Expect(labelOf(csNode + "-1")).To(BeEmpty())

		// The intruder is rebuilt onto the right lineage: the gate lifts and
		// the election proceeds normally.
		b.SetSystemID(4242)
		reconcile()
		Expect(get().Status.TargetPrimary).To(Equal(csNode+"-1"),
			"once identities agree, the most-advanced standby is electable again")

		got := get()
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("suppresses primary publication while a same-lineage rogue claimant lives", func() {
		const sbNode = "suppressnode"
		legit, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(legit.Stop)
		rogue, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(rogue.Stop)
		legit.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		rogue.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return legit.Client()
			}
			return rogue.Client()
		}
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		Expect(k8sClient.Create(ctx, &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: sbNode, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 2},
		})).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: sbNode, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sbNode, Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile()
		stampPodIP(sbNode+"-0", primaryPodIP)
		stampPodIP(sbNode+"-1", replicaPodIP)
		reconcile() // confirm the primary
		reconcile() // latch from the now-trusted claimant
		Expect(get().Status.CurrentPrimary).To(Equal(sbNode + "-0"))
		Expect(get().Status.SystemID).To(Equal("4242"))

		// Pod 1 self-promotes (same lineage, timeline ahead): a genuine
		// split-brain that may be absorbing writes. NEITHER claimant may be
		// published until it is resolved — the trusted one included, because
		// clients cannot be split between two live primaries.
		rogue.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		rogue.SetTimeline(9)
		reconcile()
		suppressed := get()
		Expect(suppressed.Status.CurrentPrimary).To(BeEmpty(),
			"no primary is published while a same-lineage rogue claimant lives")
		Expect(suppressed.Status.Phase).To(Equal(pgshardv1alpha1.NodeDegraded))

		// The rogue demotes back to a standby: still on an ahead timeline it
		// stays fenced as a blocker, but the legitimate primary — matching id
		// on exactly the recorded timeline — is published again.
		rogue.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)
		reconcile()
		recovered := get()
		Expect(recovered.Status.CurrentPrimary).To(Equal(sbNode + "-0"))

		Expect(k8sClient.Delete(ctx, &recovered)).To(Succeed())
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
