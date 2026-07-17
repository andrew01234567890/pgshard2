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

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
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
			case "127.0.0.2":
				return primaryAgent.Client()
			case "127.0.0.3":
				return laggingAgent.Client()
			default:
				return advancedAgent.Client()
			}
		}
		r := &PgShardShardReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: "pg:test", Agent: "agent:test"},
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
		stampPodIP("fo-0", ns, "127.0.0.2")
		stampPodIP("fo-1", ns, "127.0.0.3")
		stampPodIP("fo-2", ns, "127.0.0.4")
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

		// Observe pass: the poll sees fo-2 as primary and records currentPrimary.
		reconcile()
		got := get()
		Expect(got.Status.CurrentPrimary).To(Equal("fo-2"))
		Expect(got.Status.TargetPrimary).To(Equal("fo-2"))

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})
})

func stampPodIP(name, ns, ip string) {
	var pod corev1.Pod
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &pod)).To(Succeed())
	pod.Status.PodIP = ip
	pod.Status.Phase = corev1.PodRunning
	Expect(k8sClient.Status().Update(ctx, &pod)).To(Succeed())
}
