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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/agentclient"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
)

var _ = Describe("PgShardNode lifecycle", func() {
	const ns = "default"

	newNode := func(name string) *pgshardv1alpha1.PgShardNode {
		return &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardNodeSpec{
				Replicas:           2,
				PostgresConfigHash: "hash-1",
				Storage:            &pgshardv1alpha1.StorageSpec{Size: resource.MustParse("2Gi")},
			},
		}
	}

	It("creates pods, PVCs, and the service quartet with node-scoped identity", func() {
		r := &PgShardNodeReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Agents: agentclient.NewInsecurePool(),
			Images: ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
		}
		Expect(k8sClient.Create(ctx, newNode("n1"))).To(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1", Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())

		for _, suffix := range []string{"-rw", "-ro", "-r", "-pods"} { //nolint:goconst // service suffixes read clearer inline
			var svc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "n1" + suffix, Namespace: ns}, &svc)).To(Succeed(), suffix)
			if suffix == "-pods" {
				Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			}
		}
		var rw corev1.Service
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "n1-rw", Namespace: ns}, &rw)).To(Succeed())
		Expect(rw.Spec.Selector[labelRole]).To(Equal("primary"))
		Expect(rw.Spec.Selector[labelNode]).To(Equal("n1"))
		Expect(rw.Spec.Selector).NotTo(HaveKey(labelCluster), "a node's objects carry no cluster label")

		for _, name := range []string{"n1-0", "n1-1"} {
			var pod corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &pod)).To(Succeed())
			Expect(pod.Spec.InitContainers[0].Image).To(Equal(testAgentImage))
			Expect(pod.Spec.Containers[0].Command[0]).To(Equal("/pgshard/pgshard-agent"))
			Expect(pod.Spec.Containers[0].Env[0]).To(Equal(corev1.EnvVar{Name: "PGSHARD_NODE", Value: "n1"}))
			for _, e := range pod.Spec.Containers[0].Env {
				Expect(e.Name).NotTo(Equal("PGSHARD_SHARD"))
				Expect(e.Name).NotTo(Equal("PGSHARD_CLUSTER"))
			}
			Expect(pod.Annotations["pgshard.dev/config-hash"]).To(Equal("hash-1"))
			Expect(pod.Spec.Subdomain).To(Equal("n1-pods"))

			var pvc corev1.PersistentVolumeClaim
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-data", Namespace: ns}, &pvc)).To(Succeed())
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("2Gi"))
			Expect(pvc.OwnerReferences).To(BeEmpty(), "PVCs must survive node deletion")
		}

		var got pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "n1", Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.NodeProvisioning))
		Expect(got.Status.Instances).To(HaveLen(2))
	})

	It("rejects a node name that would build invalid Service names", func() {
		bad := newNode("has.dots")
		Expect(k8sClient.Create(ctx, bad)).NotTo(Succeed(), "dotted name is not a DNS label")
		long := newNode("tmp")
		long.Name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 60 chars
		Expect(k8sClient.Create(ctx, long)).NotTo(Succeed(), "over-long name overflows the 63-char Service limit")
	})

	It("does not touch pods while fenced", func() {
		r := &PgShardNodeReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Agents: agentclient.NewInsecurePool(),
			Images: ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
		}
		node := newNode("n2")
		node.Spec.Fenced = true
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n2", Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())
		var pods corev1.PodList
		Expect(k8sClient.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels{labelNode: "n2"})).To(Succeed())
		Expect(pods.Items).To(BeEmpty())
	})

	It("records the observed primary, reaches Ready, and prunes a replica on scale-down", func() {
		primary, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(primary.Stop)
		replica, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(replica.Stop)
		primary.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		replica.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_STANDBY)

		dial := func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
			if host == primaryPodIP {
				return primary.Client()
			}
			return replica.Client()
		}
		r := &PgShardNodeReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Images:    ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
			dialAgent: dial,
		}
		Expect(k8sClient.Create(ctx, newNode("n3"))).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n3", Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		get := func() pgshardv1alpha1.PgShardNode {
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "n3", Namespace: ns}, &got)).To(Succeed())
			return got
		}

		reconcile() // creates pods (no IP yet)
		stampPodIP("n3-0", primaryPodIP)
		stampPodIP("n3-1", replicaPodIP)

		reconcile()
		Expect(get().Status.CurrentPrimary).To(Equal("n3-0"))
		Expect(get().Status.Phase).To(Equal(pgshardv1alpha1.NodeReady))

		// Scale down to a single instance: the ready replica n3-1 is pruned, its
		// PVC retained for data safety.
		node := get()
		node.Spec.Replicas = 1
		Expect(k8sClient.Update(ctx, &node)).To(Succeed())
		reconcile()
		Eventually(func() bool {
			var gone corev1.Pod
			return k8sClient.Get(ctx, types.NamespacedName{Name: "n3-1", Namespace: ns}, &gone) != nil
		}, "10s", "200ms").Should(BeTrue())
		var pvc corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "n3-1-data", Namespace: ns}, &pvc)).
			To(Succeed(), "a scaled-down instance's PVC is retained")

		got := get()
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})
})
