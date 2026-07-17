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
)

var _ = Describe("PgShardShard pod lifecycle", func() {
	const ns = "default"

	newReconciler := func() *PgShardShardReconciler {
		return &PgShardShardReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Agents: agentclient.NewInsecurePool(),
			Images: ShardImages{
				Postgres: "ghcr.io/test/pgshard-postgres:test",
				Agent:    "ghcr.io/test/pgshard-agent:test",
			},
		}
	}

	reconcile := func(name string) {
		_, err := newReconciler().Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: ns},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	newShard := func(name string) *pgshardv1alpha1.PgShardShard {
		return &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef:         "c",
				KeyRange:           pgshardv1alpha1.KeyRange{End: "80"},
				Replicas:           2,
				Serving:            true,
				PostgresConfigHash: "hash-1",
				Storage: &pgshardv1alpha1.StorageSpec{
					Size: resource.MustParse("2Gi"),
				},
			},
		}
	}

	It("creates pods, PVCs, and the service quartet", func() {
		Expect(k8sClient.Create(ctx, newShard("sh1"))).To(Succeed())
		reconcile("sh1")

		for _, suffix := range []string{"-rw", "-ro", "-r", "-pods"} { //nolint:goconst // service suffixes read clearer inline
			var svc corev1.Service
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "sh1" + suffix, Namespace: ns}, &svc)).
				To(Succeed(), suffix)
			if suffix == "-pods" {
				Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			}
		}
		var rw corev1.Service
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "sh1-rw", Namespace: ns}, &rw)).To(Succeed())
		Expect(rw.Spec.Selector[labelRole]).To(Equal("primary"))

		for _, name := range []string{"sh1-0", "sh1-1"} {
			var pod corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &pod)).To(Succeed())
			Expect(pod.Spec.InitContainers[0].Image).To(Equal("ghcr.io/test/pgshard-agent:test"))
			Expect(pod.Spec.Containers[0].Command[0]).To(Equal("/pgshard/pgshard-agent"))
			Expect(pod.Annotations["pgshard.dev/config-hash"]).To(Equal("hash-1"))
			Expect(pod.Spec.Subdomain).To(Equal("sh1-pods"))

			var pvc corev1.PersistentVolumeClaim
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-data", Namespace: ns}, &pvc)).To(Succeed())
			Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("2Gi"))
			Expect(pvc.OwnerReferences).To(BeEmpty(), "PVCs must survive shard deletion")
		}

		var got pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "sh1", Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ShardProvisioning))
		Expect(got.Status.Instances).To(HaveLen(2))
	})

	const sh2pod0 = "sh2-0"

	It("is idempotent and recreates a deleted pod against the same PVC", func() {
		Expect(k8sClient.Create(ctx, newShard("sh2"))).To(Succeed())
		reconcile("sh2")
		reconcile("sh2")

		var pods corev1.PodList
		Expect(k8sClient.List(ctx, &pods, client.InNamespace(ns),
			client.MatchingLabels{labelShard: "sh2"})).To(Succeed())
		Expect(pods.Items).To(HaveLen(2))

		var pvcBefore corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sh2pod0 + "-data", Namespace: ns}, &pvcBefore)).To(Succeed())

		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sh2pod0, Namespace: ns}, &pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &pod, client.GracePeriodSeconds(0))).To(Succeed())
		// envtest has no kubelet: force finalization so the name frees up.
		Eventually(func() bool {
			var gone corev1.Pod
			err := k8sClient.Get(ctx, types.NamespacedName{Name: sh2pod0, Namespace: ns}, &gone)
			return err != nil
		}, "10s", "200ms").Should(BeTrue())

		reconcile("sh2")
		var recreated corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sh2pod0, Namespace: ns}, &recreated)).To(Succeed())

		var pvcAfter corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sh2pod0 + "-data", Namespace: ns}, &pvcAfter)).To(Succeed())
		Expect(pvcAfter.UID).To(Equal(pvcBefore.UID), "PVC identity must persist across pod recreation")
	})

	It("does not touch pods while fenced", func() {
		shard := newShard("sh3")
		shard.Spec.Fenced = true
		Expect(k8sClient.Create(ctx, shard)).To(Succeed())
		reconcile("sh3")
		var pods corev1.PodList
		Expect(k8sClient.List(ctx, &pods, client.InNamespace(ns),
			client.MatchingLabels{labelShard: "sh3"})).To(Succeed())
		Expect(pods.Items).To(BeEmpty())
	})
})
