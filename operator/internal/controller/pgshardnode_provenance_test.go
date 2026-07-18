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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

var _ = Describe("PgShardNode storage provenance", func() {
	const ns = "default"

	It("stamps new PVCs with the node's identity", func() {
		r := &PgShardNodeReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			Images: ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
		}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: "stampnode", Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "stampnode", Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())

		var pvc corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "stampnode-0-data", Namespace: ns}, &pvc)).To(Succeed())
		Expect(pvc.Labels[labelNodeUID]).To(Equal(string(node.UID)))

		Expect(k8sClient.Delete(ctx, node)).To(Succeed())
	})

	It("never mounts a retained PVC from another node identity, and recovers on explicit relabel", func() {
		const nodeName = "provnode"
		// A retained volume with a deterministic name: what a deleted node (or a
		// pre-provenance operator) leaves behind. First unlabeled, then wrongly
		// labeled — both must fence.
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-0-data", Namespace: ns},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())

		r := &PgShardNodeReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			Images: ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
		}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeName, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred(), "a foreign PVC degrades the node, it is not a reconcile error")
		}
		expectFenced := func() {
			var pod corev1.Pod
			err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0", Namespace: ns}, &pod)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"the pod that would mount the foreign volume must never be created")
			var got pgshardv1alpha1.PgShardNode
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &got)).To(Succeed())
			cond := apimeta.FindStatusCondition(got.Status.Conditions, storageProvenanceCondition)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("ForeignData"))
			Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.NodeDegraded))
		}

		reconcile()
		expectFenced() // unlabeled: identity unknown is not identity confirmed

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0-data", Namespace: ns}, pvc)).To(Succeed())
		pvc.Labels = map[string]string{labelNode: nodeName, labelNodeUID: "some-other-node-uid"}
		Expect(k8sClient.Update(ctx, pvc)).To(Succeed())
		reconcile()
		expectFenced() // wrong identity: another lineage's data

		// The explicit adoption path: a human relabels the volume to this node.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0-data", Namespace: ns}, pvc)).To(Succeed())
		pvc.Labels[labelNodeUID] = string(node.UID)
		Expect(k8sClient.Update(ctx, pvc)).To(Succeed())
		reconcile()

		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0", Namespace: ns}, &pod)).To(Succeed())
		var got pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &got)).To(Succeed())
		cond := apimeta.FindStatusCondition(got.Status.Conditions, storageProvenanceCondition)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal("Verified"))

		// The volume turns foreign UNDER a running, routed pod (a bad relabel,
		// or a pre-provenance upgrade): fencing must pull the pod out of
		// routing, not just block future pod creation.
		labeled := pod.DeepCopy()
		labeled.Labels[labelRole] = roleLabelPrimary
		Expect(k8sClient.Patch(ctx, labeled, client.MergeFrom(&pod))).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0-data", Namespace: ns}, pvc)).To(Succeed())
		pvc.Labels[labelNodeUID] = "hijacked-by-another-node"
		Expect(k8sClient.Update(ctx, pvc)).To(Succeed())
		reconcile()
		var refetched corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0", Namespace: ns}, &refetched)).To(Succeed())
		Expect(refetched.Labels).NotTo(HaveKey(labelRole),
			"a running pod on a foreign volume must be pulled out of routing")
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.NodeDegraded))

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})
})
