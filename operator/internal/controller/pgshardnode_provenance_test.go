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
	"context"
	"errors"
	"fmt"

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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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

	It("fences every foreign ordinal in one pass, not just the first", func() {
		const nodeName = "multinode"
		r := &PgShardNodeReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			Images: ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
		}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 2},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeName, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		reconcile() // healthy bring-up: pods + stamped PVCs for both ordinals

		// Both pods are serving (a primary and a replica) when BOTH volumes
		// turn foreign: stopping at ordinal 0 would leave ordinal 1's primary
		// labeled — and routed — until ordinal 0 was remediated.
		for i, role := range []string{roleLabelReplica, roleLabelPrimary} {
			podName := fmt.Sprintf("%s-%d", nodeName, i)
			var pod corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: ns}, &pod)).To(Succeed())
			labeled := pod.DeepCopy()
			labeled.Labels[labelRole] = role
			Expect(k8sClient.Patch(ctx, labeled, client.MergeFrom(&pod))).To(Succeed())

			var pvc corev1.PersistentVolumeClaim
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName + "-data", Namespace: ns}, &pvc)).To(Succeed())
			pvc.Labels[labelNodeUID] = "some-other-node"
			Expect(k8sClient.Update(ctx, &pvc)).To(Succeed())
		}
		reconcile()
		for i := range 2 {
			var pod corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: fmt.Sprintf("%s-%d", nodeName, i), Namespace: ns}, &pod)).To(Succeed())
			Expect(pod.Labels).NotTo(HaveKey(labelRole),
				"every foreign ordinal must be pulled out of routing in the same pass")
		}
		var got pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &got)).To(Succeed())
		cond := apimeta.FindStatusCondition(got.Status.Conditions, storageProvenanceCondition)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Message).To(ContainSubstring(nodeName + "-0-data"))
		Expect(cond.Message).To(ContainSubstring(nodeName + "-1-data"))

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("verifies the claims a pod actually mounts, not just the desired layout", func() {
		const nodeName = "walnode"
		r := &PgShardNodeReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			Images: ShardImages{Postgres: testPostgresImage, Agent: testAgentImage},
		}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardNodeSpec{
				Replicas: 1,
				Storage: &pgshardv1alpha1.StorageSpec{
					Size: resource.MustParse("1Gi"), WalSeparate: true,
				},
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeName, Namespace: ns}})
			Expect(err).NotTo(HaveOccurred())
		}
		reconcile() // pod mounts data AND wal claims

		// The desired layout drops the separate WAL volume, but the running
		// pod still MOUNTS it: the mounted claim turning foreign must still
		// degrade the node — checking only the desired names would miss it.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, node)).To(Succeed())
		node.Spec.Storage.WalSeparate = false
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		var wal corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0-wal", Namespace: ns}, &wal)).To(Succeed())
		wal.Labels[labelNodeUID] = "someone-else"
		Expect(k8sClient.Update(ctx, &wal)).To(Succeed())
		reconcile()
		var got pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.NodeDegraded))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, storageProvenanceCondition)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Message).To(ContainSubstring(nodeName + "-0-wal"))

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("backfills the incarnation label on pods created before it existed", func() {
		const nodeName = "oldpodnode"
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
			Expect(err).NotTo(HaveOccurred())
		}
		reconcile()

		// Simulate a pod from before the incarnation label existed (operator
		// upgrade): the uid selector would silently drop it from every
		// service, so — its owner UID already proving the incarnation — the
		// label is backfilled instead.
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0", Namespace: ns}, &pod)).To(Succeed())
		stripped := pod.DeepCopy()
		delete(stripped.Labels, labelNodeUID)
		Expect(k8sClient.Patch(ctx, stripped, client.MergeFrom(&pod))).To(Succeed())
		reconcile()
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName + "-0", Namespace: ns}, &pod)).To(Succeed())
		Expect(pod.Labels[labelNodeUID]).To(Equal(string(node.UID)),
			"a pre-upgrade pod must be backfilled, not silently unrouted")

		var got pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &got)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("creates no pod at all while any ordinal's storage is foreign", func() {
		// Pods are born with a replica role label: creating ordinal 1's pod
		// while ordinal 0 is foreign would put an unverified instance behind
		// -ro. The preflight must block ALL pod creation, not just ordinal 0's.
		const nodeName = "prenode"
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
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 2},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeName, Namespace: ns}})
		Expect(err).NotTo(HaveOccurred())
		for i := range 2 {
			var pod corev1.Pod
			getErr := k8sClient.Get(ctx, types.NamespacedName{
				Name: fmt.Sprintf("%s-%d", nodeName, i), Namespace: ns}, &pod)
			Expect(apierrors.IsNotFound(getErr)).To(BeTrue(),
				"no ordinal's pod may exist while any storage is foreign")
		}
		var got pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &got)).To(Succeed())
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.NodeDegraded))

		Expect(k8sClient.Delete(ctx, &got)).To(Succeed())
	})

	It("does not treat a concurrently-created (unverified) PVC as mountable", func() {
		// A stale informer can miss an existing claim: Get says NotFound, the
		// live Create answers AlreadyExists. That claim has NOT been verified,
		// so the reconcile must stop before any pod would mount it.
		base := fake.NewClientBuilder().WithScheme(k8sClient.Scheme()).Build()
		intercepted := interceptor.NewClient(base, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return apierrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return apierrors.NewAlreadyExists(corev1.Resource("persistentvolumeclaims"), obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
		})
		r := &PgShardNodeReconciler{Client: intercepted, Scheme: k8sClient.Scheme()}
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: "stalecache", Namespace: ns, UID: "uid-1"},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		err := r.ensureInstance(context.Background(), node, 0)
		Expect(err).To(HaveOccurred(), "an unverified concurrent claim must stop the reconcile")
		var foreign *foreignPVCError
		Expect(errors.As(err, &foreign)).To(BeFalse(),
			"a 409 is transient (retry re-verifies), not a foreign-data verdict")
		var pods corev1.PodList
		Expect(base.List(context.Background(), &pods)).To(Succeed())
		Expect(pods.Items).To(BeEmpty(), "no pod may be created against an unverified claim")
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
