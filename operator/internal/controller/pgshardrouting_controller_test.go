package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
)

var _ = Describe("PgShardRouting compilation", func() {
	const ns = "default"

	reconcile := func(cluster string) {
		r := &PgShardRoutingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: cluster, Namespace: ns},
		})
		Expect(err).NotTo(HaveOccurred())
	}
	getRouting := func(cluster string) pgshardv1alpha1.PgShardRouting {
		var rt pgshardv1alpha1.PgShardRouting
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: cluster, Namespace: ns}, &rt)).To(Succeed())
		return rt
	}

	makePod := func(name, ip string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "pg"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = ip
		pod.Status.Phase = corev1.PodRunning
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	// makeShard fabricates a placed shard whose status mirrors a ready
	// primary instance (as the shard controller maintains in production).
	makeShard := func(cluster, name, start, end, pod string) {
		s := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: cluster,
				KeyRange:   pgshardv1alpha1.KeyRange{Start: start, End: end},
				Replicas:   1,
				Serving:    true,
				Role:       pgshardv1alpha1.ShardRoleData,
			},
		}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		s.Status.CurrentPrimary = pod
		s.Status.Instances = []pgshardv1alpha1.InstanceState{
			{Pod: pod, Ready: true, Role: "primary"},
		}
		Expect(k8sClient.Status().Update(ctx, s)).To(Succeed())
	}

	newCluster := func(name string) {
		cl := &pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardClusterSpec{
				Postgres: pgshardv1alpha1.PostgresSpec{Version: "18"},
				Shards:   pgshardv1alpha1.ShardsSpec{InitialCount: 2},
			},
		}
		Expect(k8sClient.Create(ctx, cl)).To(Succeed())
	}

	It("compiles routing with endpoints and bumps epochs monotonically", func() {
		newCluster("rc1")
		makePod("rc1-p0", "127.0.1.1")
		makePod("rc1-p1", "127.0.1.2")
		makeShard("rc1", "rc1-a", "", "80", "rc1-p0")
		makeShard("rc1", "rc1-b", "80", "", "rc1-p1")

		reconcile("rc1")
		rt := getRouting("rc1")
		Expect(rt.Spec.Epoch).To(Equal(int64(1)))
		Expect(rt.Spec.TopologyGeneration).To(Equal(int64(1)))
		Expect(rt.Spec.Shards).To(HaveLen(2))
		Expect(rt.Spec.Shards[0].Primary).NotTo(BeNil())
		Expect(rt.Spec.Shards[0].Primary.Host).To(Equal("127.0.1.1"))

		// The routing object must belong to the cluster for cascade cleanup.
		var cl pgshardv1alpha1.PgShardCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc1", Namespace: ns}, &cl)).To(Succeed())
		Expect(metav1.IsControlledBy(&rt, &cl)).To(BeTrue())
		Expect(apimeta.IsStatusConditionTrue(cl.Status.Conditions, "RoutingCompiled")).To(BeTrue())

		// An identical recompile must NOT bump the epoch.
		reconcile("rc1")
		Expect(getRouting("rc1").Spec.Epoch).To(Equal(int64(1)))

		// An endpoint move bumps the epoch but NOT the topology generation.
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc1-p0", Namespace: ns}, &pod)).To(Succeed())
		pod.Status.PodIP = "127.0.1.9"
		Expect(k8sClient.Status().Update(ctx, &pod)).To(Succeed())
		reconcile("rc1")
		rt = getRouting("rc1")
		Expect(rt.Spec.Epoch).To(Equal(int64(2)))
		Expect(rt.Spec.TopologyGeneration).To(Equal(int64(1)))
	})

	It("keeps the last good routing when a compile fails", func() {
		newCluster("rc2")
		makePod("rc2-p0", "127.0.2.1")
		makePod("rc2-p1", "127.0.2.2")
		makeShard("rc2", "rc2-a", "", "80", "rc2-p0")
		makeShard("rc2", "rc2-b", "80", "", "rc2-p1")
		reconcile("rc2")
		Expect(getRouting("rc2").Spec.Epoch).To(Equal(int64(1)))

		// A serving flip that breaks the keyspace partition must refuse to
		// compile and leave the published routing untouched.
		var b pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc2-b", Namespace: ns}, &b)).To(Succeed())
		b.Spec.Serving = false
		Expect(k8sClient.Update(ctx, &b)).To(Succeed())
		reconcile("rc2")

		rt := getRouting("rc2")
		Expect(rt.Spec.Epoch).To(Equal(int64(1)), "a refused compile must not publish")
		var cl pgshardv1alpha1.PgShardCluster
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc2", Namespace: ns}, &cl)).To(Succeed())
		cond := apimeta.FindStatusCondition(cl.Status.Conditions, "RoutingCompiled")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("CompileFailed"))
	})

	It("emits and withdraws a cutover gate from a CuttingOver reshard", func() {
		newCluster("rc3")
		makePod("rc3-p0", "127.0.3.1")
		makePod("rc3-p1", "127.0.3.2")
		makeShard("rc3", "rc3-a", "", "80", "rc3-p0")
		makeShard("rc3", "rc3-b", "80", "", "rc3-p1")
		reconcile("rc3")

		rs := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: "rc3-split", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:  "rc3",
				SourceShard: "rc3-a",
				TargetRanges: []pgshardv1alpha1.KeyRange{
					{Start: "", End: "40"}, {Start: "40", End: "80"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rs)).To(Succeed())
		deadline := metav1.NewTime(time.Now().Add(time.Minute).Truncate(time.Second))
		rs.Status.Phase = pgshardv1alpha1.ReshardCuttingOver
		rs.Status.CutoverGateDeadline = &deadline
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())

		reconcile("rc3")
		rt := getRouting("rc3")
		Expect(rt.Spec.Gates).To(HaveLen(1))
		Expect(rt.Spec.Gates[0].ID).To(Equal("reshard-rc3-split"))
		Expect(rt.Spec.Gates[0].Mode).To(Equal("bufferWrites"))
		Expect(rt.Spec.Gates[0].Match.KeyRanges).To(ConsistOf(
			pgshardv1alpha1.KeyRange{Start: "", End: "80"}))
		gatedEpoch := rt.Spec.Epoch

		// Leaving CuttingOver withdraws the gate with a fresh epoch.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "rc3-split", Namespace: ns}, rs)).To(Succeed())
		rs.Status.Phase = pgshardv1alpha1.ReshardCatchingUp
		Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())
		reconcile("rc3")
		rt = getRouting("rc3")
		Expect(rt.Spec.Gates).To(BeEmpty())
		Expect(rt.Spec.Epoch).To(BeNumerically(">", gatedEpoch))
	})
})
