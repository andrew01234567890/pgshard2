package controller

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
)

var _ = Describe("PgShardReshard seeding", func() {
	const (
		ns       = "default"
		sourceIP = "127.0.0.31"
		targetIP = "127.0.0.32"
	)

	getReshard := func(name string) pgshardv1alpha1.PgShardReshard {
		var got pgshardv1alpha1.PgShardReshard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &got)).To(Succeed())
		return got
	}

	// makeNodeWithPrimary fabricates a Ready node whose CurrentPrimary is a
	// running pod owned by that node, and returns the pod.
	makeNodeWithPrimary := func(nodeName, ip string) *corev1.Pod {
		node := &pgshardv1alpha1.PgShardNode{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: ns},
			Spec:       pgshardv1alpha1.PgShardNodeSpec{Replicas: 1},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-0", Namespace: ns},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "pg"}},
			},
		}
		Expect(controllerutil.SetControllerReference(node, pod, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = ip
		pod.Status.Phase = corev1.PodRunning
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		node.Status.Phase = pgshardv1alpha1.NodeReady
		node.Status.CurrentPrimary = pod.Name
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		return pod
	}

	markVerified := func(shardName string, nodeName string, pod *corev1.Pod) {
		var shard pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: shardName, Namespace: ns}, &shard)).To(Succeed())
		var node pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName, Namespace: ns}, &node)).To(Succeed())
		shard.Status.DatabaseNode = nodeName
		shard.Status.DatabaseNodeUID = string(node.UID)
		shard.Status.DatabasePodUID = string(pod.UID)
		apimeta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
			Type: shardDatabaseReadyCondition, Status: metav1.ConditionTrue,
			Reason: "Provisioned", Message: "test",
		})
		Expect(k8sClient.Status().Update(ctx, &shard)).To(Succeed())
	}

	newSeedingFixture := func(reshardName string) (*fakes.FakeAgent, *fakes.FakeAgent, func() (ctrl.Result, error)) {
		sourceAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(sourceAgent.Stop)
		// gateCatchup only trusts WAL numbers from a ready unfenced PRIMARY.
		sourceAgent.SetRole(pgshardv1.InstanceRole_INSTANCE_ROLE_PRIMARY)
		targetAgent, err := fakes.NewFakeAgent()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(targetAgent.Stop)
		r := &PgShardReshardReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			dialAgent: func(host string, _ int32) (pgshardv1.AgentServiceClient, error) {
				if host == sourceIP {
					return sourceAgent.Client()
				}
				return targetAgent.Client()
			},
		}
		reconcile := func() (ctrl.Result, error) {
			return r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: reshardName, Namespace: ns},
			})
		}
		return sourceAgent, targetAgent, reconcile
	}

	// seedSetup builds the whole seeding stage: cluster + table config, source
	// shard on its own ready node, a reshard driven through provisioning, the
	// shared target node ready, and (optionally) targets marked verified.
	seedSetup := func(reshardName string, verifyTargets bool) (*fakes.FakeAgent, *fakes.FakeAgent, func() (ctrl.Result, error), *corev1.Pod) {
		clusterName := "c" + reshardName[4:]
		cl := &pgshardv1alpha1.PgShardCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardClusterSpec{
				Postgres:  pgshardv1alpha1.PostgresSpec{Version: "18"},
				Shards:    pgshardv1alpha1.ShardsSpec{InitialCount: 2},
				Placement: &pgshardv1alpha1.PlacementSpec{Mode: pgshardv1alpha1.PlacementShared},
			},
		}
		if err := k8sClient.Create(ctx, cl); err != nil {
			Expect(apimeta.IsNoMatchError(err)).To(BeFalse())
		}
		cfg := &pgshardv1alpha1.PgShardTableConfig{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-tables", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardTableConfigSpec{
				ClusterRef: clusterName,
				Tables: []pgshardv1alpha1.TableEntry{
					{Name: "orders", Type: pgshardv1alpha1.TableSharded,
						ShardKeyColumn: customerIDCol, ShardKeyType: pgshardv1alpha1.ShardKeyInt},
					{Name: "audit", Type: pgshardv1alpha1.TableGlobal},
					{Schema: "app", Name: "items", Type: pgshardv1alpha1.TableSharded,
						ShardKeyColumn: customerIDCol, ShardKeyType: pgshardv1alpha1.ShardKeyInt},
				},
			},
		}
		_ = k8sClient.Create(ctx, cfg)

		srcPod := makeNodeWithPrimary(clusterName+"-srcnode", sourceIP)
		src := &pgshardv1alpha1.PgShardShard{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-src", Namespace: ns},
			Spec: pgshardv1alpha1.PgShardShardSpec{
				ClusterRef: clusterName,
				KeyRange:   pgshardv1alpha1.KeyRange{Start: "40", End: "80"},
				Replicas:   1,
				Serving:    true,
				NodeRef:    clusterName + "-srcnode",
			},
		}
		Expect(k8sClient.Create(ctx, src)).To(Succeed())
		// Seeding copies OUT of the source database; it must be verified on
		// its current primary just like the targets.
		markVerified(src.Name, clusterName+"-srcnode", srcPod)

		reshard := &pgshardv1alpha1.PgShardReshard{
			ObjectMeta: metav1.ObjectMeta{Name: reshardName, Namespace: ns},
			Spec: pgshardv1alpha1.PgShardReshardSpec{
				ClusterRef:  clusterName,
				SourceShard: src.Name,
				TargetRanges: []pgshardv1alpha1.KeyRange{
					{Start: "40", End: "60"}, {Start: "60", End: "80"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, reshard)).To(Succeed())

		sourceAgent, targetAgent, reconcile := newSeedingFixture(reshardName)
		// Validating -> ProvisioningTargets -> Seeding.
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		_, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(getReshard(reshardName).Status.Phase).To(Equal(pgshardv1alpha1.ReshardSeeding))
		// The first Seeding reconcile PERSISTS the schema pin before any RPC.
		_, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		pinned := getReshard(reshardName)
		Expect(pinned.Status.SeedTablesPinned).To(BeTrue())
		Expect(pinned.Status.Phase).To(Equal(pgshardv1alpha1.ReshardSeeding))

		// Shared placement points targets at the cluster's shared node; make
		// it ready with a primary the fake target agent answers for.
		sharedNode := clusterName + "-shared"
		var node pgshardv1alpha1.PgShardNode
		targetPod := &corev1.Pod{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: sharedNode, Namespace: ns}, &node); err != nil {
			targetPod = makeNodeWithPrimary(sharedNode, targetIP)
		} else {
			Expect(k8sClient.Get(ctx,
				types.NamespacedName{Name: sharedNode + "-0", Namespace: ns}, targetPod)).To(Succeed())
		}
		if verifyTargets {
			for _, name := range getReshard(reshardName).Status.TargetShards {
				markVerified(name, sharedNode, targetPod)
			}
		}
		return sourceAgent, targetAgent, reconcile, targetPod
	}

	It("prepares the source, starts verified target workflows, and advances on streaming", func() {
		sourceAgent, targetAgent, reconcile, targetPod := seedSetup("rsd-seed", true)

		res, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).NotTo(Equal(ctrl.Result{}))

		got := getReshard("rsd-seed")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp),
			fmt.Sprintf("conditions: %+v", got.Status.Conditions))
		Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, "Seeded")).To(BeTrue())

		prepared := sourceAgent.PreparedSources()
		Expect(prepared).To(HaveLen(1))
		Expect(prepared[0].GetPublication()).To(HavePrefix("pgshard_rsd_seed_"))
		Expect(prepared[0].GetDatabase()).To(Equal("cseed-src"))
		// Sorted (schema, name); the global table is NOT seeded.
		Expect(prepared[0].GetTables()).To(HaveLen(2))
		Expect(prepared[0].GetTables()[0].GetSchema()).To(Equal("app"))
		Expect(prepared[0].GetTables()[0].GetName()).To(Equal("items"))
		Expect(prepared[0].GetTables()[1].GetSchema()).To(Equal("public"))
		Expect(prepared[0].GetTables()[1].GetName()).To(Equal("orders"))

		started := targetAgent.StartedWorkflows()
		Expect(started).To(HaveLen(2))
		for i, req := range started {
			spec := req.GetSpec()
			Expect(spec.GetKind()).To(Equal(pgshardv1.WorkflowKind_WORKFLOW_KIND_RESHARD))
			Expect(spec.GetPublication()).To(HavePrefix("pgshard_rsd_seed_"))
			Expect(spec.GetSlot()).To(Equal(spec.GetId()))
			Expect(spec.GetSourcePrimary().GetHost()).To(Equal(sourceIP))
			Expect(spec.GetSourcePrimary().GetDatabase()).To(Equal("cseed-src"))
			Expect(req.GetTargetPodUid()).To(Equal(string(targetPod.UID)))

			targetName := got.Status.TargetShards[i]
			var target pgshardv1alpha1.PgShardShard
			Expect(k8sClient.Get(ctx,
				types.NamespacedName{Name: targetName, Namespace: ns}, &target)).To(Succeed())
			Expect(spec.GetTargetDatabase()).To(Equal(targetName))
			// The runner refuses to truncate without this exact marker.
			Expect(spec.GetExpectProvenance()).To(Equal(string(target.UID)))
			Expect(spec.GetFilter().GetKeyRange().GetRange()).NotTo(BeNil())
		}
	})

	It("holds without touching any target while a target database is unverified", func() {
		_, targetAgent, reconcile, _ := seedSetup("rsd-unver", false)

		res, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		got := getReshard("rsd-unver")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardSeeding))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("TargetUnverified"))
		Expect(targetAgent.StartedWorkflows()).To(BeEmpty(),
			"an unverified target must never receive a truncating workflow")
	})

	It("surfaces a failed workflow and keeps seeding", func() {
		_, targetAgent, reconcile, _ := seedSetup("rsd-wferr", true)
		got := getReshard("rsd-wferr")
		uid := strings.ReplaceAll(string(got.UID), "-", "_")
		targetAgent.SetWorkflowPhase("pgshard_rsd_wferr_"+uid+"_t0",
			pgshardv1.WorkflowPhase_WORKFLOW_PHASE_ERROR, "preflight refused: provenance mismatch")

		res, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		got = getReshard("rsd-wferr")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardSeeding))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("WorkflowFailed"))
		Expect(cond.Message).To(ContainSubstring("provenance mismatch"))
	})

	It("gates CatchingUp on replication lag and advances to ReadyToCutover", func() {
		sourceAgent, targetAgent, reconcile, _ := seedSetup("rsd-lag", true)
		// Reach CatchingUp (workflows stream by fake default).
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(getReshard("rsd-lag").Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))

		got := getReshard("rsd-lag")
		uid := strings.ReplaceAll(string(got.UID), "-", "_")
		ids := []string{"pgshard_rsd_lag_" + uid + "_t0", "pgshard_rsd_lag_" + uid + "_t1"}
		// The slowest target is 32MiB behind: hold.
		sourceAgent.SetWalWriteLsn(64 << 20)
		targetAgent.SetWorkflowLsn(ids[0], 64<<20)
		targetAgent.SetWorkflowLsn(ids[1], 32<<20)
		res, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		got = getReshard("rsd-lag")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("Lagging"))

		// A streaming workflow with NO watermark cannot be measured: hold.
		targetAgent.ClearWorkflows()
		targetAgent.SetWorkflowPhase(ids[0], pgshardv1.WorkflowPhase_WORKFLOW_PHASE_STREAMING, "")
		targetAgent.SetWorkflowPhase(ids[1], pgshardv1.WorkflowPhase_WORKFLOW_PHASE_STREAMING, "")
		targetAgent.SetWorkflowLsn(ids[0], 64<<20)
		res, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		got = getReshard("rsd-lag")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))
		cond = apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond.Reason).To(Equal("LagUnmeasurable"))

		// An applied position AHEAD of the source is nonsense even when the
		// other target's minimum looks fine: hold.
		targetAgent.SetWorkflowLsn(ids[0], 128<<20)
		targetAgent.SetWorkflowLsn(ids[1], 63<<20)
		res, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		got = getReshard("rsd-lag")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))
		cond = apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond.Reason).To(Equal("LagUnmeasurable"))

		// Both targets close within the bound: advance.
		targetAgent.SetWorkflowLsn(ids[0], 64<<20)
		targetAgent.SetWorkflowLsn(ids[1], (64<<20)-(1<<20))
		_, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		got = getReshard("rsd-lag")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardReadyToCutover))
		Expect(apimeta.IsStatusConditionTrue(got.Status.Conditions, "Seeded")).To(BeTrue())
	})

	It("re-acks workflows lost to an agent restart during CatchingUp", func() {
		sourceAgent, targetAgent, reconcile, _ := seedSetup("rsd-rest", true)
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(getReshard("rsd-rest").Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))
		started := len(targetAgent.StartedWorkflows())

		// The registry is in-memory; a restarted agent knows nothing. The
		// CatchingUp pass must re-ack (recreate) rather than trust history.
		targetAgent.ClearWorkflows()
		// The re-acked workflows report applied=0; a 64MiB source position
		// keeps the pass in CatchingUp (Lagging) after the re-ack.
		sourceAgent.SetWalWriteLsn(64 << 20)
		_, err = reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(targetAgent.StartedWorkflows())).To(BeNumerically(">", started),
			"the lost workflows must be re-acked")
		got := getReshard("rsd-rest")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardCatchingUp))
	})

	It("fails closed when status.targetShards diverges from the spec-derived targets", func() {
		_, targetAgent, reconcile, _ := seedSetup("rsd-tamper", true)
		got := getReshard("rsd-tamper")
		got.Status.TargetShards[0] = "some-other-shard"
		Expect(k8sClient.Status().Update(ctx, &got)).To(Succeed())

		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		got = getReshard("rsd-tamper")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("TargetListMismatch"))
		Expect(targetAgent.StartedWorkflows()).To(BeEmpty(),
			"a tampered target list must never receive a truncating workflow")
	})

	It("pins the sharded schema and holds on drift instead of advancing", func() {
		_, _, reconcile, _ := seedSetup("rsd-drift", true)
		// First reconcile pins the schema and advances to CatchingUp; rewind
		// to Seeding to model drift discovered on a later reconcile.
		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		got := getReshard("rsd-drift")
		Expect(got.Status.SeedTablesPinned).To(BeTrue())
		Expect(got.Status.SeedTables).To(HaveLen(2))
		got.Status.Phase = pgshardv1alpha1.ReshardSeeding
		Expect(k8sClient.Status().Update(ctx, &got)).To(Succeed())

		var cfg pgshardv1alpha1.PgShardTableConfig
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Name: "cdrift-tables", Namespace: ns}, &cfg)).To(Succeed())
		cfg.Spec.Tables = append(cfg.Spec.Tables, pgshardv1alpha1.TableEntry{
			Name: "late_arrival", Type: pgshardv1alpha1.TableSharded,
			ShardKeyColumn: customerIDCol, ShardKeyType: pgshardv1alpha1.ShardKeyInt,
		})
		Expect(k8sClient.Update(ctx, &cfg)).To(Succeed())

		res, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		got = getReshard("rsd-drift")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardSeeding))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("SchemaDrift"))
	})

	It("stops seeding with zero RPCs when the source stops serving", func() {
		sourceAgent, targetAgent, reconcile, _ := seedSetup("rsd-hide", true)
		var src pgshardv1alpha1.PgShardShard
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Name: "chide-src", Namespace: ns}, &src)).To(Succeed())
		src.Spec.Serving = false
		Expect(k8sClient.Update(ctx, &src)).To(Succeed())

		_, err := reconcile()
		Expect(err).NotTo(HaveOccurred())

		got := getReshard("rsd-hide")
		Expect(got.Status.Phase).To(Equal(pgshardv1alpha1.ReshardFailed))
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("SourceNotServing"))
		Expect(sourceAgent.PreparedSources()).To(BeEmpty())
		Expect(targetAgent.StartedWorkflows()).To(BeEmpty())
	})

	It("holds while the source has no verified primary", func() {
		sourceAgent, _, reconcile, _ := seedSetup("rsd-nosrc", true)
		// Take the source node's primary away.
		var node pgshardv1alpha1.PgShardNode
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Name: "cnosrc-srcnode", Namespace: ns}, &node)).To(Succeed())
		node.Status.CurrentPrimary = ""
		Expect(k8sClient.Status().Update(ctx, &node)).To(Succeed())

		res, err := reconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		got := getReshard("rsd-nosrc")
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Seeded")
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("SourceUnready"))
		Expect(sourceAgent.PreparedSources()).To(BeEmpty())
	})
})
