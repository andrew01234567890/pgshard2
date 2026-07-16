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
	"cmp"
	"context"
	"slices"
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
)

// PendingFailoverMarker is written to targetPrimary while the old primary
// is being signalled to step down and before the most-advanced replica is
// elected — the CNPG two-phase handshake. Nothing promotes while this is
// the target.
const PendingFailoverMarker = "__pending__"

// evaluateFailover decides, from the polled instance statuses, whether a
// failover is warranted and what the new target should be. It is a pure
// function so the decision is unit-testable without a live cluster.
//
// Rules (CNPG-derived):
//   - A failover is warranted when the shard has instances but none of them
//     is a ready primary.
//   - The elected target is the ready replica with the highest received
//     WAL position (most advanced), breaking ties by pod name for
//     determinism.
//   - Until every WAL receiver is quiet we do not elect: electing a
//     laggard could lose shipped-but-unreceived WAL. That gate is expressed
//     by walReceiversQuiet.
type failoverDecision struct {
	warranted     bool
	targetPrimary string
	// waitReceivers is true when a failover is warranted but we must first
	// see WAL receivers drain — the caller parks on PendingFailoverMarker.
	waitReceivers bool
}

type instanceView struct {
	pod         string
	host        string
	ready       bool
	isPrimary   bool
	receivedLSN uint64
	walReceiver bool
}

func evaluateFailover(instances []instanceView) failoverDecision {
	hasReadyPrimary := false
	// Candidates: ready replicas, most-advanced first, deterministic ties.
	candidates := make([]instanceView, 0, len(instances))
	for _, inst := range instances {
		if inst.isPrimary && inst.ready {
			hasReadyPrimary = true
		}
		if inst.ready && !inst.isPrimary {
			candidates = append(candidates, inst)
		}
	}
	// No failover while a ready primary exists, and no failover when there is
	// nothing ready to elect — a shard with zero ready replicas is either
	// still provisioning or fully down, neither of which the operator can
	// resolve by promoting.
	if hasReadyPrimary || len(candidates) == 0 {
		return failoverDecision{}
	}
	slices.SortFunc(candidates, func(a, b instanceView) int {
		// Most advanced first; ties broken by pod name for determinism.
		if a.receivedLSN != b.receivedLSN {
			return cmp.Compare(b.receivedLSN, a.receivedLSN)
		}
		return strings.Compare(a.pod, b.pod)
	})

	// Don't elect while any candidate's WAL receiver is still running.
	for _, inst := range candidates {
		if inst.walReceiver {
			return failoverDecision{warranted: true, waitReceivers: true}
		}
	}
	return failoverDecision{warranted: true, targetPrimary: candidates[0].pod}
}

// reconcileFailover runs the target/current handshake for one shard. The
// operator sets targetPrimary; the elected agent (guarded by the shard
// Lease) promotes and reports the new role, which the status poll then
// records as currentPrimary. decisionEpoch increments on every new
// election so a delayed agent call from an older failover is rejected.
func (r *PgShardShardReconciler) reconcileFailover(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, views []instanceView,
) error {
	log := logf.FromContext(ctx)
	decision := evaluateFailover(views)
	if !decision.warranted {
		return nil
	}

	if decision.waitReceivers || decision.targetPrimary == "" {
		if shard.Status.TargetPrimary != PendingFailoverMarker {
			shard.Status.TargetPrimary = PendingFailoverMarker
			shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
			log.Info("failover pending: waiting for WAL receivers to drain",
				"shard", shard.Name)
		}
		return nil
	}

	if shard.Status.TargetPrimary == decision.targetPrimary &&
		shard.Status.CurrentPrimary == decision.targetPrimary {
		return nil // already promoted
	}

	// Commit the election: bump the decision epoch and instruct the elected
	// agent to promote. currentPrimary is set by the status poll once the
	// agent reports the primary role.
	if shard.Status.TargetPrimary != decision.targetPrimary {
		shard.Status.DecisionEpoch++
		shard.Status.TargetPrimary = decision.targetPrimary
		shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
		log.Info("electing new primary", "shard", shard.Name,
			"target", decision.targetPrimary, "epoch", shard.Status.DecisionEpoch)
	}

	host := hostByPod(views, decision.targetPrimary)
	if host == "" {
		return nil // target has no reachable address yet; retry next poll
	}
	agent, err := r.agentClient(host, agentPort)
	if err != nil {
		return err
	}
	if _, err := agent.Promote(ctx, &pgshardv1.PromoteRequest{
		TargetPrimary: decision.targetPrimary,
		DecisionEpoch: uint64(shard.Status.DecisionEpoch),
	}); err != nil {
		return err
	}
	return nil
}

func hostByPod(views []instanceView, pod string) string {
	for _, v := range views {
		if v.pod == pod {
			return v.host
		}
	}
	return ""
}
