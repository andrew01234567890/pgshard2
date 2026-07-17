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

// evaluateFailover decides, from the polled instance statuses and the pod the
// operator currently expects to be primary, whether a failover is warranted
// and which replica to elect. It is a pure function so the decision is
// unit-testable without a live cluster.
//
// Rules (CNPG-derived), ordered by safety. A failover is warranted when the
// shard has instances but none is a ready primary and there is a ready replica
// to promote. We elect only from a snapshot that is safe to act on:
//   - no instance still claims the primary role — a settling or wedged but
//     reachable primary may still accept writes, so promoting a second one is
//     split-brain;
//   - the pod we still expect to be primary is not merely unreachable — an
//     unpollable expected primary may be alive and serving on the far side of a
//     partition, so we cannot promote a replacement. A never-primary replica
//     that is Pending or briefly unreachable, by contrast, must NOT veto the
//     election;
//   - every WAL receiver is quiet — an active receiver's received LSN is not
//     yet final;
//   - and the most-advanced ready candidate is not behind any observed
//     instance's received LSN — electing a laggard would discard WAL (under
//     synchronous replication, acknowledged commits) that a more-advanced but
//     not-yet-ready standby already holds.
//
// The elected target is the ready replica with the highest received WAL
// position, ties broken by pod name for determinism.
//
// Two limits are deliberately left to the fencing/switchover controller:
// actively fencing (STONITH) an unreachable expected primary — so a primary pod
// force-deleted while still alive on a partition is not detected here — and a
// bounded-lag timeout so a permanently not-ready most-advanced standby cannot
// park the failover indefinitely. Until then this controller prefers waiting
// (no split-brain, no lost writes) over promoting into ambiguity.
type failoverDecision struct {
	warranted     bool
	targetPrimary string
	// wait is true when a failover is warranted but the snapshot is not yet
	// safe to elect from. The caller keeps the expected-primary identity and
	// retries.
	wait bool
}

type instanceView struct {
	pod         string
	host        string
	ready       bool
	isPrimary   bool
	receivedLSN uint64
	walReceiver bool
	// observed is true only when this instance's agent was successfully polled
	// this reconcile. A false value means "unknown", not "down": the other
	// fields are their zero values and must not be read as fact.
	observed bool
}

func evaluateFailover(instances []instanceView, expectedPrimary string) failoverDecision {
	hasReadyPrimary := false
	anyClaimsPrimary := false
	expectedPrimaryUnobserved := false
	anyReceiverActive := false
	var maxObservedLSN uint64
	candidates := make([]instanceView, 0, len(instances))
	for _, inst := range instances {
		if inst.isPrimary {
			anyClaimsPrimary = true
			if inst.ready {
				hasReadyPrimary = true
			}
		}
		if inst.observed && inst.receivedLSN > maxObservedLSN {
			maxObservedLSN = inst.receivedLSN
		}
		// Only the pod we still expect to be primary blocks the election when
		// unreachable — it may be a live primary we cannot see. A never-primary
		// replica that is Pending or briefly unreachable must not veto an
		// otherwise-safe promotion.
		if !inst.observed && inst.pod == expectedPrimary {
			expectedPrimaryUnobserved = true
		}
		if inst.walReceiver {
			anyReceiverActive = true
		}
		if inst.ready && !inst.isPrimary {
			candidates = append(candidates, inst)
		}
	}
	// No failover while a ready primary exists, and no failover when there is
	// nothing ready to elect — a shard with zero ready replicas is either
	// still provisioning or fully down, neither of which promoting can resolve.
	if hasReadyPrimary || len(candidates) == 0 {
		return failoverDecision{}
	}
	// Warranted, but only safe to elect from a no-claimant snapshot whose
	// expected primary is accounted for and whose WAL has settled.
	if anyClaimsPrimary || expectedPrimaryUnobserved || anyReceiverActive {
		return failoverDecision{warranted: true, wait: true}
	}
	slices.SortFunc(candidates, func(a, b instanceView) int {
		// Most advanced first; ties broken by pod name for determinism.
		if a.receivedLSN != b.receivedLSN {
			return cmp.Compare(b.receivedLSN, a.receivedLSN)
		}
		return strings.Compare(a.pod, b.pod)
	})
	// Never promote a ready candidate behind a more-advanced observed instance
	// (e.g. a not-ready standby that streamed further WAL before its readiness
	// lapsed): those writes may be acknowledged and would be lost.
	if candidates[0].receivedLSN < maxObservedLSN {
		return failoverDecision{warranted: true, wait: true}
	}
	return failoverDecision{warranted: true, targetPrimary: candidates[0].pod}
}

// reconcileFailover runs the target/current handshake for one shard.
// targetPrimary durably records the pod the operator expects to hold the
// primary role: in steady state it tracks the observed primary, and during a
// failover it is the elected replacement. Recording it means a transient loss
// of contact with the primary does not look like "no primary" and trigger a
// spurious promotion. Once a new target is durable the operator instructs that
// agent to promote; the agent reports the new role, which the status poll
// records as currentPrimary. decisionEpoch increments on every new election and
// guards the Promote so a delayed call from an older failover is rejected.
func (r *PgShardShardReconciler) reconcileFailover(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, views []instanceView,
) error {
	log := logf.FromContext(ctx)
	// The durable expected primary, read before we mutate it below.
	expectedPrimary := shard.Status.TargetPrimary
	decision := evaluateFailover(views, expectedPrimary)

	if !decision.warranted {
		switch {
		case shard.Status.CurrentPrimary != "":
			// Healthy: track the confirmed primary as the failover target.
			shard.Status.TargetPrimary = shard.Status.CurrentPrimary
		case shard.Status.TargetPrimary != "":
			// No confirmed primary and nothing electable this poll, but a
			// failover is in flight (we expect a primary that is not live):
			// hold FailingOver so the phase does not flap to the Degraded that
			// deriveShardStatus, run earlier this cycle, would otherwise set.
			shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
		}
		return nil
	}

	// Warranted but not yet safe to elect: keep the expected-primary identity
	// (do not lose track of who we are waiting on) and hold in FailingOver.
	if decision.wait {
		shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
		return nil
	}

	// Commit the election durably BEFORE instructing any agent. The decision
	// epoch is the fencing token: persisting the bump first guarantees a crash
	// or failed status write can never leave the persisted epoch below one an
	// agent has already applied, which would let the same epoch be reissued to
	// a different target (two agents both accepting = split-brain). The Promote
	// is driven on the next reconcile, once the epoch is durable.
	if shard.Status.TargetPrimary != decision.targetPrimary {
		shard.Status.DecisionEpoch++
		shard.Status.TargetPrimary = decision.targetPrimary
		shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
		log.Info("electing new primary", "shard", shard.Name,
			"target", decision.targetPrimary, "epoch", shard.Status.DecisionEpoch)
		return nil
	}

	// Target and epoch are durable; drive the promote. It is idempotent under
	// the agent's epoch guard, so repeating it across polls is safe.
	shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
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
