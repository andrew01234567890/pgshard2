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

// evaluateFailover decides, from the polled instance statuses, whether a
// failover is warranted and which replica to elect. It is a pure function so
// the decision is unit-testable without a live cluster.
//
// A failover is warranted when the shard has instances but none is a ready
// primary and there is a ready replica to promote. We elect only from a
// snapshot that is safe to act on:
//   - no instance still claims the primary role — a settling or wedged but
//     reachable primary may still accept writes, so promoting a second one is
//     split-brain;
//   - no instance that has a pod IP is unobservable this poll. An instance that
//     was assigned an IP (so it has run) but could not be polled is treated as
//     possibly-live: it may be a primary serving on the far side of a partition
//     (split-brain) or hold acknowledged WAL that only it has (lost write). An
//     instance with no IP has never started and can be neither, so it does not
//     block;
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
// Limits deliberately left to the fencing/switchover controller: actively
// fencing (STONITH) an unreachable instance — so a pod force-deleted while
// still alive on a partition is not detected here, and a persistently
// unreachable instance parks the failover rather than being shot; a durable
// acknowledged-WAL watermark that would let a candidate be proven safe when a
// more-advanced replica's pod is gone entirely; and a bounded-lag timeout.
// Until then this controller prefers waiting (no split-brain, no lost writes)
// over promoting into ambiguity.
type failoverDecision struct {
	warranted     bool
	targetPrimary string
	// wait is true when a failover is warranted but the snapshot is not yet
	// safe to elect from; the caller holds and retries.
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

func evaluateFailover(instances []instanceView) failoverDecision {
	hasReadyPrimary := false
	anyClaimsPrimary := false
	anyRunningUnobserved := false
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
		// A pod that has an IP has run and may be a live primary or hold WAL only
		// it has; if we could not poll it this cycle we must not elect around it.
		// A pod with no IP has never started and cannot block.
		if !inst.observed && inst.host != "" {
			anyRunningUnobserved = true
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
	// Warranted, but only safe to elect from a no-claimant snapshot in which
	// every started instance is accounted for and WAL has settled.
	if anyClaimsPrimary || anyRunningUnobserved || anyReceiverActive {
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

// reconcileFailover runs the target/current handshake for one shard. When a
// failover is elected, targetPrimary records the chosen replacement and the
// operator, once that is durable, instructs its agent to promote; the agent
// reports the new role, which the status poll records as currentPrimary.
// decisionEpoch increments on every new election and guards the Promote so a
// delayed call from an older failover is rejected. Phase is set to FailingOver
// only while actively electing/promoting, so a genuine multi-primary split-brain
// (which deriveShardStatus reports as Degraded) is not masked.
func (r *PgShardShardReconciler) reconcileFailover(
	ctx context.Context, shard *pgshardv1alpha1.PgShardShard, views []instanceView,
) error {
	log := logf.FromContext(ctx)
	decision := evaluateFailover(views)

	// Not warranted (healthy primary or nothing electable), or warranted but not
	// yet safe to elect: leave the phase deriveShardStatus computed this cycle —
	// Degraded while a split-brain or a quorum loss persists is the honest signal.
	if !decision.warranted || decision.wait {
		return nil
	}

	// Sticky target: once we have committed to a target keep driving that same
	// pod while it is still present and observable, rather than switching to a
	// different newly-preferred one. A Promote for the committed target may
	// already be in flight; switching would leave two agents promoted
	// (split-brain), and a target that is momentarily not-ready is the normal
	// mid-promotion window — not a reason to abandon it. We only re-elect once
	// the committed target is gone (absent) or unpollable (which itself parks the
	// election above). This is never a data-loss regression: the committed target
	// was the most advanced at election and no new WAL arrives once the primary
	// is gone, so any later-preferred candidate is at best tied with it. (A target
	// that is present but permanently failing to promote parks the failover — the
	// bounded-lag/fencing cancel path is deferred, as documented above.)
	target := decision.targetPrimary
	if cur := shard.Status.TargetPrimary; cur != "" && isObserved(views, cur) {
		target = cur
	}

	// Commit the election durably BEFORE instructing any agent. The decision
	// epoch is the fencing token: persisting the bump first guarantees a crash
	// or failed status write can never leave the persisted epoch below one an
	// agent has already applied, which would let the same epoch be reissued to
	// a different target (two agents both accepting = split-brain). The Promote
	// is driven on the next reconcile, once the epoch is durable.
	if shard.Status.TargetPrimary != target {
		shard.Status.DecisionEpoch++
		shard.Status.TargetPrimary = target
		shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
		log.Info("electing new primary", "shard", shard.Name,
			"target", target, "epoch", shard.Status.DecisionEpoch)
		return nil
	}

	// Target and epoch are durable; drive the promote. It is idempotent under
	// the agent's epoch guard, so repeating it across polls is safe.
	shard.Status.Phase = pgshardv1alpha1.ShardFailingOver
	host := hostByPod(views, target)
	if host == "" {
		return nil // target has no reachable address yet; retry next poll
	}
	agent, err := r.agentClient(host, agentPort)
	if err != nil {
		return err
	}
	if _, err := agent.Promote(ctx, &pgshardv1.PromoteRequest{
		TargetPrimary: target,
		DecisionEpoch: uint64(shard.Status.DecisionEpoch),
	}); err != nil {
		return err
	}
	return nil
}

// isObserved reports whether pod is present in the view set and was polled
// successfully this cycle. A committed promote target stays pinned while it is
// observed, even if momentarily not ready (the normal mid-promotion window).
func isObserved(views []instanceView, pod string) bool {
	for _, v := range views {
		if v.pod == pod {
			return v.observed
		}
	}
	return false
}

func hostByPod(views []instanceView, pod string) string {
	for _, v := range views {
		if v.pod == pod {
			return v.host
		}
	}
	return ""
}
