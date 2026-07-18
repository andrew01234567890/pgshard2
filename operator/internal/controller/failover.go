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
	pod       string
	host      string
	ready     bool
	isPrimary bool
	// isStandby is true only when the agent explicitly reported the STANDBY role.
	// An observed instance that is neither isPrimary nor isStandby has an
	// UNCONFIRMED role (agent reported UNSPECIFIED): its role is unknown, not
	// "standby by default", so it must not be elected, ready-counted, or pruned.
	isStandby   bool
	receivedLSN uint64
	walReceiver bool
	// observed is true only when this instance's agent was successfully polled
	// this reconcile. A false value means "unknown", not "down": the other
	// fields are their zero values and must not be read as fact.
	observed bool
	// systemID is the PostgreSQL system identifier the agent reported (0 when
	// unreported); timeline likewise. Identity fencing happens BEFORE the
	// election (partitionByIdentity), so the election logic itself never sees
	// an instance whose WAL positions are not comparable.
	systemID uint64
	timeline int32
}

// partitionByIdentity drops views whose reported identity cannot belong to
// this data lineage, so the election never compares WAL positions across
// unrelated histories:
//
//   - a system identifier different from the latched one is FOREIGN data (a
//     reused PVC, a restored volume) — its LSNs are meaningless here, and
//     electing it would serve another database's contents;
//   - a timeline AHEAD of the confirmed primary's, without an
//     operator-driven promotion, is a split-brain artifact (something
//     promoted itself) and is never electable. A timeline BEHIND the
//     recorded one is deliberately NOT fenced here: a healthy standby
//     legitimately lags the new timeline for a moment after every
//     promotion, and telling that apart from an abandoned branch requires
//     the timeline history file — agent work tracked with process
//     supervision. The election's own most-advanced-LSN guard still applies
//     to those.
//
// A zero on either side fences nothing — identity not yet latched, or an
// instance that did not report — the election's confirmed-role requirements
// still apply to those. Excluded pod names are returned for loud reporting;
// exclusion must never be silent.
func partitionByIdentity(
	views []instanceView, systemID uint64, timeline int32,
) (kept []instanceView, excluded []string) {
	kept = make([]instanceView, 0, len(views))
	for _, v := range views {
		foreignID := systemID != 0 && v.systemID != 0 && v.systemID != systemID
		divergentTL := timeline != 0 && v.timeline > timeline
		// The instance currently claiming primary is never dropped from the
		// view set: the handshake must keep tracking it (and its identity
		// mismatch is reported); it is the ELECTION that must not choose a
		// mismatched replacement, and candidates require the standby role.
		if (foreignID || divergentTL) && !v.isPrimary {
			excluded = append(excluded, v.pod)
			continue
		}
		kept = append(kept, v)
	}
	return kept, excluded
}

// instanceSummary is the fold of one failover snapshot: the flags and running
// values evaluateFailover decides from.
type instanceSummary struct {
	hasReadyPrimary      bool
	anyClaimsPrimary     bool
	anyRunningUnobserved bool
	anyRoleUnconfirmed   bool
	anyReceiverActive    bool
	committedDrivable    bool // committed target present, observed, not yet primary
	committedLSN         uint64
	maxObservedLSN       uint64
	candidates           []instanceView
}

// summarizeInstances folds the polled instance views into the flags and values
// the election decision reads.
func summarizeInstances(instances []instanceView, committedTarget string) instanceSummary {
	s := instanceSummary{candidates: make([]instanceView, 0, len(instances))}
	for _, inst := range instances {
		if inst.isPrimary {
			s.anyClaimsPrimary = true
			if inst.ready {
				s.hasReadyPrimary = true
			}
		}
		if inst.observed && inst.receivedLSN > s.maxObservedLSN {
			s.maxObservedLSN = inst.receivedLSN
		}
		// A pod that has an IP has run and may be a live primary or hold WAL only
		// it has; if we could not poll it this cycle we must not elect around it.
		// A pod with no IP has never started and cannot block.
		if !inst.observed && inst.host != "" {
			s.anyRunningUnobserved = true
		}
		// A polled pod whose role is neither primary nor standby has an unknown
		// role (it may be a primary that has not yet reported one); electing
		// around it is as unsafe as electing around an unpolled pod. The committed
		// target is excluded: we are already driving it to promotion, during which
		// it legitimately reports no settled role, and the committed-target path
		// governs whether to keep driving it or wait.
		if inst.observed && !inst.isPrimary && !inst.isStandby && inst.pod != committedTarget {
			s.anyRoleUnconfirmed = true
		}
		if inst.walReceiver {
			s.anyReceiverActive = true
		}
		// Only a confirmed standby is an election candidate. A ready pod with an
		// unconfirmed role is not promoted on the assumption it is a replica.
		if inst.ready && inst.isStandby {
			s.candidates = append(s.candidates, inst)
		}
		if committedTarget != "" && inst.pod == committedTarget && inst.observed && !inst.isPrimary {
			s.committedDrivable = true
			s.committedLSN = inst.receivedLSN
		}
	}
	return s
}

func evaluateFailover(instances []instanceView, committedTarget string) failoverDecision {
	s := summarizeInstances(instances, committedTarget)
	// No failover while a ready primary exists.
	if s.hasReadyPrimary {
		return failoverDecision{}
	}
	// Nothing to act on: no ready replica to elect, no committed target to drive,
	// and no outstanding commitment to wait for (provisioning or fully down).
	if len(s.candidates) == 0 && !s.committedDrivable && committedTarget == "" {
		return failoverDecision{}
	}
	// Only safe to promote from a no-claimant snapshot in which every started
	// instance is accounted for, every role is confirmed, and WAL has settled.
	if s.anyClaimsPrimary || s.anyRunningUnobserved || s.anyRoleUnconfirmed || s.anyReceiverActive {
		return failoverDecision{warranted: true, wait: true}
	}
	// A durable commitment to a specific target governs the decision: keep driving
	// that same pod, or wait for it — but never elect a DIFFERENT, possibly-behind
	// pod around it. reconcileFailover clears the commitment once its target
	// becomes a ready primary, so a committed target here is one we are still
	// driving to promotion.
	if committedTarget != "" {
		switch {
		case !s.committedDrivable:
			// Gone or without a pod IP this cycle: unaccounted for. It may return
			// holding WAL only it has, or may already be promoted, so we park rather
			// than promote a replacement. Because the operator durably committed to
			// this target (it was the most advanced at election), waiting — not a
			// WAL watermark — is the data-safe choice; only STONITH/bounded-lag
			// (deferred) may later abandon it.
			return failoverDecision{warranted: true, wait: true}
		case s.committedLSN < s.maxObservedLSN:
			// Present but behind an observed peer (e.g. rebuilt from an empty
			// volume): never promote it as a laggard.
			return failoverDecision{warranted: true, wait: true}
		default:
			// Keep driving it, even mid-promotion when it is momentarily not a ready
			// candidate (so a two-node failover is not stranded).
			return failoverDecision{warranted: true, targetPrimary: committedTarget}
		}
	}
	// Fresh election: the most-advanced ready candidate, ties broken by pod name.
	slices.SortFunc(s.candidates, func(a, b instanceView) int {
		if a.receivedLSN != b.receivedLSN {
			return cmp.Compare(b.receivedLSN, a.receivedLSN)
		}
		return strings.Compare(a.pod, b.pod)
	})
	// Never promote a ready candidate behind a more-advanced observed instance
	// (e.g. a not-ready standby that streamed further WAL before its readiness
	// lapsed): those writes may be acknowledged and would be lost.
	if s.candidates[0].receivedLSN < s.maxObservedLSN {
		return failoverDecision{warranted: true, wait: true}
	}
	return failoverDecision{warranted: true, targetPrimary: s.candidates[0].pod}
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
	// evaluateFailover applies the sticky/drive rule against the committed target,
	// so its result already names the pod to keep driving (never a different one
	// once we have committed, which would double-promote).
	decision := evaluateFailover(views, shard.Status.TargetPrimary)

	// Not warranted (healthy primary or nothing electable), or warranted but not
	// yet safe to elect: leave the phase deriveShardStatus computed this cycle —
	// Degraded while a split-brain or a quorum loss persists is the honest signal.
	if !decision.warranted || decision.wait {
		// A confirmed ready primary means any in-flight failover has completed:
		// drop the election commitment so a later failure of THIS primary starts a
		// fresh election instead of parking forever on the now-stale target.
		if !decision.warranted && shard.Status.CurrentPrimary != "" && shard.Status.TargetPrimary != "" {
			shard.Status.TargetPrimary = ""
		}
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
	// the agent's epoch guard, so repeating it across polls (including while the
	// target is mid-promotion and not yet ready) is safe.
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

// reconcileFailover runs the target/current handshake for one node. It is the
// node counterpart of the shard handshake above and shares the same pure
// election (evaluateFailover): a node fails over as a unit, so the same
// commit-epoch-before-promote, sticky-target, and no-laggard guards apply.
func (r *PgShardNodeReconciler) reconcileFailover(
	ctx context.Context, node *pgshardv1alpha1.PgShardNode, views []instanceView,
) error {
	log := logf.FromContext(ctx)
	decision := evaluateFailover(views, node.Status.TargetPrimary)

	if !decision.warranted || decision.wait {
		// A confirmed ready primary means any in-flight failover completed: drop
		// the commitment so a later failure of THIS primary elects afresh instead
		// of parking forever on the now-stale target.
		if !decision.warranted && node.Status.CurrentPrimary != "" && node.Status.TargetPrimary != "" {
			node.Status.TargetPrimary = ""
		}
		return nil
	}

	// Commit the election durably (epoch bump = the fencing token) BEFORE
	// instructing any agent, so a crash cannot leave the persisted epoch below
	// one an agent already applied. The Promote is driven next reconcile.
	if node.Status.TargetPrimary != decision.targetPrimary {
		node.Status.DecisionEpoch++
		node.Status.TargetPrimary = decision.targetPrimary
		node.Status.Phase = pgshardv1alpha1.NodeFailingOver
		log.Info("electing new primary", "node", node.Name,
			"target", decision.targetPrimary, "epoch", node.Status.DecisionEpoch)
		return nil
	}

	// Target and epoch are durable; drive the idempotent, epoch-guarded promote.
	node.Status.Phase = pgshardv1alpha1.NodeFailingOver
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
		DecisionEpoch: uint64(node.Status.DecisionEpoch),
	}); err != nil {
		return err
	}
	return nil
}
