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
	"fmt"
	"slices"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// identityInputs is the lineage identity one poll's views are checked
// against: the latched system id (0 = not yet latched), the recorded primary
// timeline (0 = unknown), and the pods the operator's own handshake already
// recognizes — the previously confirmed primary and the durably committed
// election target. Only those two are TRUSTED claimants: any other pod
// reporting the primary role either promoted itself or booted from a foreign
// volume, and must never be recognized as the serving primary.
type identityInputs struct {
	systemID  uint64
	timeline  int32
	current   string
	committed string
	// malformed: the persisted latch failed to parse. Nothing can be
	// verified, so every role is refused and publication suppressed until a
	// human repairs the status.
	malformed bool
}

// identityAssessment classifies one poll's views against the latched lineage.
type identityAssessment struct {
	// kept is what the election works over. Same-lineage views whose identity
	// is in question are not dropped from it — they are kept as ambiguity
	// BLOCKERS (a claimant keeps its claim; a standby loses only its
	// candidacy) so the election waits on them instead of electing around
	// WAL they may hold. Only proven-foreign views are dropped: their WAL is
	// another database's and can neither be lost nor elected.
	kept []instanceView
	// fenced holds "pod (reason)" for every view refused recognition,
	// candidacy, or comparison; a fence must never be silent.
	fenced []string
	// unrecognized are pods whose reported role must not be believed: the
	// caller strips their role before status derivation and label sync, so
	// they can never be published as CurrentPrimary (-rw) nor labeled into
	// -ro read routing.
	unrecognized []string
	// suppressPrimary forces CurrentPrimary to stay unpublished this poll: a
	// same-lineage claimant dispute (a rogue claimant, or a pre-latch
	// identity conflict) means SOME live instance may be accepting writes we
	// cannot vouch for, and routing must stay off until it is resolved.
	suppressPrimary bool
	// conflict is set before any identity is latched when observed instances
	// report more than one distinct nonzero system id: there is no way to
	// tell which lineage is this node's, so no election may run at all.
	conflict bool
	// blocked: a same-lineage view with a questionable identity (ahead
	// timeline or unreported id) is parking the election. The resource must
	// read Degraded, not Ready, while it persists.
	blocked bool
	// anyStartedUnobserved: a pod with an assigned address could not be
	// polled, so the identity attestation this poll is incomplete (a known
	// rogue may merely be unreachable).
	anyStartedUnobserved bool
}

func (a *identityAssessment) rogue(pod string) bool {
	return slices.Contains(a.unrecognized, pod)
}

// assessIdentity fences views whose reported identity cannot be vouched for,
// so the election never compares WAL positions across unrelated histories and
// a self-promoted or foreign instance is never served:
//
//   - a system identifier different from the latched one is FOREIGN data (a
//     reused PVC, a restored volume). Foreign views are dropped entirely:
//     never recognized, never electable, and never allowed to block this
//     lineage's own election — their WAL is not ours;
//   - a claimant is RECOGNIZED only when the operator can vouch for it: it is
//     the previously confirmed primary, the committed election target, or it
//     matches the latched id on exactly the recorded timeline (the
//     poll-blip recovery case — a self-promotion always lands AHEAD of the
//     timeline it split from, so an equal timeline is ours; the residual
//     equal-NUMBER twin branch needs the timeline history file, deferred to
//     process supervision). Any other claimant is a rogue: role stripped
//     (never CurrentPrimary/-rw), kept as a claim so the election waits, and
//     CurrentPrimary publication is suppressed while it lives — a
//     same-lineage dispute means writes may be landing where we cannot see;
//   - a same-lineage standby on an AHEAD timeline, or one reporting NO id
//     after the latch, may hold acknowledged WAL of THIS lineage: it loses
//     candidacy and recognition (role stripped, so no -ro reads) but stays
//     in the views as an ambiguity blocker — electing around WAL it may
//     hold could lose acknowledged writes. A timeline BEHIND the recorded
//     one is not fenced for standbys: every promotion leaves healthy
//     standbys momentarily behind;
//   - the durably committed target is exempt from the timeline/unreported
//     fences (never the foreign fence): pg_promote bumps the timeline before
//     the role flips, so mid-promotion it reports standby-or-unspecified on
//     an ahead timeline, and dropping it would strand the handshake.
//
// Before any identity is latched nothing can be compared; the enforceable
// invariant is agreement. On a conflict every claimant is stripped and
// publication suppressed — otherwise the first conflicting poll would
// publish a claimant as CurrentPrimary and the next poll would trust and
// latch it, silently resolving the dispute in the claimant's favor.
// Bootstrap without conflict is trust-on-first-use one poll later — the
// first confirmed primary becomes CurrentPrimary, then latches — backed by
// the PVC provenance labels that keep a foreign volume from being mounted.
func assessIdentity(views []instanceView, in identityInputs) identityAssessment {
	a := identityAssessment{kept: make([]instanceView, 0, len(views))}
	for _, v := range views {
		if !v.observed && v.host != "" {
			a.anyStartedUnobserved = true
		}
	}
	if in.malformed {
		// The persisted latch is unreadable: nothing can be compared, so
		// NOTHING may be believed — not just the election. A sole claimant
		// must not keep -rw on the strength of a latch we cannot verify.
		for _, v := range views {
			a.unrecognized = append(a.unrecognized, v.pod)
			a.fenced = append(a.fenced, fmt.Sprintf(
				"%s (latched identity is unreadable; nothing can be verified)", v.pod))
		}
		a.suppressPrimary = true
		a.kept = append(a.kept, views...)
		return a
	}
	if in.systemID == 0 {
		ids := map[uint64]bool{}
		for _, v := range views {
			if v.observed && v.systemID != 0 {
				ids[v.systemID] = true
			}
		}
		a.conflict = len(ids) > 1
		if a.conflict {
			// EVERY instance is unrecognized during a conflict — either
			// lineage could be the intruder, so no role may be believed: a
			// claimant must not be published (it would then be trusted and
			// latch itself next poll) and a standby must not keep serving
			// reads through -ro.
			for _, v := range views {
				a.unrecognized = append(a.unrecognized, v.pod)
				a.fenced = append(a.fenced, fmt.Sprintf(
					"%s (unresolved bootstrap identity conflict)", v.pod))
			}
			a.suppressPrimary = true
		}
		a.kept = append(a.kept, views...)
		return a
	}
	for _, v := range views {
		foreign := v.systemID != 0 && v.systemID != in.systemID
		unknown := v.observed && v.systemID == 0
		ahead := in.timeline != 0 && v.timeline > in.timeline
		// A claimant is vouched for ONLY by its full identity: latched id on
		// exactly the recorded timeline. A trusted pod NAME is deliberately
		// not enough — the same name can return on an old backup of the same
		// cluster (matching id, BEHIND timeline: a stale fork whose
		// publication would discard acknowledged writes) and the recorded
		// timeline never regresses to meet it.
		vouched := v.systemID == in.systemID && in.timeline != 0 && v.timeline == in.timeline
		switch {
		case v.pod == in.committed && in.committed != "" && !foreign:
			a.kept = append(a.kept, v)
		case foreign:
			// Another lineage entirely: never recognized in ANY role (its
			// reads are wrong data too), and dropped from the views — its WAL
			// is not ours, so it can neither be lost nor block our election.
			a.unrecognized = append(a.unrecognized, v.pod)
			a.fenced = append(a.fenced,
				fmt.Sprintf("%s (foreign system id %d)", v.pod, v.systemID))
		case v.isPrimary && vouched:
			a.kept = append(a.kept, v)
		case v.isPrimary:
			reason := fmt.Sprintf(
				"timeline %d does not match recorded %d and the pod is not the recognized primary",
				v.timeline, in.timeline)
			if unknown {
				reason = "no system id reported after latch"
			}
			a.unrecognized = append(a.unrecognized, v.pod)
			a.fenced = append(a.fenced,
				fmt.Sprintf("%s (unrecognized primary claimant: %s)", v.pod, reason))
			a.kept = append(a.kept, v)
			a.suppressPrimary = true
		case unknown || ahead:
			// Ambiguity blocker: strip candidacy and recognition but keep the
			// view (with its LSN and receiver activity) so the election waits
			// on it instead of losing WAL it may hold.
			reason := fmt.Sprintf("timeline %d ahead of recorded %d", v.timeline, in.timeline)
			if unknown {
				reason = "no system id reported after latch"
			}
			a.unrecognized = append(a.unrecognized, v.pod)
			a.fenced = append(a.fenced, fmt.Sprintf("%s (%s)", v.pod, reason))
			a.blocked = true
			blocked := v
			blocked.isPrimary = false
			blocked.isStandby = false
			a.kept = append(a.kept, blocked)
		default:
			a.kept = append(a.kept, v)
		}
	}
	return a
}

// parseLatchedID parses the status-latched system id. Empty means "not
// latched" (0); a nonempty value that does not parse — or parses to zero,
// which no PostgreSQL instance reports — is an error so a corrupted latch
// fails closed instead of silently disabling the fence.
func parseLatchedID(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, fmt.Errorf("latched system id is zero")
	}
	return id, nil
}

// latchIdentity latches the data-lineage id (never overwritten once set) and
// maintains the recorded primary timeline — only from a TRUSTED claimant
// whose reported id matches the latch: the pod the handshake already
// recognizes as primary (current) or is promoting (committed). An unsolicited
// claimant must not latch (a reused volume could imprint a foreign lineage
// forever), and even a trusted pod NAME returning on a foreign volume must
// not launder its timeline into the recorded one. Two further guards:
//
//   - no latch while observed instances DISAGREE on their nonzero ids. A
//     one-poll blind spot (the rival unobserved while an intruder claims
//     primary) records CurrentPrimary before any comparison is possible;
//     without this guard the next poll would latch the intruder via that
//     trust and fence the genuine lineage as "foreign". The conflict path
//     handles the poll instead;
//   - the recorded timeline only ever advances, and only the COMMITTED
//     election target may advance it past an existing value: a timeline bump
//     is a promotion, and the operator drives every legitimate promotion
//     through TargetPrimary — a reigning primary that suddenly reports a
//     higher timeline (a stale-status same-name rogue, a restored fork) must
//     be fenced by the assessment, not laundered into the record. The
//     first recorded timeline (record zero) may come from either trusted
//     name (the bootstrap latch itself).
//
// Returns the possibly just-latched id string, its parsed value, and the
// updated timeline.
func latchIdentity(
	views []instanceView, current, committed, latched string, timeline int32,
) (systemID string, expectedID uint64, tl int32, parseErr error) {
	systemID, tl = latched, timeline
	expectedID, parseErr = parseLatchedID(latched)
	ids := map[uint64]bool{}
	for _, v := range views {
		if v.observed && v.systemID != 0 {
			ids[v.systemID] = true
		}
	}
	disagreement := len(ids) > 1
	for _, v := range views {
		trusted := v.pod != "" && (v.pod == current || v.pod == committed)
		if !v.isPrimary || !v.observed || !trusted || v.systemID == 0 {
			continue
		}
		if parseErr == nil && expectedID == 0 {
			if disagreement {
				continue
			}
			systemID = strconv.FormatUint(v.systemID, 10)
			expectedID = v.systemID
		}
		if v.systemID != expectedID || v.timeline <= tl {
			continue
		}
		if tl == 0 || v.pod == committed {
			tl = v.timeline
		}
	}
	return systemID, expectedID, tl, parseErr
}

// identityConsistentCondition is the shared IdentityConsistent condition for
// one poll's assessment; nil when there is nothing to report yet (identity
// not latched and no conflict).
func identityConsistentCondition(
	a *identityAssessment, latched bool, parseErr error, generation int64,
) *metav1.Condition {
	cond := &metav1.Condition{Type: "IdentityConsistent", ObservedGeneration: generation}
	switch {
	case parseErr != nil:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "MalformedIdentity"
		cond.Message = "latched system id is not parseable; election disabled: " + parseErr.Error()
	case a.conflict:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "IdentityConflict"
		cond.Message = "instances report conflicting system ids before any identity was latched; election and primary publication disabled until they agree"
	case latched && len(a.fenced) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "ForeignInstance"
		cond.Message = "instances fenced from election or recognition: " + strings.Join(a.fenced, ", ")
	case latched && a.anyStartedUnobserved:
		// A started instance could not be polled: a known rogue may merely be
		// unreachable, so this poll cannot attest consistency — and must not
		// erase a standing report by flapping to True.
		cond.Status = metav1.ConditionUnknown
		cond.Reason = "IncompleteObservation"
		cond.Message = "a started instance could not be polled; identity attestation is incomplete this cycle"
	case latched:
		cond.Status = metav1.ConditionTrue
		cond.Reason = "Latched"
		cond.Message = "all observed instances match the latched system identity"
	default:
		return nil
	}
	return cond
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
