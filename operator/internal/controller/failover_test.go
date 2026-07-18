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
	"testing"
)

const testHostIP = "10.0.0.3"

func TestEvaluateFailover(t *testing.T) {
	cases := []struct {
		name            string
		instances       []instanceView
		committedTarget string
		wantWarranted   bool
		wantTarget      string
		wantWait        bool
	}{
		{
			name: "healthy primary: no failover",
			instances: []instanceView{
				{pod: "p0", ready: true, isPrimary: true, observed: true},
				{pod: "p1", ready: true, isStandby: true, observed: true},
			},
		},
		{
			name: "drive a committed target still mid-promotion: not stranded even as the only instance",
			instances: []instanceView{
				// committed, promoting: observed but reports no settled role yet.
				{pod: "p1", host: testHostIP, observed: true},
			},
			committedTarget: "p1",
			wantWarranted:   true,
			wantTarget:      "p1",
		},
		{
			name: "no committed target and only a not-ready instance: nothing to elect",
			instances: []instanceView{
				{pod: "p1", host: testHostIP, observed: true}, // not-ready, not committed
			},
		},
		{
			name: "sticky: keep the committed target over a tied, name-earlier ready replica",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 500, observed: true}, // sorts first
				{pod: "p2", ready: true, isStandby: true, receivedLSN: 500, observed: true}, // committed
			},
			committedTarget: "p2",
			wantWarranted:   true,
			wantTarget:      "p2",
		},
		{
			name: "committed target has fallen behind an observed peer: wait, never promote a laggard",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 100, observed: true},  // committed, behind
				{pod: "p2", ready: false, isStandby: true, receivedLSN: 300, observed: true}, // more advanced
			},
			committedTarget: "p1",
			wantWarranted:   true,
			wantWait:        true,
		},
		{
			name: "committed target lost its pod IP (evicted): park, never elect a different replica around it",
			instances: []instanceView{
				{pod: "p1", host: "", observed: false},                                      // committed, Pending
				{pod: "p2", ready: true, isStandby: true, receivedLSN: 400, observed: true}, // a behind replica
			},
			committedTarget: "p1",
			wantWarranted:   true,
			wantWait:        true,
		},
		{
			name: "committed target vanished from the set: park, do not promote a survivor",
			instances: []instanceView{
				{pod: "p2", ready: true, isStandby: true, receivedLSN: 400, observed: true},
			},
			committedTarget: "p1", // absent from the set entirely
			wantWarranted:   true,
			wantWait:        true,
		},
		{
			name: "old primary relinquished the role: elect most-advanced ready replica",
			instances: []instanceView{
				{pod: "p0", ready: false, isStandby: true, observed: true}, // demoted to standby
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: true, isStandby: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantTarget:    "p2",
		},
		{
			name: "old primary pod gone from the set: elect most-advanced ready replica",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: true, isStandby: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantTarget:    "p2",
		},
		{
			name: "tie on LSN broken by pod name",
			instances: []instanceView{
				{pod: "pb", ready: true, isStandby: true, receivedLSN: 200, observed: true},
				{pod: "pa", ready: true, isStandby: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantTarget:    "pa",
		},
		{
			name: "settling primary still claims the role: wait, never a second promote",
			instances: []instanceView{
				{pod: "p0", isPrimary: true, ready: false, observed: true},
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "a started instance is unobservable: wait, it may be a live primary or hold WAL",
			instances: []instanceView{
				{pod: "p0", host: "10.0.0.5", observed: false}, // had an IP, poll failed
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "a never-started pod (no IP) does not veto an otherwise-safe election",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 200, observed: true},
				{pod: "p2", host: "", observed: false}, // Pending / unschedulable, never ran
			},
			wantWarranted: true,
			wantTarget:    "p1",
		},
		{
			name: "a not-ready standby is still receiving WAL: wait for it to drain",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: false, isStandby: true, receivedLSN: 200, walReceiver: true, observed: true},
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "a drained not-ready standby holds more WAL: wait, do not elect a laggard",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: false, isStandby: true, receivedLSN: 200, observed: true}, // drained, ahead
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "wait while a candidate WAL receiver is still running",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 200, walReceiver: true, observed: true},
				{pod: "p2", ready: true, isStandby: true, receivedLSN: 100, observed: true},
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "primary down with no ready replica: not an electable failover",
			instances: []instanceView{
				{pod: "p0", ready: false, isStandby: true, observed: true},
				{pod: "p1", ready: false, isStandby: true, observed: true},
			},
		},
		{
			// A ready pod whose agent has not confirmed a role (reports UNSPECIFIED)
			// is not a candidate: it might be the primary that has not yet reported
			// its role. With no confirmed standby to elect, no failover is warranted.
			name: "a ready pod with an unconfirmed role is not elected",
			instances: []instanceView{
				{pod: "p0", ready: false, isStandby: true, observed: true}, // old primary, down
				{pod: "p1", ready: true, observed: true},                   // role UNSPECIFIED
			},
		},
		{
			// A confirmed standby is not promoted while a live peer's role is
			// unconfirmed — the peer may be a primary or hold more WAL (here it does).
			name: "an unconfirmed-role peer blocks electing a confirmed standby around it",
			instances: []instanceView{
				{pod: "p1", ready: true, isStandby: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: true, receivedLSN: 200, observed: true}, // role UNSPECIFIED, more WAL
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name:      "empty shard: nothing to do",
			instances: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evaluateFailover(tc.instances, tc.committedTarget)
			if got.warranted != tc.wantWarranted {
				t.Fatalf("warranted = %v, want %v", got.warranted, tc.wantWarranted)
			}
			if got.targetPrimary != tc.wantTarget {
				t.Fatalf("target = %q, want %q", got.targetPrimary, tc.wantTarget)
			}
			if got.wait != tc.wantWait {
				t.Fatalf("wait = %v, want %v", got.wait, tc.wantWait)
			}
		})
	}
}

func TestAssessIdentity(t *testing.T) {
	keptPods := func(a identityAssessment) []string {
		out := make([]string, 0, len(a.kept))
		for _, v := range a.kept {
			out = append(out, v.pod)
		}
		return out
	}
	equal := func(got, want []string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}

	cases := []struct {
		name         string
		views        []instanceView
		in           identityInputs
		wantKept     []string
		wantRogues   []string
		wantFenced   int
		wantConflict bool
	}{
		{
			name: "pre-latch, agreeing ids: everything kept, no conflict",
			views: []instanceView{
				{pod: "p0", isPrimary: true, observed: true, systemID: 4242, timeline: 1},
				{pod: "p1", isStandby: true, observed: true, systemID: 4242, timeline: 1},
			},
			in:       identityInputs{},
			wantKept: []string{"p0", "p1"},
		},
		{
			name: "pre-latch, conflicting nonzero ids: election disabled",
			views: []instanceView{
				{pod: "p0", isStandby: true, observed: true, systemID: 4242},
				{pod: "p1", isStandby: true, observed: true, systemID: 9999},
			},
			in:           identityInputs{},
			wantKept:     []string{"p0", "p1"},
			wantConflict: true,
		},
		{
			name: "foreign standby fenced regardless of LSN",
			views: []instanceView{
				{pod: "p1", isStandby: true, observed: true, systemID: 4242, timeline: 1, receivedLSN: 300},
				{pod: "p2", isStandby: true, observed: true, systemID: 9999, timeline: 1, receivedLSN: 500},
			},
			in:         identityInputs{systemID: 4242, timeline: 1},
			wantKept:   []string{"p1"},
			wantFenced: 1,
		},
		{
			name: "post-latch, a standby reporting no id is identity-unknown: fenced",
			views: []instanceView{
				{pod: "p1", isStandby: true, observed: true, systemID: 4242, timeline: 1},
				{pod: "p2", isStandby: true, observed: true, systemID: 0, timeline: 1},
			},
			in:         identityInputs{systemID: 4242, timeline: 1},
			wantKept:   []string{"p1"},
			wantFenced: 1,
		},
		{
			name: "ahead-timeline standby fenced; behind-timeline standby kept",
			views: []instanceView{
				{pod: "p1", isStandby: true, observed: true, systemID: 4242, timeline: 1}, // behind recorded 2
				{pod: "p2", isStandby: true, observed: true, systemID: 4242, timeline: 3}, // ahead
			},
			in:         identityInputs{systemID: 4242, timeline: 2},
			wantKept:   []string{"p1"},
			wantFenced: 1,
		},
		{
			name: "committed target mid-promotion (ahead timeline, unsettled role) stays kept",
			views: []instanceView{
				{pod: "p1", observed: true, systemID: 4242, timeline: 2}, // promoting: no settled role
				{pod: "p2", isStandby: true, observed: true, systemID: 4242, timeline: 1},
			},
			in:       identityInputs{systemID: 4242, timeline: 1, committed: "p1"},
			wantKept: []string{"p1", "p2"},
		},
		{
			name: "committed target on a foreign lineage is NOT kept: refuse to drive it",
			views: []instanceView{
				{pod: "p1", isStandby: true, observed: true, systemID: 9999, timeline: 1},
			},
			in:         identityInputs{systemID: 4242, timeline: 1, committed: "p1"},
			wantKept:   []string{},
			wantFenced: 1,
		},
		{
			name: "foreign claimant: rogue, dropped, and never blocks the lineage's own election",
			views: []instanceView{
				{pod: "p0", isPrimary: true, ready: true, observed: true, systemID: 9999, timeline: 5, receivedLSN: 900},
				{pod: "p1", isStandby: true, ready: true, observed: true, systemID: 4242, timeline: 1, receivedLSN: 100},
			},
			in:         identityInputs{systemID: 4242, timeline: 1},
			wantKept:   []string{"p1"},
			wantRogues: []string{"p0"},
			wantFenced: 1,
		},
		{
			name: "self-promoted claimant (ahead timeline, untrusted): rogue but kept, so it blocks election",
			views: []instanceView{
				{pod: "p0", isPrimary: true, ready: true, observed: true, systemID: 4242, timeline: 3},
				{pod: "p1", isStandby: true, ready: true, observed: true, systemID: 4242, timeline: 1},
			},
			in:         identityInputs{systemID: 4242, timeline: 1},
			wantKept:   []string{"p0", "p1"},
			wantRogues: []string{"p0"},
			wantFenced: 1,
		},
		{
			name: "claimant reporting no id after latch: rogue but kept",
			views: []instanceView{
				{pod: "p0", isPrimary: true, observed: true, systemID: 0, timeline: 1},
			},
			in:         identityInputs{systemID: 4242, timeline: 1},
			wantKept:   []string{"p0"},
			wantRogues: []string{"p0"},
			wantFenced: 1,
		},
		{
			name: "recognized current primary with clean identity: kept, not rogue",
			views: []instanceView{
				{pod: "p0", isPrimary: true, ready: true, observed: true, systemID: 4242, timeline: 2},
			},
			in:       identityInputs{systemID: 4242, timeline: 2, current: "p0"},
			wantKept: []string{"p0"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := assessIdentity(tc.views, tc.in)
			if !equal(keptPods(got), tc.wantKept) {
				t.Fatalf("kept = %v, want %v", keptPods(got), tc.wantKept)
			}
			if !equal(got.rogues, tc.wantRogues) {
				t.Fatalf("rogues = %v, want %v", got.rogues, tc.wantRogues)
			}
			if len(got.fenced) != tc.wantFenced {
				t.Fatalf("fenced = %v, want %d entries", got.fenced, tc.wantFenced)
			}
			if got.conflict != tc.wantConflict {
				t.Fatalf("conflict = %v, want %v", got.conflict, tc.wantConflict)
			}
		})
	}
}

// The compositions the codex review flagged: the fences must change the
// election outcome, and the committed-target exemption must keep a
// mid-promotion handshake drivable.
func TestIdentityFencingChangesTheElection(t *testing.T) {
	t.Run("fresh election never picks a matching-id standby on an ahead timeline", func(t *testing.T) {
		views := []instanceView{
			{pod: "p1", ready: true, isStandby: true, observed: true, systemID: 4242, timeline: 1, receivedLSN: 100},
			{pod: "p2", ready: true, isStandby: true, observed: true, systemID: 4242, timeline: 3, receivedLSN: 500},
		}
		a := assessIdentity(views, identityInputs{systemID: 4242, timeline: 1})
		d := evaluateFailover(a.kept, "")
		if d.targetPrimary != "p1" {
			t.Fatalf("elected %q, want the clean standby p1 (the ahead-timeline one must be fenced)", d.targetPrimary)
		}
	})
	t.Run("committed target stays drivable mid-promotion with its timeline already bumped", func(t *testing.T) {
		// pg_promote bumps the timeline before the role flips: the target
		// reports an unsettled role on an AHEAD timeline. Dropping it would
		// strand the handshake forever.
		views := []instanceView{
			{pod: "p1", host: testHostIP, observed: true, systemID: 4242, timeline: 2},
		}
		a := assessIdentity(views, identityInputs{systemID: 4242, timeline: 1, committed: "p1"})
		d := evaluateFailover(a.kept, "p1")
		if d.targetPrimary != "p1" {
			t.Fatalf("decision = %+v, want the committed target p1 to keep being driven", d)
		}
	})
	t.Run("a sole foreign claimant is not honored and nothing else is promoted into it", func(t *testing.T) {
		views := []instanceView{
			{pod: "p0", ready: true, isPrimary: true, observed: true, systemID: 9999, timeline: 7, receivedLSN: 900},
		}
		a := assessIdentity(views, identityInputs{systemID: 4242, timeline: 1})
		if !a.rogue("p0") {
			t.Fatal("the foreign claimant must be rogue (never CurrentPrimary)")
		}
		d := evaluateFailover(a.kept, "")
		if d.warranted || d.targetPrimary != "" {
			t.Fatalf("decision = %+v, want no election at all", d)
		}
	})
}

func TestParseLatchedID(t *testing.T) {
	if id, err := parseLatchedID(""); id != 0 || err != nil {
		t.Fatalf("empty = (%d, %v), want (0, nil)", id, err)
	}
	if id, err := parseLatchedID("4242"); id != 4242 || err != nil {
		t.Fatalf("4242 = (%d, %v)", id, err)
	}
	if _, err := parseLatchedID("not-a-number"); err == nil {
		t.Fatal("a malformed latched id must be an error (fail closed), not zero")
	}
}
