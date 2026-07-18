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
				{pod: "p1", host: "10.0.0.3", observed: true},
			},
			committedTarget: "p1",
			wantWarranted:   true,
			wantTarget:      "p1",
		},
		{
			name: "no committed target and only a not-ready instance: nothing to elect",
			instances: []instanceView{
				{pod: "p1", host: "10.0.0.3", observed: true}, // not-ready, not committed
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
