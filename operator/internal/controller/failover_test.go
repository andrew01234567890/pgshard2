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
		expectedPrimary string
		wantWarranted   bool
		wantTarget      string
		wantWait        bool
	}{
		{
			name: "healthy primary: no failover",
			instances: []instanceView{
				{pod: "p0", ready: true, isPrimary: true, observed: true},
				{pod: "p1", ready: true, observed: true},
			},
			expectedPrimary: "p0",
		},
		{
			name: "old primary relinquished the role: elect most-advanced ready replica",
			instances: []instanceView{
				{pod: "p0", ready: false, observed: true}, // demoted to standby
				{pod: "p1", ready: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: true, receivedLSN: 200, observed: true},
			},
			expectedPrimary: "p0",
			wantWarranted:   true,
			wantTarget:      "p2",
		},
		{
			name: "old primary pod gone from the set: elect most-advanced ready replica",
			instances: []instanceView{
				{pod: "p1", ready: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: true, receivedLSN: 200, observed: true},
			},
			expectedPrimary: "p0", // absent from the set — confirmed gone
			wantWarranted:   true,
			wantTarget:      "p2",
		},
		{
			name: "tie on LSN broken by pod name",
			instances: []instanceView{
				{pod: "pb", ready: true, receivedLSN: 200, observed: true},
				{pod: "pa", ready: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantTarget:    "pa",
		},
		{
			name: "settling primary still claims the role: wait, never a second promote",
			instances: []instanceView{
				{pod: "p0", isPrimary: true, ready: false, observed: true},
				{pod: "p1", ready: true, receivedLSN: 200, observed: true},
			},
			expectedPrimary: "p0",
			wantWarranted:   true,
			wantWait:        true,
		},
		{
			name: "expected primary unobserved: wait, it may be a live primary we cannot see",
			instances: []instanceView{
				{pod: "p0", observed: false}, // poll failed — unknown, not down
				{pod: "p1", ready: true, receivedLSN: 200, observed: true},
			},
			expectedPrimary: "p0",
			wantWarranted:   true,
			wantWait:        true,
		},
		{
			name: "an unobserved replica (not the expected primary) does not veto election",
			instances: []instanceView{
				{pod: "p1", ready: true, receivedLSN: 200, observed: true},
				{pod: "p2", observed: false}, // a stuck/Pending replica, never primary
			},
			expectedPrimary: "p0", // the gone primary, absent from the set
			wantWarranted:   true,
			wantTarget:      "p1",
		},
		{
			name: "a not-ready standby is still receiving WAL: wait for it to drain",
			instances: []instanceView{
				{pod: "p1", ready: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: false, receivedLSN: 200, walReceiver: true, observed: true},
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "a drained not-ready standby holds more WAL: wait, do not elect a laggard",
			instances: []instanceView{
				{pod: "p1", ready: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: false, receivedLSN: 200, observed: true}, // drained, further ahead
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "wait while a candidate WAL receiver is still running",
			instances: []instanceView{
				{pod: "p1", ready: true, receivedLSN: 200, walReceiver: true, observed: true},
				{pod: "p2", ready: true, receivedLSN: 100, observed: true},
			},
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "primary down with no ready replica: not an electable failover",
			instances: []instanceView{
				{pod: "p0", ready: false, observed: true},
				{pod: "p1", ready: false, observed: true},
			},
		},
		{
			name:      "empty shard: nothing to do",
			instances: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evaluateFailover(tc.instances, tc.expectedPrimary)
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
