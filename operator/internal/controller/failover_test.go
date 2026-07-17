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
		name          string
		instances     []instanceView
		wantWarranted bool
		wantTarget    string
		wantWait      bool
	}{
		{
			name: "healthy primary: no failover",
			instances: []instanceView{
				{pod: "p0", ready: true, isPrimary: true, observed: true},
				{pod: "p1", ready: true, observed: true},
			},
		},
		{
			name: "old primary relinquished the role: elect most-advanced ready replica",
			instances: []instanceView{
				{pod: "p0", ready: false, observed: true}, // demoted to standby
				{pod: "p1", ready: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantTarget:    "p2",
		},
		{
			name: "old primary pod gone from the set: elect most-advanced ready replica",
			instances: []instanceView{
				{pod: "p1", ready: true, receivedLSN: 100, observed: true},
				{pod: "p2", ready: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantTarget:    "p2",
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
			wantWarranted: true,
			wantWait:      true,
		},
		{
			name: "an instance is unobserved: wait, its state may be a live primary",
			instances: []instanceView{
				{pod: "p0", observed: false}, // poll failed — unknown, not down
				{pod: "p1", ready: true, receivedLSN: 200, observed: true},
			},
			wantWarranted: true,
			wantWait:      true,
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
			got := evaluateFailover(tc.instances)
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
