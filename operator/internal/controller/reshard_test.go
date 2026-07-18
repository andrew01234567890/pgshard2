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

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/topology"
)

const (
	subRange  = "40-80"
	lowerHalf = "40-60"
	upperHalf = "60-80"
)

func mustRange(t *testing.T, s string) topology.KeyRange {
	t.Helper()
	r, err := topology.ParseKeyRange(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return r
}

func TestToRange(t *testing.T) {
	full, err := toRange(pgshardv1alpha1.KeyRange{})
	if err != nil {
		t.Fatalf("empty range: %v", err)
	}
	if !full.IsFull() {
		t.Errorf("empty KeyRange should be the full range, got %s", full)
	}

	r, err := toRange(pgshardv1alpha1.KeyRange{Start: "40", End: "80"})
	if err != nil {
		t.Fatalf("40-80: %v", err)
	}
	if got := r.String(); got != subRange {
		t.Errorf("toRange(40,80) = %s, want %s", got, subRange)
	}
}

func TestValidateReshardPartition(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		targets []string
		wantErr bool
	}{
		{"even split of a sub-range", subRange, []string{lowerHalf, upperHalf}, false},
		{"four-way split", subRange, []string{"40-50", "50-60", "60-70", "70-80"}, false},
		{"full-range split", "-", []string{"-80", "80-"}, false},
		{"unsorted input accepted", subRange, []string{upperHalf, lowerHalf}, false},
		{"too few parts", subRange, []string{subRange}, true},
		{"gap", subRange, []string{"40-50", upperHalf}, true},
		{"overlap", subRange, []string{lowerHalf, "50-80"}, true},
		{"wrong start", subRange, []string{"41-60", upperHalf}, true},
		{"wrong end", subRange, []string{lowerHalf, "60-7f"}, true},
		{"end past source", subRange, []string{lowerHalf, "60-90"}, true},
		{"unbounded non-final", subRange, []string{"40-", upperHalf}, true},
		{"final bounded but source open", "-", []string{"-80", "80-c0"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := mustRange(t, tt.source)
			targets := make([]topology.KeyRange, len(tt.targets))
			for i, s := range tt.targets {
				targets[i] = mustRange(t, s)
			}
			err := validateReshardPartition(source, targets)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateReshardPartition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
