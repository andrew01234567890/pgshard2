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
	"fmt"
	"slices"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard2/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard2/operator/internal/topology"
)

// toRange converts a CRD KeyRange (canonical hex bounds, empty = unbounded) to a
// topology.KeyRange for range math.
func toRange(kr pgshardv1alpha1.KeyRange) (topology.KeyRange, error) {
	return topology.ParseKeyRange(kr.Start + "-" + kr.End)
}

// validateReshardPartition checks that targets exactly partition source: at least
// two ranges, contiguous and non-overlapping, beginning at the source start and
// ending at the source end. A reshard that got this wrong would create shards
// whose ranges do not cover the source — silently losing or double-owning keys —
// so it is rejected before any shard is created.
func validateReshardPartition(source topology.KeyRange, targets []topology.KeyRange) error {
	if len(targets) < 2 {
		return fmt.Errorf("a reshard must split the source into at least two ranges, got %d", len(targets))
	}

	sorted := make([]topology.KeyRange, len(targets))
	copy(sorted, targets)
	slices.SortFunc(sorted, func(a, b topology.KeyRange) int { return cmp.Compare(a.Start(), b.Start()) })

	if sorted[0].Start() != source.Start() {
		return fmt.Errorf("target ranges must start at the source start %s, but the first target is %s", source, sorted[0])
	}

	for i := 0; i < len(sorted)-1; i++ {
		end, closed := sorted[i].End()
		if !closed {
			return fmt.Errorf("target range %s is unbounded but is not the last range", sorted[i])
		}
		if end != sorted[i+1].Start() {
			return fmt.Errorf("target ranges are not contiguous: %s is followed by %s (gap or overlap)", sorted[i], sorted[i+1])
		}
	}

	last := sorted[len(sorted)-1]
	lastEnd, lastClosed := last.End()
	srcEnd, srcClosed := source.End()
	if lastClosed != srcClosed || (lastClosed && lastEnd != srcEnd) {
		return fmt.Errorf("target ranges must end at the source end %s, but the last target is %s", source, last)
	}

	return nil
}
