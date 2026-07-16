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
	"context"
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard2/operator/internal/pb/pgshardv1"
	"github.com/andrew01234567890/pgshard2/operator/test/fakes"
)

// An agent that returns a well-formed but empty GetStatusResponse must not
// panic the reconcile — the operator treats an empty status as unreachable.
func TestPollAgentRejectsEmptyStatus(t *testing.T) {
	agent, err := fakes.NewFakeAgent()
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Stop()
	agent.SetEmptyStatus(true)

	client, err := agent.Client()
	if err != nil {
		t.Fatal(err)
	}
	r := &PgShardShardReconciler{
		dialAgent: func(string, int32) (pgshardv1.AgentServiceClient, error) { return client, nil },
	}
	if _, err := r.pollAgent(context.Background(), "1.2.3.4"); err == nil {
		t.Fatal("empty status must be reported as an error, not dereferenced")
	}
}
