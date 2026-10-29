// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emulator

import (
	"context"
	"testing"

	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// childQueryConfigs covers both sources of child entities, LinkedEdge and Edge. The seed data holds the same
// containment in both, so every child entity test expects the same results from each.
var childQueryConfigs = []struct {
	name        string
	queryConfig mixerspanner.QueryConfig
}{
	{name: "LinkedEdge", queryConfig: mixerspanner.QueryConfig{UseMaterializedLinkedEdge: true}},
	{name: "Edge", queryConfig: mixerspanner.QueryConfig{}},
}

func TestGetChildTypes(t *testing.T) {
	s := requireSuite(t)
	tests := []struct {
		name      string
		ancestors []string
		want      map[string][]string
	}{
		{
			name:      "distinct child types per ancestor, ancestors without children omitted",
			ancestors: []string{"northamerica", "europe", "country/USA"},
			want: map[string][]string{
				"northamerica": {"Country"},
				"europe":       {"Country"},
			},
		},
		{
			name: "no ancestors",
			want: map[string][]string{},
		},
	}
	for _, cfg := range childQueryConfigs {
		client, err := s.newSpannerClient(context.Background(), cfg.queryConfig)
		if err != nil {
			t.Fatalf("newSpannerClient(%s) error = %v", cfg.name, err)
		}
		defer client.Close()
		for _, tc := range tests {
			t.Run(cfg.name+"/"+tc.name, func(t *testing.T) {
				got, err := client.GetChildTypes(context.Background(), tc.ancestors)
				if err != nil {
					t.Fatalf("GetChildTypes() error = %v", err)
				}
				// Types per ancestor come back in scan order, so compare them as sets.
				if diff := cmp.Diff(tc.want, got, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
					t.Errorf("GetChildTypes() mismatch (-want +got):\n%s", diff)
				}
			})
		}
	}
}
