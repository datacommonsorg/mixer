// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package golden

import (
	"context"
	"path"
	"runtime"
	"sort"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
)

const (
	// Number of matches to validate for tests.
	NUM_MATCHES = 20
)

func TestGetNodeProps(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()

	for _, c := range nodePropsTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return client.GetNodeProps(ctx, c.ids, c.out)
		})
	}
}

func TestGetNodeOutEdgesByID(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()

	for _, c := range nodeOutEdgesByIDTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			actual, err := client.GetNodeEdgesByID(ctx, c.ids, c.arc, c.offset)
			if err != nil {
				return nil, err
			}
			return simplifyNodes(actual), nil
		})
	}
}

func TestGetNodeInEdgesByID(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()

	for _, c := range nodeInEdgesByIDTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			actual, err := client.GetNodeEdgesByID(ctx, c.ids, c.arc, c.offset)
			if err != nil {
				return nil, err
			}
			return simplifyNodes(actual), nil
		})
	}
}

func TestGetObservations(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()

	for _, c := range observationsTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			actual, err := client.GetObservations(ctx, c.variables, c.entities)
			if err != nil {
				return nil, err
			}
			sortObservations(actual)
			return actual, nil
		})
	}
}

func TestGetObservationsContainedInPlace(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()

	for _, c := range observationsContainedInPlaceTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			actual, err := client.GetObservationsContainedInPlace(ctx, c.variables, c.containedInPlace)
			if err != nil {
				return nil, err
			}
			sortObservations(actual)
			return actual, nil
		})
	}
}

func TestSearchNodes(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()

	for _, c := range searchNodesTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			actual, err := client.SearchNodes(ctx, c.query, c.types)
			if err != nil {
				return nil, err
			}
			return simplifySearchNodes(actual), nil
		})
	}
}

func TestResolveByID(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()

	for _, c := range resolveByIDTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return client.ResolveByID(ctx, c.nodes, c.in, c.out)
		})
	}
}

// runQueryGoldenTest is a helper function that performs the golden file validation.
func runQueryGoldenTest(t *testing.T, goldenFile string, fn goldenTestFunc) {
	t.Helper()

	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	actual, err := fn(ctx)
	if err != nil {
		t.Fatalf("test function error (%v): %v", goldenFile, err)
	}

	got, err := test.StructToJSON(actual)
	if err != nil {
		t.Fatalf("StructToJSON error (%v): %v", goldenFile, err)
	}

	if test.GenerateGolden {
		err = test.WriteGolden(got, goldenDir, goldenFile)
		if err != nil {
			t.Fatalf("WriteGolden error (%v): %v", goldenFile, err)
		}
		// Exit here to avoid comparison if we're regenerating golden files
		return
	}

	want, err := test.ReadGolden(goldenDir, goldenFile)
	if err != nil {
		t.Fatalf("ReadGolden error (%v): %v", goldenFile, err)
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("%v payload mismatch (-want +got):\n%s", goldenFile, diff)
	}
}

// simplifySearchNodes simplifies search results for goldens.
func simplifySearchNodes(results []*spanner.SearchNode) []*spanner.SearchNode {
	if len(results) > NUM_MATCHES {
		results = results[:NUM_MATCHES]
	}

	for _, r := range results {
		r.Score = 0
	}

	return results
}

// simplifyNodes simplifies Node results for goldens.
func simplifyNodes(results map[string][]*spanner.Edge) map[string][]*spanner.Edge {
	filtered := map[string][]*spanner.Edge{}
	for subject_id, edges := range results {
		if len(edges) > NUM_MATCHES {
			edges = edges[:NUM_MATCHES]
		}
		filtered[subject_id] = edges
	}
	return filtered
}

// sortObservations sorts Observations by variable, entity, facet (primary key) to ensure deterministic order in tests.
// The final Observation responses will be sorted later based on facet rank.
func sortObservations(results []*spanner.Observation) {
	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]

		if a.VariableMeasured != b.VariableMeasured {
			return a.VariableMeasured < b.VariableMeasured
		}

		if a.ObservationAbout != b.ObservationAbout {
			return a.ObservationAbout < b.ObservationAbout
		}

		return a.FacetId < b.FacetId
	})
}
