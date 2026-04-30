// Copyright 2025 Google LLC
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
	"testing"

	cloudSpanner "cloud.google.com/go/spanner"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
)

func TestGetNodePropsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range nodePropsTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNodePropsQuery(c.ids, c.out), nil
		})
	}
}

func TestGetNodeOutEdgesByIDQuery(t *testing.T) {
	t.Parallel()

	for _, c := range nodeOutEdgesByIDTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, datasources.DefaultPageSize, c.offset), nil
		})
	}
}

func TestGetNodeInEdgesByIDQuery(t *testing.T) {
	t.Parallel()

	for _, c := range nodeInEdgesByIDTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, datasources.DefaultPageSize, c.offset), nil
		})
	}
}

func TestGetObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range observationsTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetObservationsQuery(c.variables, c.entities), nil
		})
	}
}

func TestGetObservationsContainedInPlaceQuery(t *testing.T) {
	t.Parallel()

	for _, c := range observationsContainedInPlaceTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetObservationsContainedInPlaceQuery(c.variables, c.containedInPlace), nil
		})
	}
}

func TestFilterStatVarsByEntityQuery(t *testing.T) {
	t.Parallel()

	for _, c := range filterStatVarsByEntityTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.FilterStatVarsByEntityQuery(c.variables, c.entities)
		})
	}
}

func TestFilterStatVarsByEntityQueryError(t *testing.T) {
	t.Parallel()

	_, err := spanner.FilterStatVarsByEntityQuery([]string{}, []string{})
	if err == nil {
		t.Errorf("FilterStatVarsByEntityQuery() expected error, got nil")
	}
}

func TestSearchNodesQuery(t *testing.T) {
	t.Parallel()

	for _, c := range searchNodesTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.SearchNodesQuery(c.query, c.types), nil
		})
	}
}

func TestResolveByIDQuery(t *testing.T) {
	t.Parallel()

	for _, c := range resolveByIDTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.ResolveByIDQuery(c.nodes, c.in, c.out), nil
		})
	}
}

func TestSparqlQuery(t *testing.T) {
	t.Parallel()

	for _, c := range sparqlTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.SparqlQuery(c.nodes, c.queries, c.opts)
		})
	}
}

func TestGetProvenanceSummaryQuery(t *testing.T) {
	t.Parallel()

	for _, c := range provenanceSummaryTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetCacheDataQuery(spanner.TypeProvenanceSummary, c.variables), nil
		})
	}
}

func TestGetEventCollectionDateQuery(t *testing.T) {
	t.Parallel()

	for _, c := range eventCollectionDateTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetEventCollectionDateQuery(c.placeDcid, c.eventType), nil
		})
	}
}

func TestGetEventCollectionDcidsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range eventCollectionDcidsTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetEventCollectionDcidsQuery(c.placeDcid, c.eventType, c.date), nil
		})
	}
}

func TestGetStatVarGroupNodeQuery(t *testing.T) {
	t.Parallel()

	for _, c := range getStatVarGroupNodeTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetStatVarGroupNodeQuery(c.nodes), nil
		})
	}
}

func TestGetSVGChildrenQuery(t *testing.T) {
	t.Parallel()

	for _, c := range getSVGChildrenTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetSVGChildrenQuery(c.node), nil
		})
	}
}

func TestGetFilteredSVGChildren(t *testing.T) {
	t.Parallel()

	for _, c := range getFilteredSVGChildrenTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetFilteredSVGChildrenQuery(c.template, c.node, c.constrainedPlaces, c.constrainedImport, c.numEntitiesExistence), nil
		})
	}
}

func TestGetFilteredTopicChildren(t *testing.T) {
	t.Parallel()

	for _, c := range getFilteredTopicTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetFilteredTopicChildrenQuery(c.nodes, c.constrainedPlaces, c.constrainedImport, c.numEntitiesExistence), nil
		})
	}
}

func TestGetTermEmbeddingQuery(t *testing.T) {
	t.Parallel()

	for _, c := range embeddingFromQueryTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetTermEmbeddingQuery(c.modelName, c.searchLabel, c.taskType), nil
		})
	}
}

func TestVectorSearchQuery(t *testing.T) {
	t.Parallel()

	for _, c := range vectorSearchNodeTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.VectorSearchQuery(c.limit, c.embeddings, c.numLeaves, c.threshold, c.nodeTypes), nil
		})
	}
}

// runQueryBuilderGoldenTest is a helper function that performs the golden file validation.
func runQueryBuilderGoldenTest(t *testing.T, goldenFile string, fn goldenTestFunc) {
	t.Helper()

	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query_builder")

	actual, err := fn(ctx)
	if err != nil {
		t.Fatalf("test function error (%v): %v", goldenFile, err)
	}
	interpolated := spanner.InterpolateSQL(actual.(*cloudSpanner.Statement))

	if test.GenerateGolden {
		err := test.WriteGolden(interpolated, goldenDir, goldenFile)
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

	if diff := cmp.Diff(want, interpolated); diff != "" {
		t.Errorf("%v payload mismatch (-want +got):\n%s", goldenFile, diff)
	}
}


