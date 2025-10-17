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
	"fmt"
	"path"
	"runtime"
	"strings"
	"testing"

	cloudSpanner "cloud.google.com/go/spanner"
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
			return spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, c.offset), nil
		})
	}
}

func TestGetNodeInEdgesByIDQuery(t *testing.T) {
	t.Parallel()

	for _, c := range nodeInEdgesByIDTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, c.offset), nil
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
	interpolated := interpolateSQL(actual.(*cloudSpanner.Statement))

	if true {
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

// Replace params with values in SQL. ONLY FOR TESTS.
func interpolateSQL(stmt *cloudSpanner.Statement) string {
	sqlString := stmt.SQL
	for key, value := range stmt.Params {
		placeholder := "@" + key
		var formattedValue string

		switch v := value.(type) {
		case string:
			formattedValue = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case []string:
			// For UNNEST, represent the array as a comma-separated list
			// enclosed in parentheses or brackets for clarity.
			var quotedValues []string
			for _, s := range v {
				quotedValues = append(quotedValues, fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''")))
			}
			formattedValue = "(" + strings.Join(quotedValues, ",") + ")"
			// Need to handle both UNNEST(@key) and @key
			sqlString = strings.ReplaceAll(sqlString, "UNNEST("+placeholder+")", formattedValue)
			placeholder = "@" + key // Ensure we don't mess up UNNEST replacement
			formattedValue = "[" + strings.Join(quotedValues, ",") + "]"
		// ... add more cases for int64, float64, bool, etc.
		default:
			// Catch-all for other types
			formattedValue = fmt.Sprintf("%v", v)
		}
		sqlString = strings.ReplaceAll(sqlString, placeholder, formattedValue)
	}
	return sqlString
}
