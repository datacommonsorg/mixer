// Copyright 2026 Google LLC
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
	"sort"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
)

func TestGetNormalizedObservations(t *testing.T) {
	client := test.NewNormalizedSpannerClient(t)
	t.Parallel()

	nc, err := spanner.NewNormalizedClient(client)
	if err != nil {
		t.Fatalf("NewNormalizedClient failed: %v", err)
	}

	for _, c := range normalizedObservationsTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			actual, err := nc.GetObservations(ctx, c.variables, c.entities)
			if err != nil {
				return nil, err
			}
			sortObservations(actual)
			return actual, nil
		})
	}
}

func TestCheckVariableExistence(t *testing.T) {
	client := test.NewNormalizedSpannerClient(t)
	t.Parallel()

	nc, err := spanner.NewNormalizedClient(client)
	if err != nil {
		t.Fatalf("NewNormalizedClient failed: %v", err)
	}

	for _, c := range checkVariableExistenceTestCases {
		goldenFile := c.golden + ".json"

		runQueryGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			actual, err := nc.CheckVariableExistence(ctx, c.variables, c.entities)
			if err != nil {
				return nil, err
			}
			sort2DStringSlice(actual)
			return actual, nil
		})
	}
}

func sort2DStringSlice(slice [][]string) {
	sort.Slice(slice, func(i, j int) bool {
		for k := 0; k < len(slice[i]) && k < len(slice[j]); k++ {
			if slice[i][k] != slice[j][k] {
				return slice[i][k] < slice[j][k]
			}
		}
		return len(slice[i]) < len(slice[j])
	})
}
