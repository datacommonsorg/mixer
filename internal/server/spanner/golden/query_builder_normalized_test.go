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
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

func TestNormalizedGetObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range normalizedObservationsTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNormalizedObservationsQuery(c.variables, c.entities), nil
		})
	}
}

func TestNormalizedGetStatVarsByEntityQuery(t *testing.T) {
	t.Parallel()

	for _, c := range checkVariableExistenceTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNormalizedStatVarsByEntityQuery(c.variables, c.entities)
		})
	}
}

func TestNormalizedGetObservationsContainedInPlaceQuery(t *testing.T) {
	t.Parallel()

	for _, c := range getObservationsContainedInPlaceTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNormalizedObservationsContainedInPlaceQuery(c.variables, &v2.ContainedInPlace{
				Ancestor:         c.ancestor,
				ChildPlaceType: c.childPlaceType,
			}), nil
		})
	}
}

func TestGetSdmxObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range sdmxObservationsTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetSdmxObservationsQuery(c.constraints), nil
		})
	}
}

