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
)

var normalizedObservationsTestCases = []struct {
	golden    string
	variables []string
	entities  []string
}{
	{
		golden:    "get_normalized_obs_basic",
		variables: []string{"Count_Person"},
		entities:  []string{"geoId/06"},
	},
	{
		golden:    "get_normalized_obs_multi",
		variables: []string{"Count_Person", "Count_Household"},
		entities:  []string{"geoId/06", "geoId/08"},
	},
}

func TestGetNormalizedObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range normalizedObservationsTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetNormalizedObservationsQuery(c.variables, c.entities), nil
		})
	}
}

var getTimeSeriesAttributesTestCases = []struct {
	golden string
	ids    []string
}{
	{
		golden: "get_timeseries_attributes_basic",
		ids:    []string{"123", "456"},
	},
}

func TestGetTimeSeriesAttributesQuery(t *testing.T) {
	t.Parallel()

	for _, c := range getTimeSeriesAttributesTestCases {
		goldenFile := c.golden + ".sql"

		runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
			return spanner.GetTimeSeriesAttributesQuery(c.ids), nil
		})
	}
}


