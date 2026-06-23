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

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

func TestMultiEntityGetObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityObservationsTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityObservationsQuery(c.variables, c.entities, c.date, spanner.DefaultTableConfig())
			})
		})
	}
}

func TestMultiEntityGetStatVarsByEntityQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityCheckVariableExistenceTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityStatVarsByEntityQuery(c.variables, c.entities, spanner.DefaultTableConfig())
			})
		})
	}
}

func TestMultiEntityCheckGroupPlaceExistenceQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityCheckGroupPlaceExistenceTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityGroupPlaceExistenceQuery(c.variableGroups, c.entities, c.predicate, spanner.DefaultTableConfig()), nil
			})
		})
	}
}

func TestMultiEntityGetObservationsContainedInPlaceQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityObservationsContainedInPlaceTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityObservationsContainedInPlaceQuery(c.variables, &v2.ContainedInPlace{
					Ancestor:       c.ancestor,
					ChildPlaceType: c.childPlaceType,
				}, c.date, spanner.DefaultTableConfig())
			})
		})
	}
}

func TestMultiEntityGetStatVarGroupNodeQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityStatVarGroupNodeTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityStatVarGroupNodeQuery(c.nodes, c.includeDefinitions, spanner.DefaultTableConfig()), nil
			})
		})
	}
}

func TestMultiEntityGetFilteredSVGChildrenQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityFilteredSVGChildrenTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityFilteredSVGChildrenQuery(c.template, c.node, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence, c.includeDefinitions, spanner.DefaultTableConfig()), nil
			})
		})
	}
}

// TestMultiEntityGetFilteredTopicChildrenQuery returns a query to get Topic children using multi-entity TimeSeries filters.
func TestMultiEntityGetFilteredTopicChildrenQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityFilteredTopicTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityFilteredTopicChildrenQuery(c.nodes, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence, spanner.DefaultTableConfig()), nil
			})
		})
	}
}

func TestMultiEntityGetSdmxObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntitySdmxObservationsTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				stmt, err := spanner.GetMultiEntitySdmxObservationsQuery(c.constraints, c.entityMappings, spanner.DefaultTableConfig())
				return stmt, err
			})
		})
	}
}

func TestMultiEntityGetSdmxObservationsQuery_Validation(t *testing.T) {
	// Case 1: Valid alphanumeric keys
	constraints := map[string]*sdmxpb.ConstraintList{
		"variableMeasured":  {Values: []string{"var1"}},
		"observationAbout":  {Values: []string{"wikidataId/Q119158"}},
		"provenance":        {Values: []string{"dc/base/INPE_Fire_Event_Count"}},
		"observationPeriod": {Values: []string{"P1Y"}},
	}
	_, err := spanner.GetMultiEntitySdmxObservationsQuery(constraints, nil, spanner.DefaultTableConfig())
	if err != nil {
		t.Errorf("expected no error for valid constraint keys, got %v", err)
	}

	// Case 2: Invalid key containing SQL injection payload
	badConstraints1 := map[string]*sdmxpb.ConstraintList{
		"variableMeasured": {Values: []string{"var1"}},
		"unit') OR 1=1 --": {Values: []string{"Percent"}},
	}
	_, err = spanner.GetMultiEntitySdmxObservationsQuery(badConstraints1, nil, spanner.DefaultTableConfig())
	if err == nil {
		t.Error("expected error for constraint key containing SQL injection payload, got nil")
	}

	// Case 3: Invalid key containing spaces
	badConstraints2 := map[string]*sdmxpb.ConstraintList{
		"variableMeasured": {Values: []string{"var1"}},
		"invalid key":      {Values: []string{"value"}},
	}
	_, err = spanner.GetMultiEntitySdmxObservationsQuery(badConstraints2, nil, spanner.DefaultTableConfig())
	if err == nil {
		t.Error("expected error for constraint key containing spaces, got nil")
	}
}

func TestMultiEntityGetSdmxAvailabilityQuery(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name        string
		componentID string
		golden      string
	}{
		{
			name:        "observation about",
			componentID: "observationAbout",
			golden:      "get_sdmx_availability_observation_about.sql",
		},
		{
			name:        "provenance",
			componentID: "provenance",
			golden:      "get_sdmx_availability_provenance.sql",
		},
		{
			name:        "unit",
			componentID: "unit",
			golden:      "get_sdmx_availability_unit.sql",
		},
		{
			name:        "measurement method",
			componentID: "measurementMethod",
			golden:      "get_sdmx_availability_measurement_method.sql",
		},
		{
			name:        "observation period",
			componentID: "observationPeriod",
			golden:      "get_sdmx_availability_observation_period.sql",
		},
		{
			name:        "variable measured",
			componentID: "variableMeasured",
			golden:      "get_sdmx_availability_variable_measured.sql",
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			runQueryBuilderGoldenTest(t, c.golden, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntitySdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
					ComponentId: c.componentID,
					Constraints: map[string]*sdmxpb.ConstraintList{
						"variableMeasured": {Values: []string{"Count_Person", "Count_Household"}},
					},
				}, spanner.DefaultTableConfig())
			})
		})
	}
}

func TestMultiEntityGetSdmxAvailabilityQuery_Validation(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *sdmxpb.SdmxAvailabilityQuery
	}{
		{
			name: "nil request",
		},
		{
			name: "missing variable measured",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.ConstraintList{},
			},
		},
		{
			name: "nil variable measured constraint",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.ConstraintList{
					"variableMeasured": nil,
				},
			},
		},
		{
			name: "empty variable measured values",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.ConstraintList{
					"variableMeasured": &sdmxpb.ConstraintList{},
				},
			},
		},
		{
			name: "unsupported component",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "TIME_PERIOD",
				Constraints: map[string]*sdmxpb.ConstraintList{
					"variableMeasured": {Values: []string{"Count_Person"}},
				},
			},
		},
		{
			name: "unsupported constraint",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.ConstraintList{
					"variableMeasured": {Values: []string{"Count_Person"}},
					"observationAbout": {Values: []string{"country/USA"}},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := spanner.GetMultiEntitySdmxAvailabilityQuery(tc.req, spanner.DefaultTableConfig())
			if err == nil {
				t.Fatal("GetMultiEntitySdmxAvailabilityQuery() error = nil, want error")
			}
		})
	}
}
