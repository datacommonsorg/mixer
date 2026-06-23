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
	"strings"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

func defaultMultiEntityStatements(t *testing.T) *spanner.MultiEntityStatements {
	t.Helper()
	stmts, err := spanner.NewMultiEntityStatements(spanner.DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}
	return stmts
}

func TestMultiEntityGetObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityObservationsTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				return spanner.GetMultiEntityObservationsQuery(c.variables, c.entities, c.date, defaultMultiEntityStatements(t))
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
				return spanner.GetMultiEntityStatVarsByEntityQuery(c.variables, c.entities, defaultMultiEntityStatements(t))
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
				return spanner.GetMultiEntityGroupPlaceExistenceQuery(c.variableGroups, c.entities, c.predicate, defaultMultiEntityStatements(t))
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
				}, c.date, defaultMultiEntityStatements(t))
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
				return spanner.GetMultiEntityStatVarGroupNodeQuery(c.nodes, c.includeDefinitions, defaultMultiEntityStatements(t))
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
				return spanner.GetMultiEntityFilteredSVGChildrenQuery(c.template, c.node, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence, c.includeDefinitions, defaultMultiEntityStatements(t))
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
				return spanner.GetMultiEntityFilteredTopicChildrenQuery(c.nodes, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence, defaultMultiEntityStatements(t))
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
				stmt, err := spanner.GetMultiEntitySdmxObservationsQuery(c.constraints, c.entityMappings, defaultMultiEntityStatements(t))
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
	_, err := spanner.GetMultiEntitySdmxObservationsQuery(constraints, nil, defaultMultiEntityStatements(t))
	if err != nil {
		t.Errorf("expected no error for valid constraint keys, got %v", err)
	}

	// Case 2: Invalid key containing SQL injection payload
	badConstraints1 := map[string]*sdmxpb.ConstraintList{
		"variableMeasured": {Values: []string{"var1"}},
		"unit') OR 1=1 --": {Values: []string{"Percent"}},
	}
	_, err = spanner.GetMultiEntitySdmxObservationsQuery(badConstraints1, nil, defaultMultiEntityStatements(t))
	if err == nil {
		t.Error("expected error for constraint key containing SQL injection payload, got nil")
	}

	// Case 3: Invalid key containing spaces
	badConstraints2 := map[string]*sdmxpb.ConstraintList{
		"variableMeasured": {Values: []string{"var1"}},
		"invalid key":      {Values: []string{"value"}},
	}
	_, err = spanner.GetMultiEntitySdmxObservationsQuery(badConstraints2, nil, defaultMultiEntityStatements(t))
	if err == nil {
		t.Error("expected error for constraint key containing spaces, got nil")
	}
}

func TestMultiEntityQueryBuildersUseCustomTableConfig(t *testing.T) {
	cfg := spanner.DefaultTableConfig()
	cfg.TimeSeriesTable = "CustomTsTable"
	cfg.ObservationTable = "CustomObsTable"
	cfg.TimeSeriesByEntity2Index = "CustomEntity2Index"
	cfg.TimeSeriesByEntity3Index = "CustomEntity3Index"
	stmts, err := spanner.NewMultiEntityStatements(cfg)
	if err != nil {
		t.Fatal(err)
	}

	obsStmt, err := spanner.GetMultiEntityObservationsQuery(
		[]string{"Count_Person"},
		[]string{"geoId/06"},
		"",
		stmts,
	)
	if err != nil {
		t.Fatalf("GetMultiEntityObservationsQuery() returned error: %v", err)
	}
	assertSQLContains(t, obsStmt.SQL, "CustomObsTable", "CustomTsTable")

	existenceStmt, err := spanner.GetMultiEntityStatVarsByEntityQuery(
		[]string{"Count_Person"},
		[]string{"geoId/06"},
		stmts,
	)
	if err != nil {
		t.Fatalf("GetMultiEntityStatVarsByEntityQuery() returned error: %v", err)
	}
	assertSQLContains(t, existenceStmt.SQL, "CustomTsTable", "CustomEntity2Index", "CustomEntity3Index")

	filteredTopicStmt, err := spanner.GetMultiEntityFilteredTopicChildrenQuery(
		[]string{"dc/topic/Test"},
		nil,
		"",
		2,
		stmts,
	)
	if err != nil {
		t.Fatalf("GetMultiEntityFilteredTopicChildrenQuery() returned error: %v", err)
	}
	assertSQLContains(t, filteredTopicStmt.SQL, "CustomTsTable", "CustomEntity2Index", "CustomEntity3Index")

	availabilityStmt, err := spanner.GetMultiEntitySdmxAvailabilityQuery(&pb.SdmxAvailabilityQuery{
		ComponentId: "observationAbout",
		Constraints: map[string]*pb.ConstraintList{
			"variableMeasured": {Values: []string{"Count_Person"}},
		},
	}, stmts)
	if err != nil {
		t.Fatalf("GetMultiEntitySdmxAvailabilityQuery() returned error: %v", err)
	}
	assertSQLContains(t, availabilityStmt.SQL, "CustomTsTable")
}

func assertSQLContains(t *testing.T, sql string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(sql, value) {
			t.Fatalf("SQL does not contain %q:\n%s", value, sql)
		}
	}
}

func TestMultiEntityQueryBuildersRejectNilStatements(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{
			name: "GetMultiEntityObservationsQuery",
			call: func() error {
				_, err := spanner.GetMultiEntityObservationsQuery([]string{"Count_Person"}, []string{"geoId/06"}, "", nil)
				return err
			},
		},
		{
			name: "GetMultiEntityStatVarsByEntityQuery",
			call: func() error {
				_, err := spanner.GetMultiEntityStatVarsByEntityQuery([]string{"Count_Person"}, []string{"geoId/06"}, nil)
				return err
			},
		},
		{
			name: "GetMultiEntityGroupPlaceExistenceQuery",
			call: func() error {
				_, err := spanner.GetMultiEntityGroupPlaceExistenceQuery([]string{"dc/g/Test"}, []string{"geoId/06"}, "memberOf", nil)
				return err
			},
		},
		{
			name: "GetMultiEntityObservationsContainedInPlaceQuery",
			call: func() error {
				_, err := spanner.GetMultiEntityObservationsContainedInPlaceQuery(
					[]string{"Count_Person"},
					&v2.ContainedInPlace{Ancestor: "country/USA", ChildPlaceType: "County"},
					"",
					nil,
				)
				return err
			},
		},
		{
			name: "GetMultiEntityStatVarGroupNodeQuery",
			call: func() error {
				_, err := spanner.GetMultiEntityStatVarGroupNodeQuery([]string{"dc/g/Test"}, false, nil)
				return err
			},
		},
		{
			name: "GetMultiEntityFilteredSVGChildrenQuery",
			call: func() error {
				_, err := spanner.GetMultiEntityFilteredSVGChildrenQuery("SV", "dc/g/Test", nil, "", 0, false, nil)
				return err
			},
		},
		{
			name: "GetMultiEntityFilteredTopicChildrenQuery",
			call: func() error {
				_, err := spanner.GetMultiEntityFilteredTopicChildrenQuery([]string{"dc/topic/Test"}, nil, "", 0, nil)
				return err
			},
		},
		{
			name: "GetMultiEntitySdmxObservationsQuery",
			call: func() error {
				_, err := spanner.GetMultiEntitySdmxObservationsQuery(
					map[string]*pb.ConstraintList{"variableMeasured": {Values: []string{"Count_Person"}}},
					nil,
					nil,
				)
				return err
			},
		},
		{
			name: "GetMultiEntitySdmxAvailabilityQuery",
			call: func() error {
				_, err := spanner.GetMultiEntitySdmxAvailabilityQuery(&pb.SdmxAvailabilityQuery{
					ComponentId: "observationAbout",
					Constraints: map[string]*pb.ConstraintList{
						"variableMeasured": {Values: []string{"Count_Person"}},
					},
				}, nil)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("error = nil, want nil statements error")
			}
			if got := err.Error(); !strings.Contains(got, tc.name) || !strings.Contains(got, "stmts cannot be nil") {
				t.Fatalf("error = %q, want %q and %q", got, tc.name, "stmts cannot be nil")
			}
		})
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
				}, defaultMultiEntityStatements(t))
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
			_, err := spanner.GetMultiEntitySdmxAvailabilityQuery(tc.req, defaultMultiEntityStatements(t))
			if err == nil {
				t.Fatal("GetMultiEntitySdmxAvailabilityQuery() error = nil, want error")
			}
		})
	}
}
