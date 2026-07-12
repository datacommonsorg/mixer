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

func TestMultiEntityGetObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityObservationsTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetObservationsQuery(c.variables, c.entities, c.date)
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetStatVarsByEntityQuery(c.variables, c.entities)
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetGroupPlaceExistenceQuery(c.variableGroups, c.entities, c.predicate)
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetObservationsContainedInPlaceQuery(c.variables, &v2.ContainedInPlace{
					Ancestor:       c.ancestor,
					ChildPlaceType: c.childPlaceType,
				}, c.date)
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetStatVarGroupNodeQuery(c.nodes, c.includeDefinitions)
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetFilteredSVGChildrenQuery(c.template, c.node, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence, c.includeDefinitions)
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetFilteredTopicChildrenQuery(c.nodes, c.constrainedPlaces, c.constrainedProvenance, c.numEntitiesExistence)
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				stmt, err := builder.GetSdmxObservationsQuery(c.constraints, c.entitySlotsByStatVar)
				return stmt, err
			})
		})
	}
}

func TestMultiEntityGetSdmxObservationsQuery_Validation(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}

	// Case 1: Valid alphanumeric keys
	constraints := map[string]*sdmxpb.ConstraintList{
		"variableMeasured":  {Values: []string{"var1"}},
		"observationAbout":  {Values: []string{"wikidataId/Q119158"}},
		"provenance":        {Values: []string{"dc/base/INPE_Fire_Event_Count"}},
		"observationPeriod": {Values: []string{"P1Y"}},
	}
	entitySlotsByStatVar := map[string]map[string]string{
		"var1": {"observationAbout": "entity1"},
	}
	_, err = builder.GetSdmxObservationsQuery(constraints, entitySlotsByStatVar)
	if err != nil {
		t.Errorf("expected no error for valid constraint keys, got %v", err)
	}

	// Case 2: Invalid key containing SQL injection payload
	badConstraints1 := map[string]*sdmxpb.ConstraintList{
		"variableMeasured": {Values: []string{"var1"}},
		"unit') OR 1=1 --": {Values: []string{"Percent"}},
	}
	_, err = builder.GetSdmxObservationsQuery(badConstraints1, nil)
	if err == nil {
		t.Error("expected error for constraint key containing SQL injection payload, got nil")
	}

	// Case 3: Invalid key containing spaces
	badConstraints2 := map[string]*sdmxpb.ConstraintList{
		"variableMeasured": {Values: []string{"var1"}},
		"invalid key":      {Values: []string{"value"}},
	}
	_, err = builder.GetSdmxObservationsQuery(badConstraints2, nil)
	if err == nil {
		t.Error("expected error for constraint key containing spaces, got nil")
	}

	// Case 4: Unsupported dynamic key should not fall back to facet JSON filtering.
	badConstraints3 := map[string]*sdmxpb.ConstraintList{
		"variableMeasured": {Values: []string{"var1"}},
		"customEntity":     {Values: []string{"value"}},
	}
	_, err = builder.GetSdmxObservationsQuery(badConstraints3, entitySlotsByStatVar)
	if err == nil {
		t.Fatal("expected error for unsupported dynamic constraint key, got nil")
	}
	if got, want := err.Error(), "GetSdmxObservationsQuery: unsupported constraint key \"customEntity\""; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}

	// Case 5: observationAbout is allowed only when resolved as an observationProperty.
	_, err = builder.GetSdmxObservationsQuery(constraints, map[string]map[string]string{
		"var1": {"destinationCountry": "entity1", "sourceCountry": "entity2"},
	})
	if err == nil {
		t.Fatal("expected error for observationAbout outside resolved observationProperties, got nil")
	}
	if got, want := err.Error(), "GetSdmxObservationsQuery: unsupported constraint key \"observationAbout\""; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestMultiEntityGetSdmxObservationsQueryDoesNotUseFacetJSONFallback(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range multiEntitySdmxObservationsTestCases {
		stmt, err := builder.GetSdmxObservationsQuery(c.constraints, c.entitySlotsByStatVar)
		if err != nil {
			t.Fatalf("GetSdmxObservationsQuery(%q) error = %v", c.name, err)
		}
		if strings.Contains(stmt.SQL, "JSON_VALUE(t.facet") {
			t.Fatalf("GetSdmxObservationsQuery(%q) SQL contains facet JSON fallback: %s", c.name, stmt.SQL)
		}
	}
}

func TestMultiEntityGetSdmxObservationsQueryUsesResolvedObservationAboutSlot(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := builder.GetSdmxObservationsQuery(
		map[string]*sdmxpb.ConstraintList{
			"variableMeasured": {Values: []string{"var1"}},
			"observationAbout": {Values: []string{"country/USA"}},
		},
		map[string]map[string]string{
			"var1": {
				"destinationCountry": "entity1",
				"observationAbout":   "entity2",
			},
		},
	)
	if err != nil {
		t.Fatalf("GetSdmxObservationsQuery() error = %v", err)
	}
	if !strings.Contains(stmt.SQL, "t.entity2 IN UNNEST(@observationAbout)") {
		t.Fatalf("SQL = %s, want observationAbout filter on entity2", stmt.SQL)
	}
	if strings.Contains(stmt.SQL, "t.entity1 IN UNNEST(@observationAbout)") {
		t.Fatalf("SQL = %s, want no observationAbout filter on entity1", stmt.SQL)
	}
}

func TestMultiEntityQueryBuildersUseCustomTableConfig(t *testing.T) {
	cfg := spanner.DefaultTableConfig()
	cfg.TimeSeriesTable = "CustomTsTable"
	cfg.ObservationTable = "CustomObsTable"
	cfg.TimeSeriesByEntity2Index = "CustomEntity2Index"
	cfg.TimeSeriesByEntity3Index = "CustomEntity3Index"
	builder, err := spanner.NewMultiEntityQueryBuilder(cfg)
	if err != nil {
		t.Fatal(err)
	}

	obsStmt, err := builder.GetObservationsQuery(
		[]string{"Count_Person"},
		[]string{"geoId/06"},
		"",
	)
	if err != nil {
		t.Fatalf("GetObservationsQuery() returned error: %v", err)
	}
	assertSQLContains(t, obsStmt.SQL, "CustomObsTable", "CustomTsTable")

	containedInStmt, err := builder.GetObservationsContainedInPlaceQuery(
		[]string{"Count_Person"},
		&v2.ContainedInPlace{Ancestor: "geoId/06", ChildPlaceType: "County"},
		"",
	)
	if err != nil {
		t.Fatalf("GetObservationsContainedInPlaceQuery() returned error: %v", err)
	}
	assertSQLContains(t, containedInStmt.SQL,
		"CustomObsTable",
		"CustomTsTable@{FORCE_INDEX=_BASE_TABLE}",
	)

	existenceStmt, err := builder.GetStatVarsByEntityQuery(
		[]string{"Count_Person"},
		[]string{"geoId/06"},
	)
	if err != nil {
		t.Fatalf("GetStatVarsByEntityQuery() returned error: %v", err)
	}
	assertSQLContains(t, existenceStmt.SQL, "CustomTsTable", "CustomEntity2Index", "CustomEntity3Index")

	filteredTopicStmt, err := builder.GetFilteredTopicChildrenQuery(
		[]string{"dc/topic/Test"},
		nil,
		"",
		2,
	)
	if err != nil {
		t.Fatalf("GetFilteredTopicChildrenQuery() returned error: %v", err)
	}
	assertSQLContains(t, filteredTopicStmt.SQL, "CustomTsTable", "CustomEntity2Index", "CustomEntity3Index")

	availabilityStmt, err := builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "observationAbout",
		Constraints: map[string]*sdmxpb.ConstraintList{
			"variableMeasured": {Values: []string{"Count_Person"}},
		},
	}, map[string]map[string]string{
		"Count_Person": {"observationAbout": "entity1"},
	})
	if err != nil {
		t.Fatalf("GetSdmxAvailabilityQuery() returned error: %v", err)
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

func TestMultiEntityGetSdmxAvailabilityQuery(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name                 string
		componentID          string
		entitySlotsByStatVar map[string]map[string]string
		golden               string
	}{
		{
			name:        "observation about",
			componentID: "observationAbout",
			entitySlotsByStatVar: map[string]map[string]string{
				"Count_Household": {"observationAbout": "entity1"},
				"Count_Person":    {"observationAbout": "entity1"},
			},
			golden: "get_sdmx_availability_observation_about.sql",
		},
		{
			name:        "dynamic observation property",
			componentID: "destinationCountry",
			entitySlotsByStatVar: map[string]map[string]string{
				"Count_Household": {"destinationCountry": "entity1", "sourceCountry": "entity2"},
				"Count_Person":    {"destinationCountry": "entity1", "sourceCountry": "entity2"},
			},
			golden: "get_sdmx_availability_destination_country.sql",
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
					ComponentId: c.componentID,
					Constraints: map[string]*sdmxpb.ConstraintList{
						"variableMeasured": {Values: []string{"Count_Person", "Count_Household"}},
					},
				}, c.entitySlotsByStatVar)
			})
		})
	}
}

func TestMultiEntityGetSdmxAvailabilityQueryWithDimensionFilters(t *testing.T) {
	runQueryBuilderGoldenTest(t, "get_sdmx_availability_filtered_destination_country.sql", func(ctx context.Context) (interface{}, error) {
		builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
		if err != nil {
			return nil, err
		}
		return builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
			ComponentId: "destinationCountry",
			Constraints: map[string]*sdmxpb.ConstraintList{
				"variableMeasured":   {Values: []string{"var2", "var1"}},
				"destinationCountry": {Values: []string{"country/PRT", "country/SGP"}},
				"sourceCountry":      {Values: []string{"country/AGO", "country/BRA"}},
				"measurementMethod":  {Values: []string{"Census", "Survey"}},
				"observationPeriod":  {Values: []string{"P1Y", "P1M"}},
				"provenance":         {Values: []string{"dc/base/one", "dc/base/two"}},
				"unit":               {Values: []string{"Percent", "Count"}},
			},
		}, map[string]map[string]string{
			"var1": {"destinationCountry": "entity1", "sourceCountry": "entity2"},
			"var2": {"destinationCountry": "entity1", "sourceCountry": "entity2"},
		})
	})
}

func TestMultiEntityGetSdmxAvailabilityQuery_Validation(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name                 string
		req                  *sdmxpb.SdmxAvailabilityQuery
		entitySlotsByStatVar map[string]map[string]string
	}{
		{
			name: "nil request",
		},
		{
			name: "nil constraints",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
			},
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
			name: "blank variable measured value",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.ConstraintList{
					"variableMeasured": {Values: []string{" "}},
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
					"customEntity":     {Values: []string{"country/USA"}},
				},
			},
		},
		{
			name: "missing target mapping",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "destinationCountry",
				Constraints: map[string]*sdmxpb.ConstraintList{
					"variableMeasured": {Values: []string{"var1"}},
				},
			},
		},
		{
			name: "inconsistent target mapping",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "destinationCountry",
				Constraints: map[string]*sdmxpb.ConstraintList{
					"variableMeasured": {Values: []string{"var1", "var2"}},
				},
			},
			entitySlotsByStatVar: map[string]map[string]string{
				"var1": {"destinationCountry": "entity1"},
				"var2": {"destinationCountry": "entity2"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := builder.GetSdmxAvailabilityQuery(tc.req, tc.entitySlotsByStatVar)
			if err == nil {
				t.Fatal("GetSdmxAvailabilityQuery() error = nil, want error")
			}
		})
	}
}
