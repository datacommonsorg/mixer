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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMultiEntityGetObservationsQuery(t *testing.T) {
	t.Parallel()

	for _, c := range multiEntityObservationsTestCases {
		c := c // Capture loop variable
		t.Run(c.name, func(t *testing.T) {
			goldenFile := c.golden + ".sql"
			runQueryBuilderGoldenTest(t, goldenFile, func(ctx context.Context) (interface{}, error) {
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{
					ContainedInPlaceAncestorFirstTypes:     c.ancestorFirstTypes,
					ContainedInPlaceEntityScanMinVariables: c.entityScanMinVars,
				})
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
				if err != nil {
					return nil, err
				}
				stmt, err := builder.GetSdmxObservationsQuery(
					c.constraints,
					c.observationPropertyToEntitySlot,
					c.containedInPlaceToRemoteDCIDs,
				)
				return stmt, err
			})
		})
	}
}

func TestMultiEntityGetSdmxObservationsQuery_Validation(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		"variableMeasured":  sdmxComponentConstraint("var1"),
		"observationAbout":  sdmxComponentConstraint("wikidataId/Q119158"),
		"provenance":        sdmxComponentConstraint("dc/base/INPE_Fire_Event_Count"),
		"observationPeriod": sdmxComponentConstraint("P1Y"),
	}
	observationPropertyToEntitySlot := map[string]string{
		"observationAbout": "entity1",
	}
	_, err = builder.GetSdmxObservationsQuery(constraints, observationPropertyToEntitySlot, nil)
	if err != nil {
		t.Errorf("expected no error for valid constraint keys, got %v", err)
	}

	for _, tc := range []struct {
		name                            string
		constraints                     map[string]*sdmxpb.SdmxComponentConstraint
		observationPropertyToEntitySlot map[string]string
		want                            string
	}{
		{
			name: "nil constraints",
			want: "GetSdmxObservationsQuery: missing required SDMX component filter variableMeasured",
		},
		{
			name: "invalid key containing SQL injection payload",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"unit') OR 1=1 --": sdmxComponentConstraint("Percent"),
			},
			want: `GetSdmxObservationsQuery: invalid SDMX component filter "unit') OR 1=1 --"`,
		},
		{
			name: "invalid key containing spaces",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"invalid key":      sdmxComponentConstraint("value"),
			},
			want: `GetSdmxObservationsQuery: invalid SDMX component filter "invalid key"`,
		},
		{
			name: "nil constraint list",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"unit":             nil,
			},
			want: `GetSdmxObservationsQuery: SDMX component filter "unit" must have at least one value`,
		},
		{
			name: "empty constraint list",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"unit":             {},
			},
			want: `GetSdmxObservationsQuery: SDMX component filter "unit" must have at least one value`,
		},
		{
			name: "blank constraint value",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"unit":             sdmxComponentConstraint(" "),
			},
			want: `GetSdmxObservationsQuery: SDMX component filter "unit" contains an empty value`,
		},
		{
			name: "missing variable measured",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"unit": sdmxComponentConstraint("Percent"),
			},
			want: "GetSdmxObservationsQuery: missing required SDMX component filter variableMeasured",
		},
		{
			name: "latest mixed with explicit date",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"TIME_PERIOD":      sdmxComponentConstraint("LATEST", "2020"),
			},
			want: "GetSdmxObservationsQuery: SDMX TIME_PERIOD filter cannot combine LATEST with explicit dates",
		},
		{
			name: "unsupported dynamic key",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"customEntity":     sdmxComponentConstraint("value"),
			},
			observationPropertyToEntitySlot: observationPropertyToEntitySlot,
			want:                            `GetSdmxObservationsQuery: unsupported SDMX component filter "customEntity"`,
		},
		{
			name: "unsupported entity slot mapping",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured":   sdmxComponentConstraint("var1"),
				"destinationCountry": sdmxComponentConstraint("country/USA"),
			},
			observationPropertyToEntitySlot: map[string]string{
				"destinationCountry": "entity4",
			},
			want: `GetSdmxObservationsQuery: SDMX observation property "destinationCountry" maps to unsupported entity slot "entity4"`,
		},
		{
			name:        "observation about outside resolved properties",
			constraints: constraints,
			observationPropertyToEntitySlot: map[string]string{
				"destinationCountry": "entity1", "sourceCountry": "entity2",
			},
			want: `GetSdmxObservationsQuery: unsupported SDMX component filter "observationAbout"`,
		},
		{
			name: "duplicate entity slot mapping",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured":   sdmxComponentConstraint("var1"),
				"destinationCountry": sdmxComponentConstraint("country/CAN"),
				"sourceCountry":      sdmxComponentConstraint("country/USA"),
			},
			observationPropertyToEntitySlot: map[string]string{
				"destinationCountry": "entity1",
				"sourceCountry":      "entity1",
			},
			want: `GetSdmxObservationsQuery: SDMX observation properties "destinationCountry" and "sourceCountry" map to the same entity slot "entity1"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := builder.GetSdmxObservationsQuery(tc.constraints, tc.observationPropertyToEntitySlot, nil)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("GetSdmxObservationsQuery() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tc.want {
				t.Fatalf("GetSdmxObservationsQuery() message = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMultiEntityGetSdmxObservationsQueryTimePlans(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name       string
		timeValues []string
		contains   []string
		excludes   []string
	}{
		{
			name:       "explicit dates use observation join",
			timeValues: []string{"2020", "2022"},
			contains: []string{
				"JOIN@{JOIN_METHOD=APPLY_JOIN} Observation o",
				"WHERE o.date IN UNNEST(@time_periods)",
				"ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value) ORDER BY o.date)",
			},
			excludes: []string{"LIMIT 1"},
		},
		{
			name:       "latest uses full-key correlated lookup",
			timeValues: []string{"LATEST"},
			contains: []string{
				"AND o.extra_entities_id = t.extra_entities_id",
				"AND o.facet_id = t.facet_id",
				"ORDER BY o.date DESC",
				"LIMIT 1",
			},
			excludes: []string{"@time_periods"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := builder.GetSdmxObservationsQuery(
				map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint("var1"),
					"TIME_PERIOD":      sdmxComponentConstraint(tc.timeValues...),
				},
				nil,
				nil,
			)
			if err != nil {
				t.Fatalf("GetSdmxObservationsQuery() error = %v", err)
			}
			for _, substring := range tc.contains {
				if !strings.Contains(statement.SQL, substring) {
					t.Errorf("GetSdmxObservationsQuery() SQL missing %q:\n%s", substring, statement.SQL)
				}
			}
			for _, substring := range tc.excludes {
				if strings.Contains(statement.SQL, substring) {
					t.Errorf("GetSdmxObservationsQuery() SQL unexpectedly contains %q:\n%s", substring, statement.SQL)
				}
			}
		})
	}
}

func TestMultiEntityGetSdmxObservationsQueryDoesNotUseFacetJSONFallback(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range multiEntitySdmxObservationsTestCases {
		stmt, err := builder.GetSdmxObservationsQuery(c.constraints, c.observationPropertyToEntitySlot, nil)
		if err != nil {
			t.Fatalf("GetSdmxObservationsQuery(%q) error = %v", c.name, err)
		}
		if strings.Contains(stmt.SQL, "JSON_VALUE(t.facet") {
			t.Fatalf("GetSdmxObservationsQuery(%q) SQL contains facet JSON fallback: %s", c.name, stmt.SQL)
		}
	}
}

func TestMultiEntityGetSdmxObservationsQueryUsesResolvedObservationAboutSlot(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := builder.GetSdmxObservationsQuery(
		map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxComponentConstraint("country/USA"),
		},
		map[string]string{
			"destinationCountry": "entity1",
			"observationAbout":   "entity2",
		},
		nil,
	)
	if err != nil {
		t.Fatalf("GetSdmxObservationsQuery() error = %v", err)
	}
	if !strings.Contains(stmt.SQL, "t.entity2 IN UNNEST(@filter_entity2)") {
		t.Fatalf("SQL = %s, want observationAbout filter on entity2", stmt.SQL)
	}
	if strings.Contains(stmt.SQL, "t.entity1 IN UNNEST(@filter_entity1)") {
		t.Fatalf("SQL = %s, want no observationAbout filter on entity1", stmt.SQL)
	}
}

func TestMultiEntityQueryBuildersUseCustomTableConfig(t *testing.T) {
	cfg := spanner.DefaultTableConfig()
	cfg.TimeSeriesTable = "CustomTsTable"
	cfg.ObservationTable = "CustomObsTable"
	cfg.TimeSeriesByEntity1Index = "CustomEntity1Index"
	cfg.TimeSeriesByEntity2Index = "CustomEntity2Index"
	cfg.TimeSeriesByEntity3Index = "CustomEntity3Index"
	builder, err := spanner.NewMultiEntityQueryBuilder(cfg, spanner.MultiEntityQueryConfig{
		ContainedInPlaceEntityScanMinVariables: 1,
	})
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

	sdmxStmt, err := builder.GetSdmxObservationsQuery(
		map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
			"source":           sdmxContainedInPlaceConstraint("country/USA", "State"),
		},
		map[string]string{"source": "entity2"},
		nil,
	)
	if err != nil {
		t.Fatalf("GetSdmxObservationsQuery() returned error: %v", err)
	}
	assertSQLContains(t, sdmxStmt.SQL, "CustomObsTable", "CustomTsTable", "CustomEntity2Index")

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
		"CustomTsTable@{FORCE_INDEX=CustomEntity1Index, SEEKABLE_KEY_SIZE=1}",
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
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
		},
	}, map[string]string{
		"observationAbout": "entity1",
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
		name                            string
		componentID                     string
		observationPropertyToEntitySlot map[string]string
		golden                          string
	}{
		{
			name:        "observation about",
			componentID: "observationAbout",
			observationPropertyToEntitySlot: map[string]string{
				"observationAbout": "entity1",
			},
			golden: "get_sdmx_availability_observation_about.sql",
		},
		{
			name:        "dynamic observation property",
			componentID: "destinationCountry",
			observationPropertyToEntitySlot: map[string]string{
				"destinationCountry": "entity1", "sourceCountry": "entity2",
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
				if err != nil {
					return nil, err
				}
				return builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
					ComponentId: c.componentID,
					Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
						"variableMeasured": sdmxComponentConstraint("Count_Person", "Count_Household"),
					},
				}, c.observationPropertyToEntitySlot)
			})
		})
	}
}

func TestMultiEntityGetSdmxAvailabilityQueryWithDimensionFilters(t *testing.T) {
	runQueryBuilderGoldenTest(t, "get_sdmx_availability_filtered_destination_country.sql", func(ctx context.Context) (interface{}, error) {
		builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
		if err != nil {
			return nil, err
		}
		return builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
			ComponentId: "destinationCountry",
			Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured":   sdmxComponentConstraint("var2", "var1"),
				"destinationCountry": sdmxComponentConstraint("country/PRT", "country/SGP"),
				"sourceCountry":      sdmxComponentConstraint("country/AGO", "country/BRA"),
				"measurementMethod":  sdmxComponentConstraint("Census", "Survey"),
				"observationPeriod":  sdmxComponentConstraint("P1Y", "P1M"),
				"provenance":         sdmxComponentConstraint("dc/base/one", "dc/base/two"),
				"unit":               sdmxComponentConstraint("Percent", "Count"),
			},
		}, map[string]string{
			"destinationCountry": "entity1", "sourceCountry": "entity2",
		})
	})
}

func TestMultiEntityGetSdmxAvailabilityQueryWithTimePeriods(t *testing.T) {
	runQueryBuilderGoldenTest(t, "get_sdmx_availability_measurement_method_with_time_periods.sql", func(ctx context.Context) (interface{}, error) {
		builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
		if err != nil {
			return nil, err
		}
		return builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
			ComponentId: "measurementMethod",
			Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_TimeSeries"),
				"TIME_PERIOD":      sdmxComponentConstraint("2023", "2020", "2020"),
			},
		}, nil)
	})
}

func TestMultiEntityGetSdmxAvailabilityQueryWithTimePeriodsAndSeriesFilter(t *testing.T) {
	runQueryBuilderGoldenTest(t, "get_sdmx_availability_measurement_method_with_time_periods_and_unit.sql", func(ctx context.Context) (interface{}, error) {
		builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
		if err != nil {
			return nil, err
		}
		return builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
			ComponentId: "measurementMethod",
			Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("Count_TimeSeries"),
				"TIME_PERIOD":      sdmxComponentConstraint("2023", "2020"),
				"unit":             sdmxComponentConstraint("Count"),
			},
		}, nil)
	})
}

func TestMultiEntityGetSdmxAvailabilityQueryTimePlan(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	statement, err := builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "measurementMethod",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_TimeSeries"),
			"TIME_PERIOD":      sdmxComponentConstraint("2020", "2023"),
		},
	}, nil)
	if err != nil {
		t.Fatalf("GetSdmxAvailabilityQuery() error = %v", err)
	}
	for _, substring := range []string{
		"SELECT DISTINCT t.measurement_method AS value",
		"FROM TimeSeries@{FORCE_INDEX=_BASE_TABLE} t",
		"JOIN@{JOIN_METHOD=MERGE_JOIN} Observation@{FORCE_INDEX=_BASE_TABLE} o",
		"USING (variable_measured, entity1, extra_entities_id, facet_id)",
		"o.date IN UNNEST(@time_periods)",
	} {
		if !strings.Contains(statement.SQL, substring) {
			t.Errorf("GetSdmxAvailabilityQuery() SQL missing %q:\n%s", substring, statement.SQL)
		}
	}
	for _, substring := range []string{"APPLY_JOIN", "GROUP BY", "LIMIT 1", "TIME_PERIOD"} {
		if strings.Contains(statement.SQL, substring) {
			t.Errorf("GetSdmxAvailabilityQuery() SQL unexpectedly contains %q:\n%s", substring, statement.SQL)
		}
	}
}

func TestMultiEntityGetSdmxAvailabilityQueryFilteredTimePlan(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	statement, err := builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "measurementMethod",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_TimeSeries"),
			"TIME_PERIOD":      sdmxComponentConstraint("2020", "2023"),
			"unit":             sdmxComponentConstraint("Count"),
		},
	}, nil)
	if err != nil {
		t.Fatalf("GetSdmxAvailabilityQuery() error = %v", err)
	}
	for _, substring := range []string{
		"FROM TimeSeries t",
		"JOIN Observation o",
		"o.date IN UNNEST(@time_periods)",
	} {
		if !strings.Contains(statement.SQL, substring) {
			t.Errorf("GetSdmxAvailabilityQuery() SQL missing %q:\n%s", substring, statement.SQL)
		}
	}
	for _, substring := range []string{"JOIN_METHOD", "FORCE_INDEX"} {
		if strings.Contains(statement.SQL, substring) {
			t.Errorf("GetSdmxAvailabilityQuery() SQL unexpectedly contains %q:\n%s", substring, statement.SQL)
		}
	}
}

func TestMultiEntityGetSdmxAvailabilityQuery_Validation(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig(), spanner.MultiEntityQueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name                            string
		req                             *sdmxpb.SdmxAvailabilityQuery
		observationPropertyToEntitySlot map[string]string
		want                            string
	}{
		{
			name: "nil request",
			want: "SDMX availability request cannot be nil",
		},
		{
			name: "nil constraints",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
			},
			want: "GetSdmxAvailabilityQuery: missing required SDMX component filter variableMeasured",
		},
		{
			name: "missing variable measured",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{},
			},
			want: "GetSdmxAvailabilityQuery: missing required SDMX component filter variableMeasured",
		},
		{
			name: "nil variable measured constraint",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": nil,
				},
			},
			want: "GetSdmxAvailabilityQuery: missing required SDMX component filter variableMeasured",
		},
		{
			name: "empty variable measured values",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": &sdmxpb.SdmxComponentConstraint{},
				},
			},
			want: "GetSdmxAvailabilityQuery: missing required SDMX component filter variableMeasured",
		},
		{
			name: "blank variable measured value",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint(" "),
				},
			},
			want: `GetSdmxAvailabilityQuery: SDMX component filter "variableMeasured" contains an empty value`,
		},
		{
			name: "unsupported component",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "TIME_PERIOD",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint("Count_Person"),
				},
			},
			want: `unsupported SDMX availability component "TIME_PERIOD"`,
		},
		{
			name: "latest time period",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint("Count_Person"),
					"TIME_PERIOD":      sdmxComponentConstraint("latest"),
				},
			},
			want: "GetSdmxAvailabilityQuery: SDMX TIME_PERIOD filter LATEST is not valid for availability; use explicit dates",
		},
		{
			name: "unsupported constraint",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "observationAbout",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint("Count_Person"),
					"customEntity":     sdmxComponentConstraint("country/USA"),
				},
			},
			want: `GetSdmxAvailabilityQuery: unsupported SDMX component filter "customEntity"`,
		},
		{
			name: "missing target mapping",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "destinationCountry",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					"variableMeasured": sdmxComponentConstraint("var1"),
				},
			},
			want: `unsupported SDMX availability component "destinationCountry"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := builder.GetSdmxAvailabilityQuery(tc.req, tc.observationPropertyToEntitySlot)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("GetSdmxAvailabilityQuery() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tc.want {
				t.Fatalf("GetSdmxAvailabilityQuery() message = %q, want %q", got, tc.want)
			}
		})
	}
}
