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
				stmt, err := builder.GetSdmxObservationsQuery(c.constraints, c.entitySlotByObservationProperty)
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

	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		"variableMeasured":  sdmxComponentConstraint("var1"),
		"observationAbout":  sdmxComponentConstraint("wikidataId/Q119158"),
		"provenance":        sdmxComponentConstraint("dc/base/INPE_Fire_Event_Count"),
		"observationPeriod": sdmxComponentConstraint("P1Y"),
	}
	entitySlotByObservationProperty := map[string]string{
		"observationAbout": "entity1",
	}
	_, err = builder.GetSdmxObservationsQuery(constraints, entitySlotByObservationProperty)
	if err != nil {
		t.Errorf("expected no error for valid constraint keys, got %v", err)
	}

	for _, tc := range []struct {
		name                            string
		constraints                     map[string]*sdmxpb.SdmxComponentConstraint
		entitySlotByObservationProperty map[string]string
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
			name: "unsupported dynamic key",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured": sdmxComponentConstraint("var1"),
				"customEntity":     sdmxComponentConstraint("value"),
			},
			entitySlotByObservationProperty: entitySlotByObservationProperty,
			want:                            `GetSdmxObservationsQuery: unsupported SDMX component filter "customEntity"`,
		},
		{
			name: "unsupported entity slot mapping",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"variableMeasured":   sdmxComponentConstraint("var1"),
				"destinationCountry": sdmxComponentConstraint("country/USA"),
			},
			entitySlotByObservationProperty: map[string]string{
				"destinationCountry": "entity4",
			},
			want: `GetSdmxObservationsQuery: SDMX observation property "destinationCountry" maps to unsupported entity slot "entity4"`,
		},
		{
			name:        "observation about outside resolved properties",
			constraints: constraints,
			entitySlotByObservationProperty: map[string]string{
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
			entitySlotByObservationProperty: map[string]string{
				"destinationCountry": "entity1",
				"sourceCountry":      "entity1",
			},
			want: `GetSdmxObservationsQuery: SDMX observation properties "destinationCountry" and "sourceCountry" map to the same entity slot "entity1"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := builder.GetSdmxObservationsQuery(tc.constraints, tc.entitySlotByObservationProperty)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("GetSdmxObservationsQuery() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tc.want {
				t.Fatalf("GetSdmxObservationsQuery() message = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMultiEntityGetSdmxObservationsQueryDoesNotUseFacetJSONFallback(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range multiEntitySdmxObservationsTestCases {
		stmt, err := builder.GetSdmxObservationsQuery(c.constraints, c.entitySlotByObservationProperty)
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
		map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxComponentConstraint("country/USA"),
		},
		map[string]string{
			"destinationCountry": "entity1",
			"observationAbout":   "entity2",
		},
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

	sdmxStmt, err := builder.GetSdmxObservationsQuery(
		map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("Count_Person"),
			"source":           sdmxContainedInPlaceConstraint("country/USA", "State"),
		},
		map[string]string{"source": "entity2"},
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
		entitySlotByObservationProperty map[string]string
		golden                          string
	}{
		{
			name:        "observation about",
			componentID: "observationAbout",
			entitySlotByObservationProperty: map[string]string{
				"observationAbout": "entity1",
			},
			golden: "get_sdmx_availability_observation_about.sql",
		},
		{
			name:        "dynamic observation property",
			componentID: "destinationCountry",
			entitySlotByObservationProperty: map[string]string{
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
				builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
				if err != nil {
					return nil, err
				}
				return builder.GetSdmxAvailabilityQuery(&sdmxpb.SdmxAvailabilityQuery{
					ComponentId: c.componentID,
					Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
						"variableMeasured": sdmxComponentConstraint("Count_Person", "Count_Household"),
					},
				}, c.entitySlotByObservationProperty)
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

func TestMultiEntityGetSdmxAvailabilityQuery_Validation(t *testing.T) {
	builder, err := spanner.NewMultiEntityQueryBuilder(spanner.DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name                            string
		req                             *sdmxpb.SdmxAvailabilityQuery
		entitySlotByObservationProperty map[string]string
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
			_, err := builder.GetSdmxAvailabilityQuery(tc.req, tc.entitySlotByObservationProperty)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("GetSdmxAvailabilityQuery() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tc.want {
				t.Fatalf("GetSdmxAvailabilityQuery() message = %q, want %q", got, tc.want)
			}
		})
	}
}
