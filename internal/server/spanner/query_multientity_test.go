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

package spanner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	cloudspanner "cloud.google.com/go/spanner"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func sdmxComponentConstraint(values ...string) *sdmxpb.SdmxComponentConstraint {
	predicates := make([]*sdmxpb.SdmxPredicate, 0, len(values))
	for _, value := range values {
		predicates = append(predicates, &sdmxpb.SdmxPredicate{Value: value})
	}
	return &sdmxpb.SdmxComponentConstraint{Predicates: predicates}
}

func sdmxContainedInPlaceConstraint(ancestor, childPlaceType string) *sdmxpb.SdmxComponentConstraint {
	return &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			datacommons.PropertyContainedInPlace: {
				Predicates: []*sdmxpb.SdmxPredicate{{Value: ancestor}},
				Transitive: true,
			},
			datacommons.PropertyTypeOf: {Predicates: []*sdmxpb.SdmxPredicate{{Value: childPlaceType}}},
		},
	}
}

func TestReconstructObservationsUsesStoredFacetID(t *testing.T) {
	observations, err := reconstructObservations([]*rawObservation{
		{
			VariableMeasured: "Count_Person",
			ObservationAbout: "geoId/06",
			FacetId:          "stored-facet-id",
			ProvenanceID: cloudspanner.NullString{
				StringVal: "dc/base/test_import",
				Valid:     true,
			},
			DatesAndValues: []*spannerObservation{
				{Date: "2020", Value: "1"},
			},
			Facets: cloudspanner.NullJSON{
				Value: map[string]interface{}{
					"facetId":    "json-facet-id",
					"importName": "test_import",
				},
				Valid: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("reconstructObservations() = %v", err)
	}

	if got, want := len(observations), 1; got != want {
		t.Fatalf("len(observations) = %d, want %d", got, want)
	}
	if got, want := observations[0].FacetId, "stored-facet-id"; got != want {
		t.Fatalf("observations[0].FacetId = %q, want %q", got, want)
	}
	if got, want := observations[0].ImportName, "test_import"; got != want {
		t.Fatalf("observations[0].ImportName = %q, want %q", got, want)
	}
	if got, want := observations[0].ProvenanceID, "dc/base/test_import"; got != want {
		t.Fatalf("observations[0].ProvenanceID = %q, want %q", got, want)
	}
}

func TestReconstructObservationsSortsDates(t *testing.T) {
	observations, err := reconstructObservations([]*rawObservation{
		{
			VariableMeasured: "Count_Person",
			ObservationAbout: "geoId/06",
			FacetId:          "stored-facet-id",
			DatesAndValues: []*spannerObservation{
				{Date: "2021", Value: "3"},
				{Date: "2019", Value: "1"},
				{Date: "2020", Value: "2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("reconstructObservations() = %v", err)
	}

	got := observations[0].Observations
	want := []struct {
		date  string
		value string
	}{
		{date: "2019", value: "1"},
		{date: "2020", value: "2"},
		{date: "2021", value: "3"},
	}
	if len(got) != len(want) {
		t.Fatalf("len(Observations) = %d, want %d", len(got), len(want))
	}
	for i, expected := range want {
		if got[i].Date != expected.date || got[i].Value != expected.value {
			t.Errorf(
				"Observations[%d] = (%q, %q), want (%q, %q)",
				i,
				got[i].Date,
				got[i].Value,
				expected.date,
				expected.value,
			)
		}
	}
}

func TestReconstructObservationsAttributes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		attributes cloudspanner.NullJSON
		want       map[string]string
	}{
		{
			name: "missing attributes",
		},
		{
			name: "json null attributes",
			attributes: cloudspanner.NullJSON{
				Valid: true,
			},
		},
		{
			name: "empty attributes",
			attributes: cloudspanner.NullJSON{
				Value: map[string]interface{}{},
				Valid: true,
			},
		},
		{
			name: "string attributes",
			attributes: cloudspanner.NullJSON{
				Value: map[string]interface{}{
					"OBS_STATUS": "A",
					"UNIT_MULT":  "6",
				},
				Valid: true,
			},
			want: map[string]string{
				"OBS_STATUS": "A",
				"UNIT_MULT":  "6",
			},
		},
		{
			name: "convert scalar attributes and skip nested values",
			attributes: cloudspanner.NullJSON{
				Value: map[string]interface{}{
					"BOOL_VALUE":   true,
					"FLOAT_VALUE":  float64(1.5),
					"INT_VALUE":    int64(6),
					"NIL_VALUE":    nil,
					"ARRAY_VALUE":  []interface{}{"skip"},
					"OBJECT_VALUE": map[string]interface{}{"skip": "me"},
					"STRING_VALUE": "keep",
				},
				Valid: true,
			},
			want: map[string]string{
				"BOOL_VALUE":   "true",
				"FLOAT_VALUE":  "1.5",
				"INT_VALUE":    "6",
				"STRING_VALUE": "keep",
			},
		},
		{
			name: "only skipped attributes",
			attributes: cloudspanner.NullJSON{
				Value: map[string]interface{}{
					"NIL_VALUE":    nil,
					"ARRAY_VALUE":  []interface{}{"skip"},
					"OBJECT_VALUE": map[string]interface{}{"skip": "me"},
				},
				Valid: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observations, err := reconstructObservations([]*rawObservation{
				{
					VariableMeasured: "Count_Person",
					ObservationAbout: "geoId/06",
					FacetId:          "stored-facet-id",
					ProvenanceID: cloudspanner.NullString{
						StringVal: "dc/base/test_import",
						Valid:     true,
					},
					DatesAndValues: []*spannerObservation{
						{Date: "2020", Value: "1", Attributes: tc.attributes},
					},
				},
			})
			if err != nil {
				t.Fatalf("reconstructObservations() = %v", err)
			}
			got := observations[0].Observations[0].Attributes
			if len(tc.want) == 0 {
				if got != nil {
					t.Fatalf("Attributes = %v, want nil", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("Attributes = %v, want %v", got, tc.want)
			}
			for key, wantValue := range tc.want {
				if gotValue := got[key]; gotValue != wantValue {
					t.Fatalf("Attributes[%q] = %q, want %q", key, gotValue, wantValue)
				}
			}
		})
	}
}

func TestReconstructObservationsInvalidAttributes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		attributes cloudspanner.NullJSON
		want       string
	}{
		{
			name: "top level non object",
			attributes: cloudspanner.NullJSON{
				Value: "not-an-object",
				Valid: true,
			},
			want: "attributes JSON must be an object",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reconstructObservations([]*rawObservation{
				{
					VariableMeasured: "Count_Person",
					ObservationAbout: "geoId/06",
					FacetId:          "stored-facet-id",
					ProvenanceID: cloudspanner.NullString{
						StringVal: "dc/base/test_import",
						Valid:     true,
					},
					DatesAndValues: []*spannerObservation{
						{Date: "2020", Value: "1", Attributes: tc.attributes},
					},
				},
			})
			if err == nil {
				t.Fatal("reconstructObservations() expected error, got nil")
			}
			if got := err.Error(); !strings.Contains(got, tc.want) {
				t.Fatalf("reconstructObservations() error = %q, want substring %q", got, tc.want)
			}
		})
	}
}

func TestPointStatJSONOmitsMissingAttributes(t *testing.T) {
	jsonBytes, err := protojson.Marshal(&pb.PointStat{Date: "2020"})
	if err != nil {
		t.Fatalf("protojson.Marshal() = %v", err)
	}
	if got := string(jsonBytes); strings.Contains(got, "attributes") {
		t.Fatalf("protojson.Marshal() = %s, want no attributes field", got)
	}

	jsonBytes, err = protojson.Marshal(&pb.PointStat{
		Date: "2020",
		Attributes: map[string]string{
			"OBS_STATUS": "A",
		},
	})
	if err != nil {
		t.Fatalf("protojson.Marshal() = %v", err)
	}
	if got := string(jsonBytes); !strings.Contains(got, `"attributes":{"OBS_STATUS":"A"}`) {
		t.Fatalf("protojson.Marshal() = %s, want attributes object", got)
	}
}

func TestMultiEntityGetObservationsContainedInPlaceInvalidArgsReturnEmpty(t *testing.T) {
	for _, tc := range []struct {
		name             string
		variables        []string
		containedInPlace *v2.ContainedInPlace
	}{
		{
			name:      "nil contained in place",
			variables: []string{"Count_Person"},
		},
		{
			name: "empty variables",
			containedInPlace: &v2.ContainedInPlace{
				Ancestor:       "geoId/06",
				ChildPlaceType: "County",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &multiEntityClient{}

			observations, err := client.GetObservationsContainedInPlace(
				context.Background(),
				tc.variables,
				tc.containedInPlace,
				"",
			)
			if err != nil {
				t.Fatalf("GetObservationsContainedInPlace() returned error: %v", err)
			}
			if got := len(observations); got != 0 {
				t.Fatalf("len(observations) = %d, want 0", got)
			}
		})
	}
}

func TestValidateObservationsRequiresProvenance(t *testing.T) {
	for _, tc := range []struct {
		name       string
		provenance cloudspanner.NullString
	}{
		{
			name: "null provenance",
		},
		{
			name: "empty provenance",
			provenance: cloudspanner.NullString{
				Valid: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observations, err := reconstructObservations([]*rawObservation{
				{
					VariableMeasured: "Count_Person",
					ObservationAbout: "geoId/06",
					FacetId:          "stored-facet-id",
					ProvenanceID:     tc.provenance,
					DatesAndValues: []*spannerObservation{
						{Date: "2020", Value: "1"},
					},
					Facets: cloudspanner.NullJSON{
						Value: map[string]interface{}{
							"importName": "test_import",
						},
						Valid: true,
					},
				},
			})
			if err != nil {
				t.Fatalf("reconstructObservations() = %v", err)
			}
			err = validateObservations(observations)
			if err == nil {
				t.Fatal("validateObservations() expected error, got nil")
			}
			if got, want := err.Error(), `observation missing provenance: variable="Count_Person" entity="geoId/06" facet_id="stored-facet-id"`; got != want {
				t.Fatalf("validateObservations() error = %q, want %q", got, want)
			}
		})
	}
}

func TestMultiEntityObservationResponseIncludesProvenanceID(t *testing.T) {
	observations, err := reconstructObservations([]*rawObservation{
		{
			VariableMeasured: "Count_Person",
			ObservationAbout: "geoId/06",
			FacetId:          "stored-facet-id",
			ProvenanceID: cloudspanner.NullString{
				StringVal: "dc/base/test_import",
				Valid:     true,
			},
			DatesAndValues: []*spannerObservation{
				{Date: "2020", Value: "1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("reconstructObservations() = %v", err)
	}

	if err := validateObservations(observations); err != nil {
		t.Fatalf("validateObservations() = %v", err)
	}

	resp := generateObsResponse(
		&pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		observations,
		true,  /* includeObs */
		true,  /* includeObsMetadata */
		false, /* shouldFilterInferiorFacets */
	)
	facet := resp.Facets["stored-facet-id"]
	if facet == nil {
		t.Fatal(`resp.Facets["stored-facet-id"] = nil`)
		return
	}
	if got, want := facet.ProvenanceId, "dc/base/test_import"; got != want {
		t.Fatalf("facet.ProvenanceId = %q, want %q", got, want)
	}
}

func TestMultiEntityGetSdmxObservationsNilRequestReturnsError(t *testing.T) {
	client := &multiEntityClient{}
	_, err := client.GetSdmxObservations(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetSdmxObservations() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX data request cannot be nil"; got != want {
		t.Fatalf("GetSdmxObservations() message = %q, want %q", got, want)
	}
}

func TestMultiEntityGetSdmxObservationsNilConstraintsReturnsError(t *testing.T) {
	client := &multiEntityClient{}
	_, err := client.GetSdmxObservations(context.Background(), &sdmxpb.SdmxDataQuery{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetSdmxObservations() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX data request constraints cannot be nil"; got != want {
		t.Fatalf("GetSdmxObservations() message = %q, want %q", got, want)
	}
}

func TestMultiEntityGetSdmxObservationsRejectsInvalidVariableMeasured(t *testing.T) {
	tests := []struct {
		name       string
		constraint *sdmxpb.SdmxComponentConstraint
		want       string
	}{
		{
			name: "nil constraint list",
			want: "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "empty value list",
			constraint: &sdmxpb.SdmxComponentConstraint{},
			want:       "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "blank value",
			constraint: sdmxComponentConstraint(" "),
			want:       "SDMX component filter \"variableMeasured\" contains an empty value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &multiEntityClient{}
			_, err := client.GetSdmxObservations(context.Background(), &sdmxpb.SdmxDataQuery{
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: tt.constraint,
				},
			})
			if err == nil {
				t.Fatal("GetSdmxObservations() error = nil, want error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("GetSdmxObservations() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tt.want {
				t.Fatalf("GetSdmxObservations() message = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPrepareSdmxObservationsQuery(t *testing.T) {
	queryBuilder, err := NewMultiEntityQueryBuilder(DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		datacommons.ComponentVariableMeasured:  sdmxComponentConstraint("var2", "var1", "var2"),
		"destinationCountry":                   sdmxComponentConstraint("country/CAN", "country/MEX"),
		"sourceCountry":                        sdmxComponentConstraint("country/USA", "country/IND"),
		datacommons.ComponentUnit:              sdmxComponentConstraint("Count", "Percent"),
		datacommons.ComponentMeasurementMethod: sdmxComponentConstraint("Census", "Survey"),
		datacommons.ComponentObservationPeriod: sdmxComponentConstraint("P1Y", "P1M"),
		datacommons.ComponentProvenance:        sdmxComponentConstraint("dc/base/one", "dc/base/two"),
		datacommons.ComponentFacetID:           sdmxComponentConstraint("facet", "alternate-facet"),
	}

	prepared, err := prepareSdmxObservationsQuery(
		context.Background(),
		constraints,
		func(_ context.Context, ids []string, arc *v2.Arc, pageSize int, offset int) (map[string][]*Edge, error) {
			if diff := cmp.Diff([]string{"var1", "var2"}, ids); diff != "" {
				t.Fatalf("GetNodeEdgesByID() ids mismatch (-want +got):\n%s", diff)
			}
			if arc == nil || !arc.Out || arc.SingleProp != "observationProperties" {
				t.Fatalf("GetNodeEdgesByID() arc = %+v, want outgoing observationProperties", arc)
			}
			if pageSize != minObservationPropertiesPageSize {
				t.Fatalf("GetNodeEdgesByID() pageSize = %d, want %d", pageSize, minObservationPropertiesPageSize)
			}
			if offset != 0 {
				t.Fatalf("GetNodeEdgesByID() offset = %d, want 0", offset)
			}
			return map[string][]*Edge{
				"var1": observationPropertiesEdges("sourceCountry", "destinationCountry"),
				"var2": observationPropertiesEdges("destinationCountry", "sourceCountry"),
			}, nil
		},
		queryBuilder,
	)
	if err != nil {
		t.Fatalf("prepareSdmxObservationsQuery() error = %v", err)
	}

	wantShape := sdmxDataShape([]string{"destinationCountry", "sourceCountry"})
	if diff := cmp.Diff(wantShape, prepared.shape, protocmp.Transform()); diff != "" {
		t.Fatalf("prepareSdmxObservationsQuery() shape mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationProperty := map[string]string{
		"destinationCountry": "entity1",
		"sourceCountry":      "entity2",
	}
	if diff := cmp.Diff(wantEntitySlotByObservationProperty, prepared.entitySlotByObservationProperty); diff != "" {
		t.Fatalf("prepareSdmxObservationsQuery() entity slots mismatch (-want +got):\n%s", diff)
	}
	wantParams := map[string]interface{}{
		datacommons.ComponentVariableMeasured: []string{"var1", "var2"},
		"filter_entity1":                      []string{"country/CAN", "country/MEX"},
		"filter_entity2":                      []string{"country/USA", "country/IND"},
		"filter_unit":                         []string{"Count", "Percent"},
		"filter_measurement_method":           []string{"Census", "Survey"},
		"filter_observation_period":           []string{"P1Y", "P1M"},
		"filter_provenance":                   []string{"dc/base/one", "dc/base/two"},
		"filter_facet_id":                     []string{"facet", "alternate-facet"},
	}
	if diff := cmp.Diff(wantParams, prepared.statement.Params); diff != "" {
		t.Fatalf("prepareSdmxObservationsQuery() params mismatch (-want +got):\n%s", diff)
	}
	if !strings.Contains(prepared.statement.SQL, "t.variable_measured IN UNNEST(@variableMeasured)") {
		t.Fatalf("prepareSdmxObservationsQuery() SQL does not parameterize variableMeasured:\n%s", prepared.statement.SQL)
	}
	if strings.Contains(prepared.statement.SQL, `t.variable_measured =`) {
		t.Fatalf("prepareSdmxObservationsQuery() SQL contains a variableMeasured literal branch:\n%s", prepared.statement.SQL)
	}

	interpolatedSQL := InterpolateSQL(prepared.statement)
	for _, fragment := range []string{
		`t.variable_measured IN ('var1','var2') AND t.entity1 IN ('country/CAN','country/MEX')`,
		`t.entity2 IN ('country/USA','country/IND')`,
		`t.facet_id IN ('facet','alternate-facet')`,
		`t.measurement_method IN ('Census','Survey')`,
		`t.observation_period IN ('P1Y','P1M')`,
		`t.provenance IN ('dc/base/one','dc/base/two')`,
		`t.unit IN ('Count','Percent')`,
	} {
		if !strings.Contains(interpolatedSQL, fragment) {
			t.Errorf("prepareSdmxObservationsQuery() SQL does not contain %q:\n%s", fragment, interpolatedSQL)
		}
	}
}

func TestCompileSdmxConstraintsCanonicalizesObservationPropertyNames(t *testing.T) {
	compile := func(observationProperty string) *compiledSdmxConstraints {
		t.Helper()
		compiled, err := compileSdmxConstraints(
			map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("Count_Person"),
				observationProperty:                   sdmxComponentConstraint("country/USA"),
				datacommons.ComponentUnit:             sdmxComponentConstraint("Count"),
			},
			map[string]string{observationProperty: "entity1"},
		)
		if err != nil {
			t.Fatalf("compileSdmxConstraints() error = %v", err)
		}
		return compiled
	}

	beforeUnit := compile("aaaRegion")
	afterUnit := compile("zzzRegion")
	if diff := cmp.Diff(beforeUnit.where, afterUnit.where); diff != "" {
		t.Fatalf("compileSdmxConstraints() SQL depends on observation property name (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(beforeUnit.params, afterUnit.params); diff != "" {
		t.Fatalf("compileSdmxConstraints() params depend on observation property name (-want +got):\n%s", diff)
	}
	if got, want := beforeUnit.where, "t.variable_measured IN UNNEST(@variableMeasured) AND t.entity1 IN UNNEST(@filter_entity1) AND t.unit IN UNNEST(@filter_unit)"; got != want {
		t.Fatalf("compileSdmxConstraints() where = %q, want %q", got, want)
	}
}

func TestGetSdmxObservationsQueryCanonicalizesContainedInPlaceObservationPropertyNames(t *testing.T) {
	queryBuilder, err := NewMultiEntityQueryBuilder(DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}

	build := func(directComponent, containedComponent string) *cloudspanner.Statement {
		t.Helper()
		statement, err := queryBuilder.GetSdmxObservationsQuery(
			map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("Count_Migration"),
				directComponent:                       sdmxComponentConstraint("country/CAN"),
				containedComponent:                    sdmxContainedInPlaceConstraint("northamerica", "Country"),
			},
			map[string]string{
				directComponent:    "entity1",
				containedComponent: "entity2",
			},
		)
		if err != nil {
			t.Fatalf("GetSdmxObservationsQuery() error = %v", err)
		}
		return statement
	}

	first := build("destinationCountry", "sourceCountry")
	second := build("arrivalRegion", "originRegion")
	if diff := cmp.Diff(first.SQL, second.SQL); diff != "" {
		t.Fatalf("GetSdmxObservationsQuery() SQL depends on observation property names (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(first.Params, second.Params); diff != "" {
		t.Fatalf("GetSdmxObservationsQuery() params depend on observation property names (-want +got):\n%s", diff)
	}
}

func TestPrepareSdmxObservationsQueryWithContainedInPlace(t *testing.T) {
	queryBuilder, err := NewMultiEntityQueryBuilder(DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		datacommons.ComponentVariableMeasured: sdmxComponentConstraint("Count_Migration"),
		"containedAncestor0":                  sdmxComponentConstraint("country/CAN"),
		"sourceCountry":                       sdmxContainedInPlaceConstraint("country/USA", "State"),
	}
	prepared, err := prepareSdmxObservationsQuery(
		context.Background(),
		constraints,
		func(_ context.Context, ids []string, arc *v2.Arc, pageSize int, offset int) (map[string][]*Edge, error) {
			return map[string][]*Edge{
				"Count_Migration": observationPropertiesEdges("containedAncestor0", "sourceCountry"),
			}, nil
		},
		queryBuilder,
	)
	if err != nil {
		t.Fatalf("prepareSdmxObservationsQuery() error = %v", err)
	}
	wantParams := map[string]interface{}{
		datacommons.ComponentVariableMeasured: []string{"Count_Migration"},
		"filter_entity1":                      []string{"country/CAN"},
		"containment_0_ancestor":              "country/USA",
		"containment_0_child_place_type":      "State",
	}
	if diff := cmp.Diff(wantParams, prepared.statement.Params); diff != "" {
		t.Fatalf("prepareSdmxObservationsQuery() params mismatch (-want +got):\n%s", diff)
	}
	for _, fragment := range []string{
		"contained.predicate = 'linkedContainedInPlace'",
		"typed.predicate = 'typeOf'",
		"TimeSeries@{FORCE_INDEX=TimeSeriesByEntity2}",
		"ON t.entity2 = anchor.place_id",
		"t.variable_measured IN UNNEST(@variableMeasured)",
		"t.entity1 IN UNNEST(@filter_entity1)",
		"t.entity2 IS NOT NULL",
	} {
		if !strings.Contains(prepared.statement.SQL, fragment) {
			t.Errorf("prepareSdmxObservationsQuery() SQL does not contain %q:\n%s", fragment, prepared.statement.SQL)
		}
	}
}

func TestPrepareSdmxObservationsQueryRejectsPropertyConstraintOutsideObservationProperties(t *testing.T) {
	queryBuilder, err := NewMultiEntityQueryBuilder(DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, err = prepareSdmxObservationsQuery(
		context.Background(),
		map[string]*sdmxpb.SdmxComponentConstraint{
			datacommons.ComponentVariableMeasured: sdmxComponentConstraint("Count_Person"),
			"customEntity":                        sdmxContainedInPlaceConstraint("country/USA", "County"),
		},
		func(_ context.Context, ids []string, arc *v2.Arc, pageSize int, offset int) (map[string][]*Edge, error) {
			return map[string][]*Edge{
				"Count_Person": observationPropertiesEdges(datacommons.ComponentObservationAbout),
			}, nil
		},
		queryBuilder,
	)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("prepareSdmxObservationsQuery() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
}

func TestPrepareSdmxAvailabilityQuery(t *testing.T) {
	queryBuilder, err := NewMultiEntityQueryBuilder(DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}
	req := &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: "destinationCountry",
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			datacommons.ComponentVariableMeasured:  sdmxComponentConstraint("var2", "var1", "var2"),
			"destinationCountry":                   sdmxComponentConstraint("country/CAN", "country/MEX"),
			"sourceCountry":                        sdmxComponentConstraint("country/USA", "country/IND"),
			datacommons.ComponentUnit:              sdmxComponentConstraint("Count", "Percent"),
			datacommons.ComponentMeasurementMethod: sdmxComponentConstraint("Census", "Survey"),
			datacommons.ComponentObservationPeriod: sdmxComponentConstraint("P1Y", "P1M"),
			datacommons.ComponentProvenance:        sdmxComponentConstraint("dc/base/one", "dc/base/two"),
		},
	}

	stmt, err := prepareSdmxAvailabilityQuery(
		context.Background(),
		req,
		func(_ context.Context, ids []string, arc *v2.Arc, pageSize int, offset int) (map[string][]*Edge, error) {
			if diff := cmp.Diff([]string{"var1", "var2"}, ids); diff != "" {
				t.Fatalf("GetNodeEdgesByID() ids mismatch (-want +got):\n%s", diff)
			}
			if arc == nil || !arc.Out || arc.SingleProp != "observationProperties" {
				t.Fatalf("GetNodeEdgesByID() arc = %+v, want outgoing observationProperties", arc)
			}
			if pageSize != minObservationPropertiesPageSize || offset != 0 {
				t.Fatalf("GetNodeEdgesByID() pageSize, offset = %d, %d, want %d, 0", pageSize, offset, minObservationPropertiesPageSize)
			}
			return map[string][]*Edge{
				"var1": observationPropertiesEdges("sourceCountry", "destinationCountry"),
				"var2": observationPropertiesEdges("destinationCountry", "sourceCountry"),
			}, nil
		},
		queryBuilder,
	)
	if err != nil {
		t.Fatalf("prepareSdmxAvailabilityQuery() error = %v", err)
	}

	wantParams := map[string]interface{}{
		datacommons.ComponentVariableMeasured: []string{"var1", "var2"},
		"filter_entity1":                      []string{"country/CAN", "country/MEX"},
		"filter_entity2":                      []string{"country/USA", "country/IND"},
		"filter_unit":                         []string{"Count", "Percent"},
		"filter_measurement_method":           []string{"Census", "Survey"},
		"filter_observation_period":           []string{"P1Y", "P1M"},
		"filter_provenance":                   []string{"dc/base/one", "dc/base/two"},
	}
	if diff := cmp.Diff(wantParams, stmt.Params); diff != "" {
		t.Fatalf("prepareSdmxAvailabilityQuery() params mismatch (-want +got):\n%s", diff)
	}
	if !strings.Contains(stmt.SQL, "t.variable_measured IN UNNEST(@variableMeasured)") {
		t.Fatalf("prepareSdmxAvailabilityQuery() SQL does not parameterize variableMeasured:\n%s", stmt.SQL)
	}
	if strings.Contains(stmt.SQL, `t.variable_measured =`) {
		t.Fatalf("prepareSdmxAvailabilityQuery() SQL contains a variableMeasured literal branch:\n%s", stmt.SQL)
	}
	interpolatedSQL := InterpolateSQL(stmt)
	for _, fragment := range []string{
		"SELECT DISTINCT t.entity1 AS value",
		`t.variable_measured IN ('var1','var2') AND t.entity1 IN ('country/CAN','country/MEX')`,
		`t.entity2 IN ('country/USA','country/IND')`,
		`t.measurement_method IN ('Census','Survey')`,
	} {
		if !strings.Contains(interpolatedSQL, fragment) {
			t.Errorf("prepareSdmxAvailabilityQuery() SQL does not contain %q:\n%s", fragment, interpolatedSQL)
		}
	}
}

func TestPrepareSdmxAvailabilityQueryValidation(t *testing.T) {
	queryBuilder, err := NewMultiEntityQueryBuilder(DefaultTableConfig())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		req        *sdmxpb.SdmxAvailabilityQuery
		edges      map[string][]*Edge
		wantErrSub string
	}{
		{
			name: "target absent from shape",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: datacommons.ComponentObservationAbout,
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				},
			},
			edges:      map[string][]*Edge{"var1": observationPropertiesEdges("destinationCountry", "sourceCountry")},
			wantErrSub: "unsupported SDMX availability component \"observationAbout\"",
		},
		{
			name: "filter absent from shape",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "destinationCountry",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
					"customEntity":                        sdmxComponentConstraint("country/USA"),
				},
			},
			edges:      map[string][]*Edge{"var1": observationPropertiesEdges("destinationCountry", "sourceCountry")},
			wantErrSub: "unsupported SDMX component filter \"customEntity\"",
		},
		{
			name: "facet ID filter unsupported",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: datacommons.ComponentObservationAbout,
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
					datacommons.ComponentFacetID:          sdmxComponentConstraint("facet"),
				},
			},
			edges:      map[string][]*Edge{"var1": nil},
			wantErrSub: "unsupported SDMX component filter \"facetId\"",
		},
		{
			name: "incompatible stat var shapes",
			req: &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: "destinationCountry",
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1", "var2"),
				},
			},
			edges: map[string][]*Edge{
				"var1": observationPropertiesEdges("destinationCountry", "sourceCountry"),
				"var2": nil,
			},
			wantErrSub: "incompatible observationProperties",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := prepareSdmxAvailabilityQuery(
				context.Background(),
				tt.req,
				func(context.Context, []string, *v2.Arc, int, int) (map[string][]*Edge, error) {
					return tt.edges, nil
				},
				queryBuilder,
			)
			if err == nil {
				t.Fatal("prepareSdmxAvailabilityQuery() error = nil, want error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("prepareSdmxAvailabilityQuery() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if !strings.Contains(status.Convert(err).Message(), tt.wantErrSub) {
				t.Fatalf("prepareSdmxAvailabilityQuery() message = %q, want substring %q", status.Convert(err).Message(), tt.wantErrSub)
			}
		})
	}
}

func TestPrepareSdmxShapeMetadataError(t *testing.T) {
	_, err := prepareSdmxShape(
		context.Background(),
		map[string]*sdmxpb.SdmxComponentConstraint{
			datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
		},
		func(context.Context, []string, *v2.Arc, int, int) (map[string][]*Edge, error) {
			return nil, errors.New("metadata unavailable")
		},
	)
	if err == nil {
		t.Fatal("prepareSdmxShape() error = nil, want error")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("prepareSdmxShape() code = %v, want %v; err = %v", got, codes.Internal, err)
	}
	if got, want := status.Convert(err).Message(), "Internal server error occurred while processing the request."; got != want {
		t.Fatalf("prepareSdmxShape() message = %q, want %q", got, want)
	}
}

func TestSdmxBackendError(t *testing.T) {
	for _, backendCode := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied} {
		t.Run(backendCode.String(), func(t *testing.T) {
			err := sdmxBackendError("SDMX backend failed", status.Error(backendCode, "backend details"))
			if got := status.Code(err); got != codes.Internal {
				t.Fatalf("sdmxBackendError() code = %v, want %v", got, codes.Internal)
			}
			if got, want := status.Convert(err).Message(), "Internal server error occurred while processing the request."; got != want {
				t.Fatalf("sdmxBackendError() message = %q, want %q", got, want)
			}
		})
	}

	for _, requestCode := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		t.Run(requestCode.String(), func(t *testing.T) {
			backendErr := status.Error(requestCode, "request stopped")
			if got := sdmxBackendError("SDMX backend failed", backendErr); got != backendErr {
				t.Fatalf("sdmxBackendError() = %v, want original request error %v", got, backendErr)
			}
		})
	}
}

func TestSdmxAvailabilityValueExpressionValidation(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		componentID                     string
		entitySlotByObservationProperty map[string]string
		want                            string
	}{
		{
			name:        "missing component mapping",
			componentID: "destinationCountry",
			want:        `unsupported SDMX availability component "destinationCountry"`,
		},
		{
			name:        "empty component mapping",
			componentID: "destinationCountry",
			entitySlotByObservationProperty: map[string]string{
				"destinationCountry": "",
			},
			want: `unsupported SDMX availability component "destinationCountry"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sdmxAvailabilityValueExpression(tc.componentID, tc.entitySlotByObservationProperty)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("sdmxAvailabilityValueExpression() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tc.want {
				t.Fatalf("sdmxAvailabilityValueExpression() message = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMultiEntityGetSdmxAvailabilityNilRequestReturnsError(t *testing.T) {
	client := &multiEntityClient{}
	_, err := client.GetSdmxAvailability(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetSdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX availability request cannot be nil"; got != want {
		t.Fatalf("GetSdmxAvailability() message = %q, want %q", got, want)
	}
}

func TestMultiEntityGetSdmxAvailabilityNilConstraintsReturnsError(t *testing.T) {
	client := &multiEntityClient{}
	_, err := client.GetSdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetSdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	if got, want := status.Convert(err).Message(), "SDMX availability request constraints cannot be nil"; got != want {
		t.Fatalf("GetSdmxAvailability() message = %q, want %q", got, want)
	}
}

func TestMultiEntitySdmxRejectsUnsupportedOperator(t *testing.T) {
	constraint := &sdmxpb.SdmxComponentConstraint{
		Predicates: []*sdmxpb.SdmxPredicate{{
			Operator: sdmxpb.SdmxOperator(1),
			Value:    "var1",
		}},
	}
	endpoints := []struct {
		name string
		call func(*multiEntityClient, *sdmxpb.SdmxComponentConstraint) error
	}{
		{
			name: "data",
			call: func(client *multiEntityClient, constraint *sdmxpb.SdmxComponentConstraint) error {
				_, err := client.GetSdmxObservations(context.Background(), &sdmxpb.SdmxDataQuery{
					Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
						datacommons.ComponentVariableMeasured: constraint,
					},
				})
				return err
			},
		},
		{
			name: "availability",
			call: func(client *multiEntityClient, constraint *sdmxpb.SdmxComponentConstraint) error {
				_, err := client.GetSdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{
					ComponentId: datacommons.ComponentVariableMeasured,
					Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
						datacommons.ComponentVariableMeasured: constraint,
					},
				})
				return err
			},
		},
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint.name, func(t *testing.T) {
			err := endpoint.call(&multiEntityClient{}, constraint)
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("SDMX backend code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
			}
			if got, want := status.Convert(err).Message(), "SDMX operators other than EQ are not implemented yet"; got != want {
				t.Fatalf("SDMX backend message = %q, want %q", got, want)
			}
		})
	}
}

func TestMultiEntitySdmxAvailabilityRejectsPropertyConstraints(t *testing.T) {
	_, err := (&multiEntityClient{}).GetSdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: datacommons.ComponentObservationAbout,
		Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			datacommons.ComponentVariableMeasured: sdmxComponentConstraint("Count_Person"),
			datacommons.ComponentObservationAbout: {
				PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
					"typeOf": {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}},
				},
			},
		},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("GetSdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
}

func TestMultiEntityGetSdmxAvailabilityRejectsInvalidVariableMeasured(t *testing.T) {
	tests := []struct {
		name       string
		constraint *sdmxpb.SdmxComponentConstraint
		want       string
	}{
		{
			name: "nil constraint list",
			want: "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "empty value list",
			constraint: &sdmxpb.SdmxComponentConstraint{},
			want:       "missing required SDMX component filter variableMeasured",
		},
		{
			name:       "blank value",
			constraint: sdmxComponentConstraint(" "),
			want:       "SDMX component filter \"variableMeasured\" contains an empty value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &multiEntityClient{}
			_, err := client.GetSdmxAvailability(context.Background(), &sdmxpb.SdmxAvailabilityQuery{
				ComponentId: datacommons.ComponentVariableMeasured,
				Constraints: map[string]*sdmxpb.SdmxComponentConstraint{
					datacommons.ComponentVariableMeasured: tt.constraint,
				},
			})
			if err == nil {
				t.Fatal("GetSdmxAvailability() error = nil, want error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("GetSdmxAvailability() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tt.want {
				t.Fatalf("GetSdmxAvailability() message = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPopulateSdmxFacetComponents(t *testing.T) {
	series := &sdmxpb.SdmxTimeSeries{
		Dimensions: map[string]string{
			datacommons.ComponentVariableMeasured: "Count_Person",
			datacommons.ComponentObservationAbout: "country/USA",
			datacommons.ComponentProvenance:       "dc/base/test_import",
		},
	}
	populateSdmxFacetComponents(series, map[string]interface{}{
		datacommons.ComponentUnit:              "Person",
		datacommons.ComponentMeasurementMethod: "Census",
		datacommons.ComponentObservationPeriod: "P1Y",
		datacommons.ComponentScalingFactor:     "0",
		datacommons.ComponentVariableMeasured:  "Count_Dropped",
		datacommons.ComponentObservationAbout:  "country/DROPPED",
		datacommons.ComponentProvenance:        "dc/dropped",
		datacommons.ComponentTimePeriod:        "2020",
		datacommons.ComponentObservationValue:  "99",
		datacommons.ComponentFacetID:           "json-facet-id",
		"customFacet":                          "drop",
		"nestedFacet":                          map[string]interface{}{"drop": "me"},
	})

	wantDimensionValues := map[string]string{
		datacommons.ComponentVariableMeasured:  "Count_Person",
		datacommons.ComponentObservationAbout:  "country/USA",
		datacommons.ComponentProvenance:        "dc/base/test_import",
		datacommons.ComponentUnit:              "Person",
		datacommons.ComponentMeasurementMethod: "Census",
		datacommons.ComponentObservationPeriod: "P1Y",
	}
	if len(series.Dimensions) != len(wantDimensionValues) {
		t.Fatalf("Dimensions = %v, want %v", series.Dimensions, wantDimensionValues)
	}
	for key, want := range wantDimensionValues {
		if got := series.Dimensions[key]; got != want {
			t.Fatalf("Dimensions[%q] = %q, want %q", key, got, want)
		}
	}

	wantAttributes := map[string]string{
		datacommons.ComponentScalingFactor: "0",
	}
	if len(series.Attributes) != len(wantAttributes) {
		t.Fatalf("Attributes = %v, want %v", series.Attributes, wantAttributes)
	}
	for key, want := range wantAttributes {
		if got := series.Attributes[key]; got != want {
			t.Fatalf("Attributes[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestPopulateSdmxFacetID(t *testing.T) {
	for _, tc := range []struct {
		name       string
		attributes map[string]string
		facetID    string
		want       map[string]string
	}{
		{
			name: "empty facet ID",
		},
		{
			name:    "without existing attributes",
			facetID: "stored-facet-id",
			want: map[string]string{
				datacommons.ComponentFacetID: "stored-facet-id",
			},
		},
		{
			name: "with existing attributes",
			attributes: map[string]string{
				datacommons.ComponentScalingFactor: "0",
			},
			facetID: "stored-facet-id",
			want: map[string]string{
				datacommons.ComponentScalingFactor: "0",
				datacommons.ComponentFacetID:       "stored-facet-id",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			series := &sdmxpb.SdmxTimeSeries{Attributes: tc.attributes}

			populateSdmxFacetID(series, tc.facetID)

			if diff := cmp.Diff(tc.want, series.Attributes); diff != "" {
				t.Fatalf("Attributes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSdmxDataShapeFacetID(t *testing.T) {
	shape := sdmxDataShape([]string{datacommons.ComponentObservationAbout})
	var facetIDComponent *sdmxpb.SdmxComponent
	for _, component := range shape.GetComponents() {
		if component.GetId() == datacommons.ComponentFacetID {
			facetIDComponent = component
			break
		}
	}

	if facetIDComponent == nil {
		t.Fatal("shape does not contain facet ID")
	}
	if got := facetIDComponent.GetKind(); got != sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_ATTRIBUTE {
		t.Fatalf("facet ID component kind = %v, want attribute", got)
	}
	if got := shape.GetComponents()[len(shape.GetComponents())-1].GetId(); got != datacommons.ComponentFacetID {
		t.Fatalf("last component = %q, want %q", got, datacommons.ComponentFacetID)
	}
}

func TestSdmxFacetComponentKind(t *testing.T) {
	tests := []struct {
		componentID string
		wantKind    datacommons.ComponentKind
		wantOK      bool
	}{
		{
			componentID: datacommons.ComponentUnit,
			wantKind:    datacommons.ComponentKindDimension,
			wantOK:      true,
		},
		{
			componentID: datacommons.ComponentScalingFactor,
			wantKind:    datacommons.ComponentKindAttribute,
			wantOK:      true,
		},
		{
			componentID: datacommons.ComponentProvenance,
		},
	}

	for _, tc := range tests {
		t.Run(tc.componentID, func(t *testing.T) {
			gotKind, gotOK := sdmxFacetComponentKind(tc.componentID)
			if gotOK != tc.wantOK {
				t.Fatalf("sdmxFacetComponentKind(%q) ok = %v, want %v", tc.componentID, gotOK, tc.wantOK)
			}
			if gotKind != tc.wantKind {
				t.Fatalf("sdmxFacetComponentKind(%q) kind = %q, want %q", tc.componentID, gotKind, tc.wantKind)
			}
		})
	}
}

func TestResolveSdmxEntityShape(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": {
			nil, // nil edge
			// Input order is intentionally non-alphabetical; sorted properties define entity slots.
			{
				Predicate: "observationProperties",
				Value:     " sourceCountry ",
			},
			{
				Predicate: "observationProperties",
				Value:     "destinationCountry",
			},
			{
				Predicate: "observationProperties",
				Value:     "destinationCountry",
			},
			{
				Predicate: "observationProperties",
				Value:     "",
			},
			{
				Predicate: "otherPredicate",
				Value:     "ignoredCountry",
			},
		},
		"var2": {
			{
				Predicate: "observationProperties",
				Value:     "",
			},
		},
	}

	gotObservationProperties, gotEntitySlotByObservationProperty, err := resolveSdmxEntityShape([]string{"var1"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}
	wantObservationProperties := []string{"destinationCountry", "sourceCountry"}
	if len(gotObservationProperties) != len(wantObservationProperties) {
		t.Fatalf("observationProperties = %v, want %v", gotObservationProperties, wantObservationProperties)
	}
	for i, want := range wantObservationProperties {
		if gotObservationProperties[i] != want {
			t.Fatalf("observationProperties = %v, want %v", gotObservationProperties, wantObservationProperties)
		}
	}
	wantEntitySlotByObservationProperty := map[string]string{
		"destinationCountry": "entity1",
		"sourceCountry":      "entity2",
	}
	if diff := cmp.Diff(wantEntitySlotByObservationProperty, gotEntitySlotByObservationProperty); diff != "" {
		t.Fatalf("entity slot mapping mismatch (-want +got):\n%s", diff)
	}

	gotObservationProperties, gotEntitySlotByObservationProperty, err = resolveSdmxEntityShape([]string{"var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}
	wantObservationProperties = []string{datacommons.ComponentObservationAbout}
	if len(gotObservationProperties) != len(wantObservationProperties) || gotObservationProperties[0] != wantObservationProperties[0] {
		t.Fatalf("observationProperties = %v, want %v", gotObservationProperties, wantObservationProperties)
	}
	wantEntitySlotByObservationProperty = map[string]string{
		datacommons.ComponentObservationAbout: "entity1",
	}
	if diff := cmp.Diff(wantEntitySlotByObservationProperty, gotEntitySlotByObservationProperty); diff != "" {
		t.Fatalf("entity slot mapping mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeAcceptsSameTwoEntityShapeDifferentOrder(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges("sourceCountry", "destinationCountry"),
		"var2": observationPropertiesEdges("destinationCountry", "sourceCountry"),
	}

	gotObservationProperties, gotEntitySlotByObservationProperty, err := resolveSdmxEntityShape([]string{"var1", "var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{"destinationCountry", "sourceCountry"}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationProperty := map[string]string{
		"destinationCountry": "entity1",
		"sourceCountry":      "entity2",
	}
	if diff := cmp.Diff(wantEntitySlotByObservationProperty, gotEntitySlotByObservationProperty); diff != "" {
		t.Fatalf("entity slot mapping mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeAcceptsSameThreeEntityShapeDifferentOrder(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges("sourceCountry", "transportMode", "destinationCountry"),
		"var2": observationPropertiesEdges("transportMode", "destinationCountry", "sourceCountry"),
	}

	gotObservationProperties, gotEntitySlotByObservationProperty, err := resolveSdmxEntityShape([]string{"var1", "var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{"destinationCountry", "sourceCountry", "transportMode"}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationProperty := map[string]string{
		"destinationCountry": "entity1",
		"sourceCountry":      "entity2",
		"transportMode":      "entity3",
	}
	if diff := cmp.Diff(wantEntitySlotByObservationProperty, gotEntitySlotByObservationProperty); diff != "" {
		t.Fatalf("entity slot mapping mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeUsesExplicitObservationAboutProperty(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges(datacommons.ComponentObservationAbout),
	}

	gotObservationProperties, gotEntitySlotByObservationProperty, err := resolveSdmxEntityShape([]string{"var1"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{datacommons.ComponentObservationAbout}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationProperty := map[string]string{
		datacommons.ComponentObservationAbout: "entity1",
	}
	if diff := cmp.Diff(wantEntitySlotByObservationProperty, gotEntitySlotByObservationProperty); diff != "" {
		t.Fatalf("entity slot mapping mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeAcceptsObservationAboutInMultiEntityShape(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges("sourceCountry", datacommons.ComponentObservationAbout, "destinationCountry"),
		"var2": observationPropertiesEdges(datacommons.ComponentObservationAbout, "destinationCountry", "sourceCountry"),
	}

	gotObservationProperties, gotEntitySlotByObservationProperty, err := resolveSdmxEntityShape([]string{"var1", "var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{"destinationCountry", datacommons.ComponentObservationAbout, "sourceCountry"}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationProperty := map[string]string{
		"destinationCountry":                  "entity1",
		datacommons.ComponentObservationAbout: "entity2",
		"sourceCountry":                       "entity3",
	}
	if diff := cmp.Diff(wantEntitySlotByObservationProperty, gotEntitySlotByObservationProperty); diff != "" {
		t.Fatalf("entity slot mapping mismatch (-want +got):\n%s", diff)
	}
}

func TestSdmxSeriesDimensionsUsesEntitySlotMapping(t *testing.T) {
	got := sdmxSeriesDimensions(
		"var1",
		map[string]string{
			"entity1": "country/MEX",
			"entity2": "country/USA",
		},
		map[string]string{
			"destinationCountry":                  "entity1",
			datacommons.ComponentObservationAbout: "entity2",
		},
	)

	want := map[string]string{
		datacommons.ComponentVariableMeasured: "var1",
		"destinationCountry":                  "country/MEX",
		datacommons.ComponentObservationAbout: "country/USA",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("sdmxSeriesDimensions() mismatch (-want +got):\n%s", diff)
	}
}

func TestValidateSdmxDataConstraintComponents(t *testing.T) {
	shape := sdmxDataShape([]string{"destinationCountry", "sourceCountry"})

	tests := []struct {
		name        string
		constraints map[string]*sdmxpb.SdmxComponentConstraint
		wantError   string
	}{
		{
			name: "known static filters pass",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured:  sdmxComponentConstraint("var1"),
				datacommons.ComponentUnit:              sdmxComponentConstraint("USD"),
				datacommons.ComponentMeasurementMethod: sdmxComponentConstraint("Census"),
				datacommons.ComponentObservationPeriod: sdmxComponentConstraint("P1Y"),
				datacommons.ComponentProvenance:        sdmxComponentConstraint("dc/base/test"),
			},
		},
		{
			name: "known entity filter passes",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				"destinationCountry":                  sdmxComponentConstraint("country/USA"),
			},
		},
		{
			name: "filterable attribute passes",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentFacetID:          sdmxComponentConstraint("facet"),
			},
		},
		{
			name: "observationAbout not in shape fails",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentObservationAbout: sdmxComponentConstraint("country/USA"),
			},
			wantError: "unsupported SDMX component filter \"observationAbout\"; filterable components are [destinationCountry facetId measurementMethod observationPeriod provenance sourceCountry unit variableMeasured]",
		},
		{
			name: "unknown dynamic filter fails",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				"customEntity":                        sdmxComponentConstraint("country/USA"),
			},
			wantError: "unsupported SDMX component filter \"customEntity\"; filterable components are [destinationCountry facetId measurementMethod observationPeriod provenance sourceCountry unit variableMeasured]",
		},
		{
			name: "time period fails",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentTimePeriod:       sdmxComponentConstraint("2020"),
			},
			wantError: "unsupported SDMX component filter \"TIME_PERIOD\"; filterable components are [destinationCountry facetId measurementMethod observationPeriod provenance sourceCountry unit variableMeasured]",
		},
		{
			name: "measure fails",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentObservationValue: sdmxComponentConstraint("10"),
			},
			wantError: "unsupported SDMX component filter \"OBS_VALUE\"; filterable components are [destinationCountry facetId measurementMethod observationPeriod provenance sourceCountry unit variableMeasured]",
		},
		{
			name: "attribute fails",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentScalingFactor:    sdmxComponentConstraint("0"),
			},
			wantError: "unsupported SDMX component filter \"scalingFactor\"; filterable components are [destinationCountry facetId measurementMethod observationPeriod provenance sourceCountry unit variableMeasured]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSdmxDataConstraintComponents(tt.constraints, shape)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateSdmxDataConstraintComponents() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("validateSdmxDataConstraintComponents() error = nil, want error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("validateSdmxDataConstraintComponents() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tt.wantError {
				t.Fatalf("validateSdmxDataConstraintComponents() message = %q, want %q", got, tt.wantError)
			}
		})
	}
}

func TestValidateSdmxRequiredObservationProperty(t *testing.T) {
	tests := []struct {
		name                  string
		constraints           map[string]*sdmxpb.SdmxComponentConstraint
		observationProperties []string
		wantError             string
	}{
		{
			name: "one observation property passes",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"destinationCountry": sdmxComponentConstraint("country/CAN"),
			},
			observationProperties: []string{"destinationCountry", "sourceCountry"},
		},
		{
			name: "multiple observation properties pass",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				"destinationCountry": sdmxComponentConstraint("country/CAN"),
				"sourceCountry":      sdmxComponentConstraint("country/USA"),
			},
			observationProperties: []string{"destinationCountry", "sourceCountry"},
		},
		{
			name: "filterable attribute does not satisfy requirement",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentFacetID:          sdmxComponentConstraint("facet"),
			},
			observationProperties: []string{"destinationCountry", "sourceCountry"},
			wantError:             "SDMX data query must include at least one observation property filter; allowed observation properties are [destinationCountry sourceCountry]",
		},
		{
			name: "fallback observation about is required",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
			},
			observationProperties: []string{datacommons.ComponentObservationAbout},
			wantError:             "SDMX data query must include at least one observation property filter; allowed observation properties are [observationAbout]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSdmxRequiredObservationProperty(tt.constraints, tt.observationProperties)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateSdmxRequiredObservationProperty() error = %v, want nil", err)
				}
				return
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("validateSdmxRequiredObservationProperty() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tt.wantError {
				t.Fatalf("validateSdmxRequiredObservationProperty() message = %q, want %q", got, tt.wantError)
			}
		})
	}
}

func TestFilterableAttributesHaveStaticDataColumns(t *testing.T) {
	for componentID := range datacommons.FilterableAttributes {
		kind, ok := datacommons.DataComponentKind(componentID)
		if !ok || kind != datacommons.ComponentKindAttribute {
			t.Errorf("filterable attribute %q is not registered as an attribute", componentID)
		}
		column, ok := sdmxStaticDataFilterColumn(componentID)
		if !ok || column == "" {
			t.Errorf("filterable attribute %q has no static data column", componentID)
		}
	}
}

func TestValidateSdmxAvailabilityComponent(t *testing.T) {
	shape := sdmxDataShape([]string{"destinationCountry", "sourceCountry"})
	for _, tt := range []struct {
		name        string
		componentID string
		wantError   bool
	}{
		{name: "variable measured", componentID: datacommons.ComponentVariableMeasured},
		{name: "dynamic dimension", componentID: "destinationCountry"},
		{name: "fixed dimension", componentID: datacommons.ComponentUnit},
		{name: "dimension absent from shape", componentID: datacommons.ComponentObservationAbout, wantError: true},
		{name: "time period", componentID: datacommons.ComponentTimePeriod, wantError: true},
		{name: "measure", componentID: datacommons.ComponentObservationValue, wantError: true},
		{name: "attribute", componentID: datacommons.ComponentScalingFactor, wantError: true},
		{name: "facet ID attribute", componentID: datacommons.ComponentFacetID, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSdmxAvailabilityComponent(tt.componentID, shape)
			if tt.wantError && status.Code(err) != codes.InvalidArgument {
				t.Fatalf("validateSdmxAvailabilityComponent() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if !tt.wantError && err != nil {
				t.Fatalf("validateSdmxAvailabilityComponent() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateSdmxConstraintValues(t *testing.T) {
	tests := []struct {
		name        string
		constraints map[string]*sdmxpb.SdmxComponentConstraint
		wantError   string
	}{
		{
			name: "multiple values pass",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1", "var2"),
				datacommons.ComponentUnit:             sdmxComponentConstraint("Count", "Percent"),
			},
		},
		{
			name:        "missing variable measured fails",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{},
			wantError:   "SDMX component filter variableMeasured must be specified",
		},
		{
			name: "nil values fail",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentUnit:             nil,
			},
			wantError: "SDMX component filter \"unit\" must have at least one value",
		},
		{
			name: "empty values fail",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentUnit:             {},
			},
			wantError: "SDMX component filter \"unit\" must have at least one value",
		},
		{
			name: "blank value fails",
			constraints: map[string]*sdmxpb.SdmxComponentConstraint{
				datacommons.ComponentVariableMeasured: sdmxComponentConstraint("var1"),
				datacommons.ComponentUnit:             sdmxComponentConstraint(" "),
			},
			wantError: "SDMX component filter \"unit\" contains an empty value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSdmxConstraintValues(tt.constraints)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateSdmxConstraintValues() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("validateSdmxConstraintValues() error = nil, want error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("validateSdmxConstraintValues() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tt.wantError {
				t.Fatalf("validateSdmxConstraintValues() message = %q, want %q", got, tt.wantError)
			}
		})
	}
}

func TestResolveSdmxEntityShapeTooManyProperties(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": {
			{Predicate: "observationProperties", Value: "destinationCountry"},
			{Predicate: "observationProperties", Value: "intermediaryCountry"},
			{Predicate: "observationProperties", Value: "sourceCountry"},
			{Predicate: "observationProperties", Value: "transportMode"},
		},
	}

	_, _, err := resolveSdmxEntityShape([]string{"var1"}, observationPropertyEdgesByStatVar)
	if err == nil {
		t.Fatal("resolveSdmxEntityShape() error = nil, want error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("resolveSdmxEntityShape() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
	want := "resolveSdmxEntityShape: stat var \"var1\" has 4 observationProperties; max supported entity slots is 3"
	if got := status.Convert(err).Message(); got != want {
		t.Fatalf("resolveSdmxEntityShape() message = %q, want %q", got, want)
	}
}

func TestResolveSdmxEntityShapeIncompatibleVariables(t *testing.T) {
	tests := []struct {
		name                              string
		vars                              []string
		observationPropertyEdgesByStatVar map[string][]*Edge
		want                              string
	}{
		{
			name: "single and multi",
			vars: []string{"multi", "single"},
			observationPropertyEdgesByStatVar: map[string][]*Edge{
				"single": nil,
				"multi":  observationPropertiesEdges("destinationCountry", "sourceCountry"),
			},
			want: "resolveSdmxEntityShape: incompatible observationProperties for stat var \"single\": got [observationAbout], want [destinationCountry sourceCountry] from stat var \"multi\"",
		},
		{
			name: "same count different property",
			vars: []string{"var1", "var2"},
			observationPropertyEdgesByStatVar: map[string][]*Edge{
				"var1": observationPropertiesEdges("destinationCountry", "sourceCountry"),
				"var2": observationPropertiesEdges("destinationCountry", "originCountry"),
			},
			want: "resolveSdmxEntityShape: incompatible observationProperties for stat var \"var2\": got [destinationCountry originCountry], want [destinationCountry sourceCountry] from stat var \"var1\"",
		},
		{
			name: "different property count",
			vars: []string{"var1", "var2"},
			observationPropertyEdgesByStatVar: map[string][]*Edge{
				"var1": observationPropertiesEdges("destinationCountry", "sourceCountry"),
				"var2": observationPropertiesEdges("destinationCountry", "sourceCountry", "transportMode"),
			},
			want: "resolveSdmxEntityShape: incompatible observationProperties for stat var \"var2\": got [destinationCountry sourceCountry transportMode], want [destinationCountry sourceCountry] from stat var \"var1\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := resolveSdmxEntityShape(tt.vars, tt.observationPropertyEdgesByStatVar)
			if err == nil {
				t.Fatal("resolveSdmxEntityShape() error = nil, want error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("resolveSdmxEntityShape() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got := status.Convert(err).Message(); got != tt.want {
				t.Fatalf("resolveSdmxEntityShape() message = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveSdmxEntityShapeRejectsReservedObservationProperties(t *testing.T) {
	reservedComponents := []string{
		datacommons.ComponentVariableMeasured,
		datacommons.ComponentUnit,
		datacommons.ComponentMeasurementMethod,
		datacommons.ComponentObservationPeriod,
		datacommons.ComponentProvenance,
		datacommons.ComponentTimePeriod,
		datacommons.ComponentObservationValue,
		datacommons.ComponentScalingFactor,
	}

	for _, componentID := range reservedComponents {
		t.Run(componentID, func(t *testing.T) {
			_, _, err := resolveSdmxEntityShape(
				[]string{"var1"},
				map[string][]*Edge{"var1": observationPropertiesEdges(componentID)},
			)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("resolveSdmxEntityShape() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			want := fmt.Sprintf("reserved observationProperty %q", componentID)
			if got := status.Convert(err).Message(); !strings.Contains(got, want) {
				t.Fatalf("resolveSdmxEntityShape() message = %q, want substring %q", got, want)
			}
		})
	}
}

func observationPropertiesEdges(properties ...string) []*Edge {
	edges := make([]*Edge, 0, len(properties))
	for _, property := range properties {
		edges = append(edges, &Edge{
			Predicate: "observationProperties",
			Value:     property,
		})
	}
	return edges
}

func TestObservationPropertiesEntityMappingPageSize(t *testing.T) {
	tests := []struct {
		name          string
		variableCount int
		want          int
	}{
		{
			name:          "uses minimum page size for small requests",
			variableCount: 1,
			want:          minObservationPropertiesPageSize,
		},
		{
			name:          "uses minimum page size at boundary",
			variableCount: 25,
			want:          minObservationPropertiesPageSize,
		},
		{
			name:          "scales past minimum",
			variableCount: 26,
			want:          104,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := observationPropertiesPageSize(tc.variableCount); got != tc.want {
				t.Fatalf("observationPropertiesPageSize(%d) = %d, want %d", tc.variableCount, got, tc.want)
			}
		})
	}
}
