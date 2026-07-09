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
)

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
	if err == nil {
		t.Fatal("GetSdmxObservations() with nil request expected error, got nil")
	}
	if got, want := err.Error(), "GetSdmxObservations: request cannot be nil"; got != want {
		t.Fatalf("GetSdmxObservations() error = %q, want %q", got, want)
	}
}

func TestMultiEntityGetSdmxAvailabilityNilRequestReturnsError(t *testing.T) {
	client := &multiEntityClient{}
	_, err := client.GetSdmxAvailability(context.Background(), nil)
	if err == nil {
		t.Fatal("GetSdmxAvailability() with nil request expected error, got nil")
	}
	if got, want := err.Error(), "GetSdmxAvailability: request cannot be nil"; got != want {
		t.Fatalf("GetSdmxAvailability() error = %q, want %q", got, want)
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

	gotObservationProperties, gotEntitySlotByObservationPropertyByStatVar, err := resolveSdmxEntityShape([]string{"var1"}, observationPropertyEdgesByStatVar)
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
	wantEntitySlotByObservationPropertyByStatVar := map[string]map[string]string{
		"var1": {
			"destinationCountry": "entity1",
			"sourceCountry":      "entity2",
		},
	}
	if len(gotEntitySlotByObservationPropertyByStatVar) != len(wantEntitySlotByObservationPropertyByStatVar) {
		t.Fatalf("entity slot mappings = %v, want %v", gotEntitySlotByObservationPropertyByStatVar, wantEntitySlotByObservationPropertyByStatVar)
	}
	for statVarID, gotEntitySlotByObservationProperty := range gotEntitySlotByObservationPropertyByStatVar {
		wantEntitySlotByObservationProperty := wantEntitySlotByObservationPropertyByStatVar[statVarID]
		if len(gotEntitySlotByObservationProperty) != len(wantEntitySlotByObservationProperty) {
			t.Fatalf("Entity slot mapping for %s = %v, want %v", statVarID, gotEntitySlotByObservationProperty, wantEntitySlotByObservationProperty)
		}
		for k, v := range gotEntitySlotByObservationProperty {
			if wantEntitySlotByObservationProperty[k] != v {
				t.Errorf("Entity slot mapping for %s[%q] = %q, want %q", statVarID, k, v, wantEntitySlotByObservationProperty[k])
			}
		}
	}

	gotObservationProperties, gotEntitySlotByObservationPropertyByStatVar, err = resolveSdmxEntityShape([]string{"var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}
	wantObservationProperties = []string{datacommons.ComponentObservationAbout}
	if len(gotObservationProperties) != len(wantObservationProperties) || gotObservationProperties[0] != wantObservationProperties[0] {
		t.Fatalf("observationProperties = %v, want %v", gotObservationProperties, wantObservationProperties)
	}
	wantEntitySlotByObservationPropertyByStatVar = map[string]map[string]string{
		"var2": {
			datacommons.ComponentObservationAbout: "entity1",
		},
	}
	if diff := cmp.Diff(wantEntitySlotByObservationPropertyByStatVar, gotEntitySlotByObservationPropertyByStatVar); diff != "" {
		t.Fatalf("entity slot mappings mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeAcceptsSameTwoEntityShapeDifferentOrder(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges("sourceCountry", "destinationCountry"),
		"var2": observationPropertiesEdges("destinationCountry", "sourceCountry"),
	}

	gotObservationProperties, gotEntitySlotByObservationPropertyByStatVar, err := resolveSdmxEntityShape([]string{"var1", "var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{"destinationCountry", "sourceCountry"}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationPropertyByStatVar := map[string]map[string]string{
		"var1": {
			"destinationCountry": "entity1",
			"sourceCountry":      "entity2",
		},
		"var2": {
			"destinationCountry": "entity1",
			"sourceCountry":      "entity2",
		},
	}
	if diff := cmp.Diff(wantEntitySlotByObservationPropertyByStatVar, gotEntitySlotByObservationPropertyByStatVar); diff != "" {
		t.Fatalf("entity slot mappings mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeAcceptsSameThreeEntityShapeDifferentOrder(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges("sourceCountry", "transportMode", "destinationCountry"),
		"var2": observationPropertiesEdges("transportMode", "destinationCountry", "sourceCountry"),
	}

	gotObservationProperties, gotEntitySlotByObservationPropertyByStatVar, err := resolveSdmxEntityShape([]string{"var1", "var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{"destinationCountry", "sourceCountry", "transportMode"}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationPropertyByStatVar := map[string]map[string]string{
		"var1": {
			"destinationCountry": "entity1",
			"sourceCountry":      "entity2",
			"transportMode":      "entity3",
		},
		"var2": {
			"destinationCountry": "entity1",
			"sourceCountry":      "entity2",
			"transportMode":      "entity3",
		},
	}
	if diff := cmp.Diff(wantEntitySlotByObservationPropertyByStatVar, gotEntitySlotByObservationPropertyByStatVar); diff != "" {
		t.Fatalf("entity slot mappings mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeUsesExplicitObservationAboutProperty(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges(datacommons.ComponentObservationAbout),
	}

	gotObservationProperties, gotEntitySlotByObservationPropertyByStatVar, err := resolveSdmxEntityShape([]string{"var1"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{datacommons.ComponentObservationAbout}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationPropertyByStatVar := map[string]map[string]string{
		"var1": {
			datacommons.ComponentObservationAbout: "entity1",
		},
	}
	if diff := cmp.Diff(wantEntitySlotByObservationPropertyByStatVar, gotEntitySlotByObservationPropertyByStatVar); diff != "" {
		t.Fatalf("entity slot mappings mismatch (-want +got):\n%s", diff)
	}
}

func TestResolveSdmxEntityShapeAcceptsObservationAboutInMultiEntityShape(t *testing.T) {
	observationPropertyEdgesByStatVar := map[string][]*Edge{
		"var1": observationPropertiesEdges("sourceCountry", datacommons.ComponentObservationAbout, "destinationCountry"),
		"var2": observationPropertiesEdges(datacommons.ComponentObservationAbout, "destinationCountry", "sourceCountry"),
	}

	gotObservationProperties, gotEntitySlotByObservationPropertyByStatVar, err := resolveSdmxEntityShape([]string{"var1", "var2"}, observationPropertyEdgesByStatVar)
	if err != nil {
		t.Fatalf("resolveSdmxEntityShape() error = %v", err)
	}

	wantObservationProperties := []string{"destinationCountry", datacommons.ComponentObservationAbout, "sourceCountry"}
	if diff := cmp.Diff(wantObservationProperties, gotObservationProperties); diff != "" {
		t.Fatalf("observationProperties mismatch (-want +got):\n%s", diff)
	}
	wantEntitySlotByObservationPropertyByStatVar := map[string]map[string]string{
		"var1": {
			"destinationCountry":                  "entity1",
			datacommons.ComponentObservationAbout: "entity2",
			"sourceCountry":                       "entity3",
		},
		"var2": {
			"destinationCountry":                  "entity1",
			datacommons.ComponentObservationAbout: "entity2",
			"sourceCountry":                       "entity3",
		},
	}
	if diff := cmp.Diff(wantEntitySlotByObservationPropertyByStatVar, gotEntitySlotByObservationPropertyByStatVar); diff != "" {
		t.Fatalf("entity slot mappings mismatch (-want +got):\n%s", diff)
	}
}

func TestSdmxSeriesDimensionsUsesEntitySlotMapping(t *testing.T) {
	got := sdmxSeriesDimensions(
		"var1",
		map[string]string{
			"entity1": "country/MEX",
			"entity2": "country/USA",
		},
		map[string]map[string]string{
			"var1": {
				"destinationCountry":                  "entity1",
				datacommons.ComponentObservationAbout: "entity2",
			},
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
	entitySlotByObservationPropertyByStatVar := map[string]map[string]string{
		"var1": {
			"destinationCountry": "entity1",
			"sourceCountry":      "entity2",
		},
	}

	tests := []struct {
		name        string
		constraints map[string]*sdmxpb.ConstraintList
		wantError   string
	}{
		{
			name: "known static filters pass",
			constraints: map[string]*sdmxpb.ConstraintList{
				datacommons.ComponentVariableMeasured:  {Values: []string{"var1"}},
				datacommons.ComponentUnit:              {Values: []string{"USD"}},
				datacommons.ComponentMeasurementMethod: {Values: []string{"Census"}},
				datacommons.ComponentObservationPeriod: {Values: []string{"P1Y"}},
				datacommons.ComponentProvenance:        {Values: []string{"dc/base/test"}},
			},
		},
		{
			name: "known entity filter passes",
			constraints: map[string]*sdmxpb.ConstraintList{
				datacommons.ComponentVariableMeasured: {Values: []string{"var1"}},
				"destinationCountry":                  {Values: []string{"country/USA"}},
			},
		},
		{
			name: "observationAbout not in shape fails",
			constraints: map[string]*sdmxpb.ConstraintList{
				datacommons.ComponentVariableMeasured: {Values: []string{"var1"}},
				datacommons.ComponentObservationAbout: {Values: []string{"country/USA"}},
			},
			wantError: "GetSdmxObservations: unsupported SDMX component filter \"observationAbout\" for stat var \"var1\"; resolved observationProperties are [destinationCountry sourceCountry]",
		},
		{
			name: "unknown dynamic filter fails",
			constraints: map[string]*sdmxpb.ConstraintList{
				datacommons.ComponentVariableMeasured: {Values: []string{"var1"}},
				"customEntity":                        {Values: []string{"country/USA"}},
			},
			wantError: "GetSdmxObservations: unsupported SDMX component filter \"customEntity\" for stat var \"var1\"; resolved observationProperties are [destinationCountry sourceCountry]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSdmxDataConstraintComponents(tt.constraints, []string{"var1"}, entitySlotByObservationPropertyByStatVar)
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
