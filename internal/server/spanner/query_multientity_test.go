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
	obs := &sdmxpb.SdmxObservation{
		Provenance: "dc/base/test_import",
		Dimensions: map[string]string{
			datacommons.ComponentVariableMeasured: "Count_Person",
			datacommons.ComponentObservationAbout: "country/USA",
		},
	}
	populateSdmxFacetComponents(obs, map[string]interface{}{
		datacommons.ComponentUnit:              "Person",
		datacommons.ComponentMeasurementMethod: "Census",
		datacommons.ComponentObservationPeriod: "P1Y",
		datacommons.ComponentScalingFactor:     "0",
		"customFacet":                          "drop",
		"nestedFacet":                          map[string]interface{}{"drop": "me"},
	})

	wantDimensions := map[string]string{
		datacommons.ComponentVariableMeasured:  "Count_Person",
		datacommons.ComponentObservationAbout:  "country/USA",
		datacommons.ComponentUnit:              "Person",
		datacommons.ComponentMeasurementMethod: "Census",
		datacommons.ComponentObservationPeriod: "P1Y",
	}
	if len(obs.Dimensions) != len(wantDimensions) {
		t.Fatalf("Dimensions = %v, want %v", obs.Dimensions, wantDimensions)
	}
	for key, want := range wantDimensions {
		if got := obs.Dimensions[key]; got != want {
			t.Fatalf("Dimensions[%q] = %q, want %q", key, got, want)
		}
	}
	if _, ok := obs.Dimensions[datacommons.ComponentProvenance]; ok {
		t.Fatalf("Dimensions[%q] is set; provenance should remain top-level only", datacommons.ComponentProvenance)
	}

	wantAttributes := map[string]string{
		datacommons.ComponentScalingFactor: "0",
	}
	if len(obs.Attributes) != len(wantAttributes) {
		t.Fatalf("Attributes = %v, want %v", obs.Attributes, wantAttributes)
	}
	for key, want := range wantAttributes {
		if got := obs.Attributes[key]; got != want {
			t.Fatalf("Attributes[%q] = %q, want %q", key, got, want)
		}
	}
	if got, want := obs.Provenance, "dc/base/test_import"; got != want {
		t.Fatalf("Provenance = %q, want %q", got, want)
	}
}

func TestObservationPropertiesEntityMappings(t *testing.T) {
	edgesMap := map[string][]*Edge{
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

	got, err := observationPropertiesEntityMappings(edgesMap)
	if err != nil {
		t.Fatalf("observationPropertiesEntityMappings() error = %v", err)
	}
	want := map[string]map[string]string{
		"var1": {
			"destinationCountry": "entity1",
			"sourceCountry":      "entity2",
		},
	}

	if len(got) != len(want) {
		t.Fatalf("observationPropertiesEntityMappings() = %v, want %v", got, want)
	}
	for varDcid, gotMapping := range got {
		wantMapping := want[varDcid]
		if len(gotMapping) != len(wantMapping) {
			t.Fatalf("Mapping for %s = %v, want %v", varDcid, gotMapping, wantMapping)
		}
		for k, v := range gotMapping {
			if wantMapping[k] != v {
				t.Errorf("Mapping for %s[%q] = %q, want %q", varDcid, k, v, wantMapping[k])
			}
		}
	}
}

func TestObservationPropertiesEntityMappingsTooManyProperties(t *testing.T) {
	edgesMap := map[string][]*Edge{
		"var1": {
			{Predicate: "observationProperties", Value: "destinationCountry"},
			{Predicate: "observationProperties", Value: "intermediaryCountry"},
			{Predicate: "observationProperties", Value: "sourceCountry"},
			{Predicate: "observationProperties", Value: "transportMode"},
		},
	}

	_, err := observationPropertiesEntityMappings(edgesMap)
	if err == nil {
		t.Fatal("observationPropertiesEntityMappings() error = nil, want error")
	}
	want := "observationPropertiesEntityMappings: stat var \"var1\" has 4 observationProperties; max supported entity slots is 3"
	if err.Error() != want {
		t.Fatalf("observationPropertiesEntityMappings() error = %q, want %q", err.Error(), want)
	}
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
