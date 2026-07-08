package jsonstatv2

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/google/go-cmp/cmp"
)

func TestJSONStatFormatter_Golden(t *testing.T) {
	formatter := &JSONStatFormatter{}

	tests := []struct {
		name       string
		goldenFile string
		obs        []*sdmxpb.SdmxObservation
	}{
		{
			name:       "Basic grid with missing cell",
			goldenFile: "basic_grid.json",
			obs: []*sdmxpb.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					DatesAndValues: []*sdmxpb.SdmxDateValue{
						{Date: "2020", Value: "331000000"},
						{Date: "2021", Value: "332000000"},
					},
					Dimensions: map[string]string{
						"gender":           "Male",
						"observationAbout": "country/USA",
					},
				},
				{
					VariableMeasured: "Count_Person",
					DatesAndValues: []*sdmxpb.SdmxDateValue{
						{Date: "2020", Value: "160000000"},
					},
					Dimensions: map[string]string{
						"gender":           "Female",
						"observationAbout": "country/USA",
					},
				},
			},
		},
		{
			name:       "No observationAbout",
			goldenFile: "no_observation_about.json",
			obs: []*sdmxpb.SdmxObservation{
				{
					VariableMeasured: "Exports",
					DatesAndValues: []*sdmxpb.SdmxDateValue{
						{Date: "2020", Value: "1000"},
					},
					Dimensions: map[string]string{
						"source":      "country/USA",
						"destination": "country/CHN",
					},
				},
			},
		},
		{
			name:       "Missing Dimensions",
			goldenFile: "missing_dimensions.json",
			obs: []*sdmxpb.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					DatesAndValues: []*sdmxpb.SdmxDateValue{
						{Date: "2020", Value: "331000000"},
					},
					Dimensions: map[string]string{
						"gender":           "Male",
						"observationAbout": "country/USA",
					},
				},
				{
					VariableMeasured: "Count_Person",
					DatesAndValues: []*sdmxpb.SdmxDateValue{
						{Date: "2020", Value: "332000000"},
					},
					Dimensions: map[string]string{
						"observationAbout": "country/USA",
					},
				},
			},
		},
		{
			name:       "Facet Attributes",
			goldenFile: "facet_attributes.json",
			obs: []*sdmxpb.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					Provenance:       "prov1",
					DatesAndValues: []*sdmxpb.SdmxDateValue{
						{Date: "2020", Value: "331000000"},
					},
					Dimensions: map[string]string{
						"gender":            "Male",
						"measurementMethod": "Census",
						"observationAbout":  "country/USA",
						"observationPeriod": "P1Y",
						"unit":              "Person",
					},
					Attributes: map[string]string{
						"scalingFactor": "0",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := formatter.Format(tt.obs)
			if err != nil {
				t.Fatalf("Format() error: %v", err)
			}

			// Construct expected map
			var gotMap map[string]interface{}
			if err := json.Unmarshal([]byte(output), &gotMap); err != nil {
				t.Fatalf("Failed to unmarshal output: %v", err)
			}

			goldenPath := filepath.Join("golden", tt.goldenFile)

			// Update golden files if UPDATE_GOLDEN=true
			if os.Getenv("UPDATE_GOLDEN") == "true" {
				b, err := json.MarshalIndent(gotMap, "", "  ")
				if err != nil {
					t.Fatalf("Failed to marshal golden output: %v", err)
				}
				if err := os.WriteFile(goldenPath, b, 0644); err != nil {
					t.Fatalf("Failed to write golden file: %v", err)
				}
				return
			}

			goldenBytes, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("Failed to read golden file %q: %v", goldenPath, err)
			}

			var wantMap map[string]interface{}
			if err := json.Unmarshal(goldenBytes, &wantMap); err != nil {
				t.Fatalf("Failed to unmarshal golden file: %v", err)
			}

			if diff := cmp.Diff(wantMap, gotMap); diff != "" {
				t.Errorf("Formatter output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestJSONStatFormatter_SDMXComponentNames(t *testing.T) {
	formatter := &JSONStatFormatter{}
	output, err := formatter.Format([]*sdmxpb.SdmxObservation{
		{
			VariableMeasured: "Count_Person",
			DatesAndValues: []*sdmxpb.SdmxDateValue{
				{Date: "2020", Value: "1"},
			},
			Dimensions: map[string]string{
				"gender": "Male",
			},
		},
		{
			VariableMeasured: "Count_Person",
			DatesAndValues: []*sdmxpb.SdmxDateValue{
				{Date: "2020", Value: "2"},
			},
			Dimensions: map[string]string{},
		},
	})
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	dimensions := got["dimension"].(map[string]interface{})
	if _, ok := dimensions[datacommons.ComponentTimePeriod]; !ok {
		t.Fatalf("Missing %q dimension in output", datacommons.ComponentTimePeriod)
	}
	if _, ok := dimensions["observationDate"]; ok {
		t.Fatal("Output should not include legacy observationDate dimension")
	}
	if _, ok := got["value"]; !ok {
		t.Fatal("JSON-stat output should keep top-level value field")
	}
	if _, ok := got[datacommons.ComponentObservationValue]; ok {
		t.Fatalf("JSON-stat output should not rename top-level value field to %q", datacommons.ComponentObservationValue)
	}

	gender := dimensions["gender"].(map[string]interface{})
	category := gender["category"].(map[string]interface{})
	indices := category["index"].([]interface{})
	if !containsJSONValue(indices, datacommons.FallbackNotAvailable) {
		t.Fatalf("Missing fallback category %q in gender index: %v", datacommons.FallbackNotAvailable, indices)
	}

	extension := got["extension"].(map[string]interface{})
	measures := extension["measures"].(map[string]interface{})
	if _, ok := measures[datacommons.ComponentObservationValue]; !ok {
		t.Fatalf("Missing %q measure metadata in extension", datacommons.ComponentObservationValue)
	}
}

func containsJSONValue(values []interface{}, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
