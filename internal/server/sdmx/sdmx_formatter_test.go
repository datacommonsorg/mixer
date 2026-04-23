package sdmx

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	pb_int "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/google/go-cmp/cmp"
)

func TestJSONStatFormatter_Golden(t *testing.T) {
	formatter := &JSONStatFormatter{}

	tests := []struct {
		name       string
		goldenFile string
		obs        []*pb_int.SdmxObservation
	}{
		{
			name:       "Basic grid with missing cell",
			goldenFile: "basic_grid.json",
			obs: []*pb_int.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					DatesAndValues: []*pb_int.SdmxDateValue{
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
					DatesAndValues: []*pb_int.SdmxDateValue{
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
			obs: []*pb_int.SdmxObservation{
				{
					VariableMeasured: "Exports",
					DatesAndValues: []*pb_int.SdmxDateValue{
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
			obs: []*pb_int.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					DatesAndValues: []*pb_int.SdmxDateValue{
						{Date: "2020", Value: "331000000"},
					},
					Dimensions: map[string]string{
						"gender":           "Male",
						"observationAbout": "country/USA",
					},
				},
				{
					VariableMeasured: "Count_Person",
					DatesAndValues: []*pb_int.SdmxDateValue{
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
			obs: []*pb_int.SdmxObservation{
				{
					VariableMeasured: "Count_Person",
					Provenance:       "prov1",
					DatesAndValues: []*pb_int.SdmxDateValue{
						{Date: "2020", Value: "331000000"},
					},
					Dimensions: map[string]string{
						"gender":           "Male",
						"observationAbout": "country/USA",
					},
					Attributes: map[string]string{
						"unit":              "Person",
						"measurementMethod": "Census",
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
