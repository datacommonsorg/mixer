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

package dispatcher

import (
	"encoding/json"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/google/go-cmp/cmp"

)

func TestJSONStatFormatter(t *testing.T) {
	formatter := &JSONStatFormatter{}

	obs := []*datasource.SdmxObservation{
		{
			VariableMeasured: "Count_Person",
			DatesAndValues: []*datasource.SdmxDateValue{
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
			DatesAndValues: []*datasource.SdmxDateValue{
				{Date: "2020", Value: "160000000"},
			},
			Dimensions: map[string]string{
				"gender":           "Female",
				"observationAbout": "country/USA",
			},
		},
	}


	output, err := formatter.Format(obs)
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}

	// Parse output to verify structure
	var resp JSONStatResponse
	if err := json.Unmarshal([]byte(output), &resp); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	// Verify ID order
	wantId := []string{"variableMeasured", "gender", "observationAbout", "observationDate"}
	if diff := cmp.Diff(wantId, resp.Id); diff != "" {
		t.Errorf("Dimension ID mismatch (-want +got):\n%s", diff)
	}

	// Verify size
	wantSize := []int{1, 2, 1, 2} // variable:1, gender:2, place:1, date:2
	if diff := cmp.Diff(wantSize, resp.Size); diff != "" {
		t.Errorf("Size mismatch (-want +got):\n%s", diff)
	}

	// Verify values length
	if len(resp.Value) != 4 {
		t.Errorf("Value length = %d, want 4", len(resp.Value))
	}

	// Verify specific values (Female 2021 should be null!)
	// Index calculation:
	// variable: Count_Person (0)
	// gender: Female (0), Male (1) - sorted
	// place: country/USA (0)
	// date: 2020 (0), 2021 (1)
	//
	// Female (0), 2020 (0) -> index 0
	// Female (0), 2021 (1) -> index 1 -> null
	// Male (1), 2020 (0)   -> index 2
	// Male (1), 2021 (1)   -> index 3

	wantValues := []interface{}{160000000.0, nil, 331000000.0, 332000000.0}
	if diff := cmp.Diff(wantValues, resp.Value); diff != "" {
		t.Errorf("Values mismatch (-want +got):\n%s", diff)
	}
}

func TestJSONStatFormatter_NoObservationAbout(t *testing.T) {
	formatter := &JSONStatFormatter{}

	obs := []*datasource.SdmxObservation{
		{
			VariableMeasured: "Exports",
			DatesAndValues: []*datasource.SdmxDateValue{
				{Date: "2020", Value: "1000"},
			},
			Dimensions: map[string]string{
				"source":      "country/USA",
				"destination": "country/CHN",
			},
		},
	}


	output, err := formatter.Format(obs)
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}

	var resp JSONStatResponse
	if err := json.Unmarshal([]byte(output), &resp); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	// Verify ID does NOT contain observationAbout
	for _, id := range resp.Id {
		if id == "observationAbout" {
			t.Errorf("Found observationAbout in dimensions, want it omitted")
		}
	}

	// Verify sizes (variable:1, destination:1, source:1, date:1)
	// They are sorted alphabetically after variableMeasured, so:
	// destination, source
	wantId := []string{"variableMeasured", "destination", "source", "observationDate"}
	if diff := cmp.Diff(wantId, resp.Id); diff != "" {
		t.Errorf("Dimension ID mismatch (-want +got):\n%s", diff)
	}
}

func TestJSONStatFormatter_MissingDimensions(t *testing.T) {
	formatter := &JSONStatFormatter{}

	obs := []*datasource.SdmxObservation{
		{
			VariableMeasured: "Count_Person",
			DatesAndValues: []*datasource.SdmxDateValue{
				{Date: "2020", Value: "331000000"},
			},
			Dimensions: map[string]string{
				"gender":           "Male",
				"observationAbout": "country/USA",
			},
		},
		{
			VariableMeasured: "Count_Person",
			DatesAndValues: []*datasource.SdmxDateValue{
				{Date: "2020", Value: "332000000"},
			},
			// gender is missing!
			Dimensions: map[string]string{
				"observationAbout": "country/USA",
			},
		},
	}


	output, err := formatter.Format(obs)
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}

	var resp JSONStatResponse
	if err := json.Unmarshal([]byte(output), &resp); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	// Verify size
	// variable:1, gender:2 (Male, _N/A_), place:1, date:1
	wantSize := []int{1, 2, 1, 1}
	if diff := cmp.Diff(wantSize, resp.Size); diff != "" {
		t.Errorf("Size mismatch (-want +got):\n%s", diff)
	}

	// Verify categories for gender include _N/A_
	genderDim := resp.Dimension["gender"]
	// "Male" (M=77) comes before "_N/A_" (_=95) in ASCII
	wantCategories := []string{"Male", "_N/A_"}
	if diff := cmp.Diff(wantCategories, genderDim.Category.Index); diff != "" {
		t.Errorf("Gender categories mismatch (-want +got):\n%s", diff)
	}
}

func TestJSONStatFormatter_FacetAttributes(t *testing.T) {
	formatter := &JSONStatFormatter{}

	obs := []*datasource.SdmxObservation{
		{
			VariableMeasured: "Count_Person",
			Provenance:       "prov1",
			DatesAndValues: []*datasource.SdmxDateValue{
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
	}


	output, err := formatter.Format(obs)
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}

	var resp JSONStatResponse
	if err := json.Unmarshal([]byte(output), &resp); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	// Verify ID does NOT contain unit or measurementMethod
	for _, id := range resp.Id {
		if id == "unit" || id == "measurementMethod" {
			t.Errorf("Found facet attribute %q in dimensions, want it omitted", id)
		}
	}

	// Verify extensions.annotations
	annotations, ok := resp.Extension["annotations"].(map[string]interface{})
	if !ok {
		t.Fatalf("annotations missing or invalid in extension")
	}

	prov1, ok := annotations["prov1"].(map[string]interface{})
	if !ok {
		t.Fatalf("prov1 missing or invalid in annotations")
	}

	if prov1["unit"] != "Person" {
		t.Errorf("prov1.unit = %v, want 'Person'", prov1["unit"])
	}
	if prov1["measurementMethod"] != "Census" {
		t.Errorf("prov1.measurementMethod = %v, want 'Census'", prov1["measurementMethod"])
	}
}

