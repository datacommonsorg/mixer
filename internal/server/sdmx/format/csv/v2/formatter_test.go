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

package csvv2

import (
	"encoding/csv"
	"strings"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/google/go-cmp/cmp"
)

func TestCSVFormatter_HeaderOnly(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DC:DF_OBS(1.0.0)"}

	got, err := formatter.Format(testSdmxResult([]string{datacommons.ComponentObservationAbout}, nil))
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor,facetId\r\n"
	if got != want {
		t.Errorf("Format() = %q, want %q", got, want)
	}
}

func TestCSVFormatter_Rows(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DC:DF_OBS(1.0.0)"}

	got, err := formatter.Format(testSdmxResult([]string{datacommons.ComponentObservationAbout}, []*sdmxpb.SdmxTimeSeries{
		{
			Dimensions: map[string]string{
				"variableMeasured":  "Count_Person",
				"observationAbout":  "country/USA",
				"unit":              "Person",
				"measurementMethod": "Census",
				"observationPeriod": "P1Y",
				"provenance":        "dc/base",
			},
			Attributes: map[string]string{
				"scalingFactor": "0",
				"facetId":       "stored-facet-id",
			},
			Points: []*sdmxpb.SdmxDataPoint{
				{TimePeriod: "2020", ObservationValue: "1.50"},
				{TimePeriod: "2021", ObservationValue: "2"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := [][]string{
		testCSVHeader([]string{datacommons.ComponentObservationAbout}),
		{"dataflow", "DC:DF_OBS(1.0.0)", "I", "Count_Person", "country/USA", "Person", "Census", "P1Y", "dc/base", "2020", "1.50", "0", "stored-facet-id"},
		{"dataflow", "DC:DF_OBS(1.0.0)", "I", "Count_Person", "country/USA", "Person", "Census", "P1Y", "dc/base", "2021", "2", "0", "stored-facet-id"},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVFormatter_FacetID(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DC:DF_OBS(1.0.0)"}
	result := testSdmxResult([]string{datacommons.ComponentObservationAbout}, []*sdmxpb.SdmxTimeSeries{
		{
			Dimensions: map[string]string{
				datacommons.ComponentVariableMeasured: "Count_Person",
				datacommons.ComponentObservationAbout: "country/USA",
			},
			Attributes: map[string]string{
				datacommons.ComponentScalingFactor: "0",
				datacommons.ComponentFacetID:       "stored-facet-id",
			},
			Points: []*sdmxpb.SdmxDataPoint{
				{TimePeriod: "2020", ObservationValue: "1"},
			},
		},
	})
	got, err := formatter.Format(result)
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := [][]string{
		testCSVHeader([]string{datacommons.ComponentObservationAbout}),
		{"dataflow", "DC:DF_OBS(1.0.0)", "I", "Count_Person", "country/USA", datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, "2020", "1", "0", "stored-facet-id"},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVFormatter_MultiEntityHeader(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DC:DF_OBS(1.0.0)"}

	got, err := formatter.Format(testSdmxResult([]string{"destinationCountry", "sourceCountry"}, []*sdmxpb.SdmxTimeSeries{
		{
			Dimensions: map[string]string{
				datacommons.ComponentVariableMeasured: "Count_Person",
				"destinationCountry":                  "country/CAN",
				"sourceCountry":                       "country/USA",
				datacommons.ComponentProvenance:       "dc/base",
			},
			Points: []*sdmxpb.SdmxDataPoint{
				{TimePeriod: "2020", ObservationValue: "1"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := [][]string{
		testCSVHeader([]string{"destinationCountry", "sourceCountry"}),
		{"dataflow", "DC:DF_OBS(1.0.0)", "I", "Count_Person", "country/CAN", "country/USA", datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, "dc/base", "2020", "1", "", ""},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
	}
	if strings.Contains(strings.Split(got, "\r\n")[0], datacommons.ComponentObservationAbout) {
		t.Fatalf("multi-entity header contains %q: %q", datacommons.ComponentObservationAbout, got)
	}
}

func TestCSVFormatter_SkipsNilObservations(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DC:DF_OBS(1.0.0)"}

	got, err := formatter.Format(testSdmxResult([]string{datacommons.ComponentObservationAbout}, []*sdmxpb.SdmxTimeSeries{
		nil,
		{
			Dimensions: map[string]string{
				"variableMeasured": "Count_Person",
				"observationAbout": "country/USA",
			},
		},
	}))
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := [][]string{
		testCSVHeader([]string{datacommons.ComponentObservationAbout}),
		{"dataflow", "DC:DF_OBS(1.0.0)", "I", "Count_Person", "country/USA", datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, "", "", ""},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVFormatter_MissingFieldsAndEscaping(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DC:DF_OBS(1.0.0)"}

	got, err := formatter.Format(testSdmxResult([]string{datacommons.ComponentObservationAbout}, []*sdmxpb.SdmxTimeSeries{
		{
			Dimensions: map[string]string{
				"variableMeasured": "Count,\"Person\"",
			},
			Points: []*sdmxpb.SdmxDataPoint{
				{TimePeriod: "2020", ObservationValue: "foo,bar"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}
	if !strings.Contains(got, "\"Count,\"\"Person\"\"\"") {
		t.Fatalf("Format() did not CSV-escape quoted value: %q", got)
	}

	want := [][]string{
		testCSVHeader([]string{datacommons.ComponentObservationAbout}),
		{"dataflow", "DC:DF_OBS(1.0.0)", "I", "Count,\"Person\"", datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, "2020", "foo,bar", "", ""},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
	}
}

func testSdmxResult(observationProperties []string, series []*sdmxpb.SdmxTimeSeries) *sdmxpb.SdmxDataResult {
	components := datacommons.DataComponentsForObservationProperties(observationProperties)
	result := &sdmxpb.SdmxDataResult{
		Shape: &sdmxpb.SdmxDataShape{
			Components: make([]*sdmxpb.SdmxComponent, 0, len(components)),
		},
		Series: series,
	}
	for _, component := range components {
		result.Shape.Components = append(result.Shape.Components, &sdmxpb.SdmxComponent{
			Id:   component.ID,
			Kind: testProtoComponentKind(component.Kind),
		})
	}
	return result
}

func testCSVHeader(observationProperties []string) []string {
	return dataCSVHeader(datacommons.DataComponentsForObservationProperties(observationProperties))
}

func testProtoComponentKind(kind datacommons.ComponentKind) sdmxpb.SdmxComponentKind {
	switch kind {
	case datacommons.ComponentKindDimension:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION
	case datacommons.ComponentKindMeasure:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_MEASURE
	case datacommons.ComponentKindAttribute:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_ATTRIBUTE
	default:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_UNSPECIFIED
	}
}

func parseCSV(t *testing.T, input string) [][]string {
	t.Helper()
	reader := csv.NewReader(strings.NewReader(input))
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	return rows
}
