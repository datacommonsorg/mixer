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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/google/go-cmp/cmp"
)

func TestCSVFormatter_HeaderOnly(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DATACOMMONS:DF_OBSERVATIONS(1.0.0)"}

	got, err := formatter.Format(nil)
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := "STRUCTURE,STRUCTURE_ID,ACTION,variableMeasured,observationAbout,unit,measurementMethod,observationPeriod,provenance,TIME_PERIOD,OBS_VALUE,scalingFactor\r\n"
	if got != want {
		t.Errorf("Format() = %q, want %q", got, want)
	}
}

func TestCSVFormatter_Rows(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DATACOMMONS:DF_OBSERVATIONS(1.0.0)"}

	got, err := formatter.Format([]*pb.SdmxObservation{
		{
			VariableMeasured: "Count_Person",
			Provenance:       "dc/base",
			DatesAndValues: []*pb.SdmxDateValue{
				{Date: "2020", Value: "1.50"},
				{Date: "2021", Value: "2"},
			},
			Dimensions: map[string]string{
				"observationAbout": "country/USA",
			},
			Attributes: map[string]string{
				"unit":              "Person",
				"measurementMethod": "Census",
				"observationPeriod": "P1Y",
				"scalingFactor":     "0",
			},
		},
	})
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := [][]string{
		dataCSVHeader(),
		{"dataflow", "DATACOMMONS:DF_OBSERVATIONS(1.0.0)", "I", "Count_Person", "country/USA", "Person", "Census", "P1Y", "dc/base", "2020", "1.50", "0"},
		{"dataflow", "DATACOMMONS:DF_OBSERVATIONS(1.0.0)", "I", "Count_Person", "country/USA", "Person", "Census", "P1Y", "dc/base", "2021", "2", "0"},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVFormatter_SkipsNilObservations(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DATACOMMONS:DF_OBSERVATIONS(1.0.0)"}

	got, err := formatter.Format([]*pb.SdmxObservation{
		nil,
		{
			VariableMeasured: "Count_Person",
			Dimensions: map[string]string{
				"observationAbout": "country/USA",
			},
		},
	})
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}

	want := [][]string{
		dataCSVHeader(),
		{"dataflow", "DATACOMMONS:DF_OBSERVATIONS(1.0.0)", "I", "Count_Person", "country/USA", datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, "", ""},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVFormatter_MissingFieldsAndEscaping(t *testing.T) {
	formatter := &CSVFormatter{StructureID: "DATACOMMONS:DF_OBSERVATIONS(1.0.0)"}

	got, err := formatter.Format([]*pb.SdmxObservation{
		{
			VariableMeasured: "Count,\"Person\"",
			DatesAndValues: []*pb.SdmxDateValue{
				{Date: "2020", Value: "foo,bar"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Format() error = %v", err)
	}
	if !strings.Contains(got, "\"Count,\"\"Person\"\"\"") {
		t.Fatalf("Format() did not CSV-escape quoted value: %q", got)
	}

	want := [][]string{
		dataCSVHeader(),
		{"dataflow", "DATACOMMONS:DF_OBSERVATIONS(1.0.0)", "I", "Count,\"Person\"", datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, datacommons.FallbackNotAvailable, "2020", "foo,bar", ""},
	}
	if diff := cmp.Diff(want, parseCSV(t, got)); diff != "" {
		t.Errorf("Format() mismatch (-want +got):\n%s", diff)
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
