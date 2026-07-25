// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emulator

import (
	"context"
	"testing"

	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestGetObservationsContainedInPlaceAccessPaths(t *testing.T) {
	s := requireSuite(t)
	containedInPlace := &v2.ContainedInPlace{
		Ancestor:       "northamerica",
		ChildPlaceType: "Country",
	}
	variables := []string{"Count_Person", "Count_TimeSeries"}
	want := []*mixerspanner.Observation{
		{
			VariableMeasured:  "Count_Person",
			ObservationAbout:  "country/USA",
			FacetId:           "facet",
			Observations:      mixerspanner.TimeSeries{{Date: "2024", Value: "100"}},
			ProvenanceID:      "dc/base/HumanReadableStatVars",
			ObservationPeriod: "P1Y",
			MeasurementMethod: "Census",
			Unit:              "Count",
		},
		{
			VariableMeasured:  "Count_TimeSeries",
			ObservationAbout:  "country/USA",
			FacetId:           "facet-a",
			Observations:      mixerspanner.TimeSeries{{Date: "2022", Value: "12"}},
			ProvenanceID:      "dc/base/HumanReadableStatVars",
			ObservationPeriod: "P1Y",
			MeasurementMethod: "Census",
			Unit:              "Count",
		},
		{
			VariableMeasured:  "Count_TimeSeries",
			ObservationAbout:  "country/USA",
			FacetId:           "facet-b",
			Observations:      mixerspanner.TimeSeries{{Date: "2023", Value: "23"}},
			ProvenanceID:      "dc/base/HumanReadableStatVars",
			ObservationPeriod: "P1Y",
			MeasurementMethod: "Survey",
			Unit:              "Count",
		},
	}

	tests := []struct {
		name        string
		queryConfig mixerspanner.QueryConfig
	}{
		{
			name: "variable seeks for non-preferred place type",
			queryConfig: mixerspanner.QueryConfig{
				ContainedInPlacePreferTimeSeriesScanPlaceTypes: []string{"Place"},
				ContainedInPlaceEntityScanMinVariables:         2,
			},
		},
		{
			name: "entity scan for preferred place type",
			queryConfig: mixerspanner.QueryConfig{
				ContainedInPlacePreferTimeSeriesScanPlaceTypes: []string{"Country"},
				ContainedInPlaceEntityScanMinVariables:         2,
			},
		},
	}

	// Query-builder goldens assert which index hint each configuration emits.
	// These emulator cases complement them by proving that both access paths
	// execute against the same data and preserve the latest-observation result.
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := s.newSpannerClient(context.Background(), test.queryConfig)
			if err != nil {
				t.Fatalf("newSpannerClient() error = %v", err)
			}
			defer client.Close()

			got, err := client.GetObservationsContainedInPlace(
				context.Background(), variables, containedInPlace, "latest")
			if err != nil {
				t.Fatalf("GetObservationsContainedInPlace() error = %v", err)
			}

			less := func(a, b *mixerspanner.Observation) bool {
				if a.VariableMeasured != b.VariableMeasured {
					return a.VariableMeasured < b.VariableMeasured
				}
				if a.ObservationAbout != b.ObservationAbout {
					return a.ObservationAbout < b.ObservationAbout
				}
				return a.FacetId < b.FacetId
			}
			if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
				t.Errorf("observations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
