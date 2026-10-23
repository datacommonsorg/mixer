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

func point(date, value string) *mixerspanner.DateValue {
	return &mixerspanner.DateValue{Date: date, Value: value}
}

func lessObservation(a, b *mixerspanner.Observation) bool {
	if a.VariableMeasured != b.VariableMeasured {
		return a.VariableMeasured < b.VariableMeasured
	}
	if a.ObservationAbout != b.ObservationAbout {
		return a.ObservationAbout < b.ObservationAbout
	}
	return a.FacetId < b.FacetId
}

func TestGetObservations(t *testing.T) {
	s := requireSuite(t)
	farmCensus := func(entity string, points ...*mixerspanner.DateValue) *mixerspanner.Observation {
		return &mixerspanner.Observation{
			VariableMeasured:  "Count_Farm",
			ObservationAbout:  entity,
			FacetId:           "farm-census",
			Observations:      points,
			ImportName:        "USDA_Census",
			ProvenanceID:      "dc/base/USDA_Census",
			ObservationPeriod: "P5Y",
			MeasurementMethod: "USDA_Census",
			Unit:              "Count",
			ProvenanceURL:     "https://www.nass.usda.gov/AgCensus/",
		}
	}
	farmArea := func(points ...*mixerspanner.DateValue) *mixerspanner.Observation {
		return &mixerspanner.Observation{
			VariableMeasured:  "Area_Farm",
			ObservationAbout:  "geoId/01",
			FacetId:           "farm-area",
			Observations:      points,
			ProvenanceID:      "dc/base/OtherAg",
			MeasurementMethod: "Survey",
			Unit:              "Acre",
		}
	}

	tests := []struct {
		name      string
		variables []string
		entities  []string
		date      string
		want      []*mixerspanner.Observation
	}{
		{
			name:      "variables and entities, all dates",
			variables: []string{"Count_Farm", "Area_Farm"},
			entities:  []string{"geoId/01", "geoId/02"},
			want: []*mixerspanner.Observation{
				farmCensus("geoId/01", point("2017", "100"), point("2022", "90")),
				farmCensus("geoId/02", point("2022", "10")),
				farmArea(point("2022", "5000")),
			},
		},
		{
			name:      "variables and entities, latest date",
			variables: []string{"Count_Farm", "Area_Farm"},
			entities:  []string{"geoId/01", "geoId/02"},
			date:      "LATEST",
			want: []*mixerspanner.Observation{
				farmCensus("geoId/01", point("2022", "90")),
				farmCensus("geoId/02", point("2022", "10")),
				farmArea(point("2022", "5000")),
			},
		},
		{
			name:      "variables and entities, specific date drops series without it",
			variables: []string{"Count_Farm", "Area_Farm"},
			entities:  []string{"geoId/01", "geoId/02"},
			date:      "2017",
			want: []*mixerspanner.Observation{
				farmCensus("geoId/01", point("2017", "100")),
			},
		},
		{
			name:      "entity without data",
			variables: []string{"Count_Farm"},
			entities:  []string{"geoId/99"},
		},
		{
			name:     "entities only, all dates",
			entities: []string{"geoId/01"},
			want: []*mixerspanner.Observation{
				farmCensus("geoId/01", point("2017", "100"), point("2022", "90")),
				farmArea(point("2022", "5000")),
			},
		},
		{
			name:     "entities only, latest date",
			entities: []string{"geoId/01"},
			date:     "LATEST",
			want: []*mixerspanner.Observation{
				farmCensus("geoId/01", point("2022", "90")),
				farmArea(point("2022", "5000")),
			},
		},
		{
			name:     "entities only, specific date",
			entities: []string{"geoId/01"},
			date:     "2017",
			want: []*mixerspanner.Observation{
				farmCensus("geoId/01", point("2017", "100")),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := s.spannerClient.GetObservations(context.Background(), test.variables, test.entities, test.date)
			if err != nil {
				t.Fatalf("GetObservations() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got, cmpopts.SortSlices(lessObservation), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("GetObservations() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetObservationsContainedInPlaceAccessPaths(t *testing.T) {
	s := requireSuite(t)
	containedInPlace := &v2.ContainedInPlace{
		Ancestor:       "northamerica",
		ChildPlaceType: "Country",
	}
	variables := []string{"Count_Person", "Count_TimeSeries"}
	usaSeries := func(variable, facetID, measurementMethod string, points ...*mixerspanner.DateValue) *mixerspanner.Observation {
		return &mixerspanner.Observation{
			VariableMeasured:  variable,
			ObservationAbout:  "country/USA",
			FacetId:           facetID,
			Observations:      points,
			ProvenanceID:      "dc/base/HumanReadableStatVars",
			ObservationPeriod: "P1Y",
			MeasurementMethod: measurementMethod,
			Unit:              "Count",
		}
	}
	dates := []struct {
		date string
		want []*mixerspanner.Observation
	}{
		{
			date: "latest",
			want: []*mixerspanner.Observation{
				usaSeries("Count_Person", "facet", "Census", point("2024", "100")),
				usaSeries("Count_TimeSeries", "facet-a", "Census", point("2022", "12")),
				usaSeries("Count_TimeSeries", "facet-b", "Survey", point("2023", "23")),
			},
		},
		{
			date: "",
			want: []*mixerspanner.Observation{
				usaSeries("Count_Person", "facet", "Census", point("2024", "100")),
				usaSeries("Count_TimeSeries", "facet-a", "Census", point("2020", "10"), point("2022", "12")),
				usaSeries("Count_TimeSeries", "facet-b", "Survey", point("2021", "20"), point("2023", "23")),
			},
		},
		{
			date: "2022",
			want: []*mixerspanner.Observation{
				usaSeries("Count_TimeSeries", "facet-a", "Census", point("2022", "12")),
			},
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
			name: "variable seeks for non-preferred place type with materialized table",
			queryConfig: mixerspanner.QueryConfig{
				ContainedInPlacePreferTimeSeriesScanPlaceTypes: []string{"Place"},
				ContainedInPlaceEntityScanMinVariables:         2,
				UseMaterializedLinkedEdge:                      true,
			},
		},
		{
			name: "entity scan for preferred place type",
			queryConfig: mixerspanner.QueryConfig{
				ContainedInPlacePreferTimeSeriesScanPlaceTypes: []string{"Country"},
				ContainedInPlaceEntityScanMinVariables:         2,
			},
		},
		{
			name: "entity scan for preferred place type with materialized table",
			queryConfig: mixerspanner.QueryConfig{
				ContainedInPlacePreferTimeSeriesScanPlaceTypes: []string{"Country"},
				ContainedInPlaceEntityScanMinVariables:         2,
				UseMaterializedLinkedEdge:                      true,
			},
		},
	}

	// Query-builder goldens assert which index hint each configuration emits.
	// These emulator cases complement them by proving that every access path
	// returns the same observations for the latest date, all dates and a
	// specific date.
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := s.newSpannerClient(context.Background(), test.queryConfig)
			if err != nil {
				t.Fatalf("newSpannerClient() error = %v", err)
			}
			defer client.Close()

			for _, d := range dates {
				got, err := client.GetObservationsContainedInPlace(
					context.Background(), variables, containedInPlace, d.date)
				if err != nil {
					t.Fatalf("GetObservationsContainedInPlace(date=%q) error = %v", d.date, err)
				}

				if diff := cmp.Diff(d.want, got, cmpopts.SortSlices(lessObservation)); diff != "" {
					t.Errorf("date=%q: observations mismatch (-want +got):\n%s", d.date, diff)
				}
			}
		})
	}
}
