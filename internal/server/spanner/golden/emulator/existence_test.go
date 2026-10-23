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
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// sortRows orders existence rows so tests don't depend on UNION ALL order.
func sortRows(rows [][]string) {
	slices.SortFunc(rows, slices.Compare)
}

func TestCheckVariableExistence(t *testing.T) {
	s := requireSuite(t)
	tests := []struct {
		name      string
		variables []string
		entities  []string
		want      [][]string
	}{
		{
			name:      "variables and entities",
			variables: []string{"Area_Farm", "Area_Farm_Irrigated", "Count_Farm", "Count_FarmShipment"},
			entities:  []string{"geoId/01", "geoId/02"},
			want: [][]string{
				{"Area_Farm", "geoId/01"},
				{"Count_Farm", "geoId/01"},
				{"Count_Farm", "geoId/02"},
				// geoId/01 is the second entity of the shipment series.
				{"Count_FarmShipment", "geoId/01"},
			},
		},
		{
			name:      "variables only",
			variables: []string{"Count_FarmShipment"},
			want: [][]string{
				{"Count_FarmShipment", "geoId/01"},
				{"Count_FarmShipment", "geoId/04"},
				{"Count_FarmShipment", "geoId/05"},
			},
		},
		{
			name:     "entities only",
			entities: []string{"geoId/01"},
			want: [][]string{
				{"Area_Farm", "geoId/01"},
				{"Count_Farm", "geoId/01"},
				{"Count_FarmShipment", "geoId/01"},
			},
		},
		{
			name:      "second and third entity slots",
			variables: []string{"Count_MigrationByObservationAbout"},
			entities:  []string{"country/MEX", "country/USA"},
			want: [][]string{
				{"Count_MigrationByObservationAbout", "country/MEX"},
				{"Count_MigrationByObservationAbout", "country/USA"},
			},
		},
		{
			name:      "variables only, every entity slot",
			variables: []string{"Count_MigrationByObservationAbout"},
			want: [][]string{
				{"Count_MigrationByObservationAbout", "country/CAN"},
				{"Count_MigrationByObservationAbout", "country/MEX"},
				{"Count_MigrationByObservationAbout", "country/USA"},
			},
		},
		{
			name:     "entities only, third entity slot",
			entities: []string{"Sea"},
			want:     [][]string{{"Count_MigrationByTransportMode", "Sea"}},
		},
		{
			name:      "no data",
			variables: []string{"Area_Farm_Irrigated"},
			entities:  []string{"geoId/01"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := s.spannerClient.CheckVariableExistence(context.Background(), test.variables, test.entities)
			if err != nil {
				t.Fatalf("CheckVariableExistence() error = %v", err)
			}
			sortRows(got)
			if diff := cmp.Diff(test.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("CheckVariableExistence() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCheckVariableGroupPlaceExistence(t *testing.T) {
	s := requireSuite(t)
	tests := []struct {
		name           string
		variableGroups []string
		entities       []string
		predicate      string
		want           [][]string
	}{
		{
			name:           "stat var groups",
			variableGroups: []string{"dc/g/Farm", "dc/g/Farm_Area"},
			entities:       []string{"geoId/01", "geoId/02"},
			predicate:      "linkedMemberOf",
			want: [][]string{
				{"dc/g/Farm", "geoId/01"},
				{"dc/g/Farm", "geoId/02"},
				{"dc/g/Farm_Area", "geoId/01"},
			},
		},
		{
			name:           "stat var group matched through second entity slot",
			variableGroups: []string{"dc/g/Farm"},
			entities:       []string{"geoId/05"},
			predicate:      "linkedMemberOf",
			want:           [][]string{{"dc/g/Farm", "geoId/05"}},
		},
		{
			name:           "topics",
			variableGroups: []string{"dc/topic/Agriculture", "dc/topic/Empty"},
			entities:       []string{"geoId/02"},
			predicate:      "linkedMember",
			want:           [][]string{{"dc/topic/Agriculture", "geoId/02"}},
		},
		{
			name:           "no data",
			variableGroups: []string{"dc/g/Farm_Area"},
			entities:       []string{"geoId/02"},
			predicate:      "linkedMemberOf",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := s.spannerClient.CheckVariableGroupPlaceExistence(context.Background(), test.variableGroups, test.entities, test.predicate)
			if err != nil {
				t.Fatalf("CheckVariableGroupPlaceExistence() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("CheckVariableGroupPlaceExistence() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCheckVariableSourceExistence(t *testing.T) {
	s := requireSuite(t)
	tests := []struct {
		name      string
		variables []string
		sources   []string
		predicate string
		want      [][]string
	}{
		{
			name:      "stat vars",
			variables: []string{"Area_Farm", "Count_Farm", "Count_FarmShipment"},
			sources:   []string{"dc/s/USDA"},
			want: [][]string{
				{"Count_Farm", "dc/s/USDA"},
				{"Count_FarmShipment", "dc/s/USDA"},
			},
		},
		{
			name:      "stat var groups",
			variables: []string{"dc/g/Farm", "dc/g/Farm_Area"},
			sources:   []string{"dc/d/USDA_AgCensus", "dc/s/Other"},
			predicate: "linkedMemberOf",
			want: [][]string{
				{"dc/g/Farm", "dc/d/USDA_AgCensus"},
				{"dc/g/Farm", "dc/s/Other"},
				{"dc/g/Farm_Area", "dc/s/Other"},
			},
		},
		{
			name:      "topics",
			variables: []string{"dc/topic/Agriculture"},
			sources:   []string{"dc/s/USDA"},
			predicate: "linkedMember",
			want:      [][]string{{"dc/topic/Agriculture", "dc/s/USDA"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := s.spannerClient.CheckVariableSourceExistence(context.Background(), test.variables, test.sources, test.predicate)
			if err != nil {
				t.Fatalf("CheckVariableSourceExistence() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("CheckVariableSourceExistence() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
