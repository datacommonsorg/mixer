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

package emulator

import (
	"context"
	"testing"

	"github.com/datacommonsorg/mixer/internal/agent"
	"github.com/google/go-cmp/cmp"
)

// v2MetadataSourceColumns mirrors the agent's sources table columns.
var v2MetadataSourceColumns = []string{
	"source_id", "import_name", "provenance_url", "unit", "scaling_factor", "measurement_method",
	"observation_period", "earliest_date", "latest_date", "observation_count", "entities_with_data",
}

func TestV2GetVariableMetadataEmulator(t *testing.T) {
	s := requireSuite(t)

	tests := []struct {
		name string
		req  *agent.V2GetVariableMetadataRequest
		want *agent.V2GetVariableMetadataResponse
	}{
		{
			name: "sources ranked by latest date with all-date spans",
			req: &agent.V2GetVariableMetadataRequest{
				VariableDcids: []string{"Count_TimeSeries"},
				EntityDcids:   []string{"country/USA"},
			},
			want: &agent.V2GetVariableMetadataResponse{
				Variables: []*agent.V2VariableMetadata{{
					Dcid:       "Count_TimeSeries",
					Properties: map[string]any{},
					Sources: &agent.Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"facet-b", "dc/base/HumanReadableStatVars", "", "Count", "", "Survey", "P1Y", "2021", "2023", 2, []string{"country/USA"}},
						{"facet-a", "dc/base/HumanReadableStatVars", "", "Count", "", "Census", "P1Y", "2020", "2022", 2, []string{"country/USA"}},
					}},
				}},
				Datasets: []*agent.V2DatasetMetadata{},
			},
		},
		{
			name: "explicit entities with coverage per entity, dataset properties, and an empty variable note",
			req: &agent.V2GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Farm", "Area_Farm", "Area_Farm_Irrigated"},
				EntityDcids:   []string{"geoId/01", "geoId/02"},
			},
			want: &agent.V2GetVariableMetadataResponse{
				Variables: []*agent.V2VariableMetadata{
					{
						Dcid:       "Count_Farm",
						Properties: map[string]any{},
						Sources: &agent.Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
							{"farm-census", "USDA Census of Agriculture", "https://www.nass.usda.gov/AgCensus/", "Count", "", "USDA_Census", "P5Y", "2017", "2022", 3, []string{"geoId/01", "geoId/02"}},
						}},
					},
					{
						Dcid:       "Area_Farm",
						Properties: map[string]any{},
						Sources: &agent.Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
							{"farm-area", "dc/base/OtherAg", "", "Acre", "", "Survey", "", "2022", "2022", 1, []string{"geoId/01"}},
						}},
					},
					{
						Dcid:       "Area_Farm_Irrigated",
						Properties: map[string]any{},
						Sources:    &agent.Table{Columns: v2MetadataSourceColumns},
						Note:       "no observations found for Area_Farm_Irrigated on the given entity_dcids.",
					},
				},
				// The seed has no nodes for the parent sources (dc/s/USDA, dc/s/Other) or dc/d/USDA_AgCensus, so V2Node
				// returns no source or isPartOf arcs, and dc/base/OtherAg has no dataset properties, so it is omitted.
				// Merging source properties is covered by the agent unit tests.
				Datasets: []*agent.V2DatasetMetadata{
					{
						ImportName: "USDA Census of Agriculture",
						Properties: map[string]any{"url": "https://www.nass.usda.gov/AgCensus/"},
					},
				},
			},
		},
		{
			name: "child mode checks sampled child entities and reports the parent without counts",
			req: &agent.V2GetVariableMetadataRequest{
				VariableDcids:       []string{"Count_Person"},
				EntityDcids:         []string{"northamerica"},
				ChildEntityTypeDcid: "Country",
			},
			want: &agent.V2GetVariableMetadataResponse{
				Variables: []*agent.V2VariableMetadata{{
					Dcid:       "Count_Person",
					Name:       "Total Population",
					Properties: map[string]any{},
					Sources: &agent.Table{Columns: v2MetadataSourceColumns, Rows: [][]any{
						{"facet", "dc/base/HumanReadableStatVars", "", "Count", "", "Census", "P1Y", "2024", "2024", nil, []string{"northamerica"}},
					}},
				}},
				Datasets: []*agent.V2DatasetMetadata{},
			},
		},
	}

	for _, accessPath := range sdmxContainedInPlaceAccessPaths {
		t.Run(accessPath.name, func(t *testing.T) {
			svc := newEmulatorAgentService(newSDMXAccessPathClient(t, s, accessPath.queryConfig))
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					got, err := svc.V2GetVariableMetadata(context.Background(), tc.req)
					if err != nil {
						t.Fatalf("V2GetVariableMetadata() unexpected error: %v", err)
					}
					if diff := cmp.Diff(tc.want, got); diff != "" {
						t.Errorf("V2GetVariableMetadata() mismatch (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}
