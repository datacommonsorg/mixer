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
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/agent"
	"github.com/datacommonsorg/mixer/internal/featureflags"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newEmulatorAgentService(client mixerspanner.SpannerClient) *agent.Service {
	spannerSource := mixerspanner.NewSpannerDataSource(client, nil)
	sources := datasources.NewDataSources([]datasource.DataSource{spannerSource}, nil)
	mixerSrv := server.NewMixerServer(
		&store.Store{},
		&resource.Metadata{},
		dispatcher.NewDispatcher(nil, sources),
		&featureflags.Flags{EnableSDMXDataApi: true},
		&server.MixerServerOptions{UseSpannerGraph: true},
	)
	return agent.NewService(mixerSrv, agent.NewCache(mixerSrv), nil)
}

func TestV2GetObservationsEmulator(t *testing.T) {
	s := requireSuite(t)

	tests := []struct {
		name string
		req  *agent.V2GetObservationsRequest
		want *agent.V2GetObservationsResponse
	}{
		{
			name: "multi-variable explicit entities with provenance url, provenance dcid fallback, and empty variable note",
			req: &agent.V2GetObservationsRequest{
				VariableDcids: []string{"Count_Farm", "Area_Farm", "Area_Farm_Irrigated"},
				EntityDcids:   []string{"geoId/01", "geoId/02"},
			},
			want: &agent.V2GetObservationsResponse{
				EntityMetadata: &agent.Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"geoId/01", ""},
						{"geoId/02", ""},
					},
				},
				Observations: []*agent.V2VariableObservations{
					{
						Variable: &agent.ObservationNode{Dcid: "Count_Farm"},
						Data: &agent.Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows: [][]any{
								{"geoId/01", "2022", 90.0},
								{"geoId/02", "2022", 10.0},
							},
						},
						SourceMetadata: &agent.V2ObservationSourceMeta{
							SourceID:          "farm-census",
							ImportName:        "USDA Census of Agriculture",
							ProvenanceURL:     "https://www.nass.usda.gov/AgCensus/",
							Unit:              "Count",
							MeasurementMethod: "USDA_Census",
							ObservationPeriod: "P5Y",
						},
					},
					{
						Variable: &agent.ObservationNode{Dcid: "Area_Farm"},
						Data: &agent.Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows:    [][]any{{"geoId/01", "2022", 5000.0}},
						},
						SourceMetadata: &agent.V2ObservationSourceMeta{
							SourceID:          "farm-area",
							ImportName:        "dc/base/OtherAg",
							Unit:              "Acre",
							MeasurementMethod: "Survey",
						},
					},
					{
						Variable: &agent.ObservationNode{Dcid: "Area_Farm_Irrigated"},
						Data:     &agent.Table{Columns: []string{"observationAbout", "date", "value"}},
						Note:     "no observations found for Area_Farm_Irrigated on the given entity_dcids.",
					},
				},
			},
		},
		{
			name: "child mode with 2 variables, LATEST pushdown, facet ranking, and alternative_sources",
			req: &agent.V2GetObservationsRequest{
				VariableDcids:       []string{"Count_Person", "Count_TimeSeries"},
				EntityDcids:         []string{"northamerica"},
				ChildEntityTypeDcid: "Country",
			},
			want: &agent.V2GetObservationsResponse{
				EntityMetadata: &agent.Table{
					Columns: []string{"dcid", "name"},
					Rows:    [][]any{{"country/USA", "United States"}},
				},
				Observations: []*agent.V2VariableObservations{
					{
						Variable: &agent.ObservationNode{Dcid: "Count_Person", Name: "Total Population"},
						Data: &agent.Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows:    [][]any{{"country/USA", "2024", 100.0}},
						},
						SourceMetadata: &agent.V2ObservationSourceMeta{
							SourceID:          "facet",
							ImportName:        "dc/base/HumanReadableStatVars",
							Unit:              "Count",
							MeasurementMethod: "Census",
							ObservationPeriod: "P1Y",
						},
					},
					{
						Variable: &agent.ObservationNode{Dcid: "Count_TimeSeries"},
						Data: &agent.Table{
							Columns: []string{"observationAbout", "date", "value"},
							Rows:    [][]any{{"country/USA", "2023", 23.0}},
						},
						SourceMetadata: &agent.V2ObservationSourceMeta{
							SourceID:          "facet-b",
							ImportName:        "dc/base/HumanReadableStatVars",
							Unit:              "Count",
							MeasurementMethod: "Survey",
							ObservationPeriod: "P1Y",
						},
						AlternativeSources: []*agent.V2AlternativeSource{
							{
								SourceMetadata: &agent.V2ObservationSourceMeta{
									SourceID:          "facet-a",
									ImportName:        "dc/base/HumanReadableStatVars",
									Unit:              "Count",
									MeasurementMethod: "Census",
									ObservationPeriod: "P1Y",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "three-entity variable with unconstrained transportMode counterpart in entity_metadata",
			req: &agent.V2GetObservationsRequest{
				VariableDcids:      []string{"Count_MigrationByTransportMode"},
				EntityDcids:        []string{"country/USA"},
				EntityProperty:     "sourceCountry",
				AdditionalEntities: map[string][]string{"destinationCountry": {"country/CAN"}},
			},
			want: &agent.V2GetObservationsResponse{
				EntityMetadata: &agent.Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"Air", "Air Transport"},
						{"Sea", "Sea Transport"},
						{"country/CAN", "Canada"},
						{"country/USA", "United States"},
					},
				},
				Observations: []*agent.V2VariableObservations{
					{
						Variable: &agent.ObservationNode{Dcid: "Count_MigrationByTransportMode"},
						Data: &agent.Table{
							Columns: []string{"destinationCountry", "sourceCountry", "transportMode", "date", "value"},
							Rows: [][]any{
								{"country/CAN", "country/USA", "Air", "2024", 7.0},
								{"country/CAN", "country/USA", "Sea", "2024", 3.0},
							},
						},
						SourceMetadata: &agent.V2ObservationSourceMeta{
							SourceID:          "facet",
							ImportName:        "dc/base/HumanReadableStatVars",
							Unit:              "Count",
							MeasurementMethod: "Census",
							ObservationPeriod: "P1Y",
						},
					},
				},
			},
		},
		{
			name: "multi-variable compatible multi-entity batch",
			req: &agent.V2GetObservationsRequest{
				VariableDcids:  []string{"Count_Migration", "Count_Refugee"},
				EntityDcids:    []string{"country/CAN", "country/MEX"},
				EntityProperty: "destinationCountry",
			},
			want: &agent.V2GetObservationsResponse{
				EntityMetadata: &agent.Table{
					Columns: []string{"dcid", "name"},
					Rows: [][]any{
						{"country/CAN", "Canada"},
						{"country/IND", ""},
						{"country/MEX", "Mexico"},
						{"country/USA", "United States"},
					},
				},
				Observations: []*agent.V2VariableObservations{
					{
						Variable: &agent.ObservationNode{Dcid: "Count_Migration"},
						Data: &agent.Table{
							Columns: []string{"destinationCountry", "sourceCountry", "date", "value"},
							Rows:    [][]any{{"country/CAN", "country/USA", "2024", 10.0}},
						},
						SourceMetadata: &agent.V2ObservationSourceMeta{
							SourceID:          "facet",
							ImportName:        "dc/base/HumanReadableStatVars",
							Unit:              "Count",
							MeasurementMethod: "Census",
							ObservationPeriod: "P1Y",
						},
					},
					{
						Variable: &agent.ObservationNode{Dcid: "Count_Refugee"},
						Data: &agent.Table{
							Columns: []string{"destinationCountry", "sourceCountry", "date", "value"},
							Rows:    [][]any{{"country/MEX", "country/IND", "2023", 20.0}},
						},
						SourceMetadata: &agent.V2ObservationSourceMeta{
							SourceID:          "alternate-facet",
							ImportName:        "dc/base/Other",
							Unit:              "Percent",
							MeasurementMethod: "Survey",
							ObservationPeriod: "P1M",
						},
					},
				},
			},
		},
	}

	errCases := []struct {
		name           string
		req            *agent.V2GetObservationsRequest
		wantSubstrings []string
	}{
		{
			name: "incompatible observation_properties across variables",
			req: &agent.V2GetObservationsRequest{
				VariableDcids: []string{"Count_Person", "Count_Migration"},
				EntityDcids:   []string{"country/USA"},
			},
			wantSubstrings: []string{
				"different observation_properties",
				`Count_Person: ["observationAbout"]`,
				`Count_Migration: ["destinationCountry" "sourceCountry"]`,
			},
		},
		{
			name: "wrong default entity_property on multi-entity variable",
			req: &agent.V2GetObservationsRequest{
				VariableDcids: []string{"Count_Migration"},
				EntityDcids:   []string{"country/USA"},
			},
			wantSubstrings: []string{
				`uses observation_properties ["destinationCountry" "sourceCountry"]`,
				`not ["observationAbout"]`,
			},
		},
	}

	for _, accessPath := range sdmxContainedInPlaceAccessPaths {
		t.Run(accessPath.name, func(t *testing.T) {
			client := newSDMXAccessPathClient(t, s, accessPath.queryConfig)
			svc := newEmulatorAgentService(client)

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					got, err := svc.V2GetObservations(context.Background(), tc.req)
					if err != nil {
						t.Fatalf("V2GetObservations() unexpected error: %v", err)
					}
					if diff := cmp.Diff(tc.want, got); diff != "" {
						t.Errorf("V2GetObservations() mismatch (-want +got):\n%s", diff)
					}
				})
			}

			for _, tc := range errCases {
				t.Run(tc.name, func(t *testing.T) {
					_, err := svc.V2GetObservations(context.Background(), tc.req)
					st := status.Convert(err)
					if st.Code() != codes.InvalidArgument {
						t.Fatalf("V2GetObservations() code = %v (%q), want InvalidArgument", st.Code(), st.Message())
					}
					for _, sub := range tc.wantSubstrings {
						if !strings.Contains(st.Message(), sub) {
							t.Errorf("V2GetObservations() message = %q, missing %q", st.Message(), sub)
						}
					}
				})
			}
		})
	}
}
