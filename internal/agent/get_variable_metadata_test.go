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

package agent

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockVMMixerServer struct {
	Mixer
	nodeData map[string]*pbv2.LinkedGraph
	bulkData map[string]*pb.StatVarSummary
	obsData  map[string]*pb.Facet
	obsErr   error // optional error to simulate critical path failure
}

func (m *mockVMMixerServer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	resp := &pbv2.NodeResponse{Data: make(map[string]*pbv2.LinkedGraph)}
	for _, n := range in.GetNodes() {
		if g, ok := m.nodeData[n]; ok {
			resp.Data[n] = g
		}
	}
	return resp, nil
}

func (m *mockVMMixerServer) V2BulkVariableInfo(ctx context.Context, in *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error) {
	resp := &pbv1.BulkVariableInfoResponse{}
	for _, n := range in.GetNodes() {
		if summary, ok := m.bulkData[n]; ok {
			resp.Data = append(resp.Data, &pbv1.VariableInfoResponse{
				Node: n,
				Info: summary,
			})
		}
	}
	return resp, nil
}

func (m *mockVMMixerServer) V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	if m.obsErr != nil {
		return nil, m.obsErr
	}
	resp := &pbv2.ObservationResponse{
		Facets:     make(map[string]*pb.Facet),
		ByVariable: make(map[string]*pbv2.VariableObservation),
	}
	for k, v := range m.obsData {
		resp.Facets[k] = v
	}
	for _, vDcid := range in.GetVariable().GetDcids() {
		var fList []*pbv2.FacetObservation
		for k := range m.obsData {
			fList = append(fList, &pbv2.FacetObservation{
				FacetId:      k,
				ObsCount:     54,
				EarliestDate: "2010",
				LatestDate:   "2020",
			})
		}
		resp.ByVariable[vDcid] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{
				"geoId/06": {OrderedFacets: fList},
			},
		}
	}
	return resp, nil
}

func TestGetVariableMetadata(t *testing.T) {
	for _, tc := range []struct {
		desc              string
		req               *pbv2.GetVariableMetadataRequest
		nodeData          map[string]*pbv2.LinkedGraph
		bulkData          map[string]*pb.StatVarSummary
		obsData           map[string]*pb.Facet
		obsErr            error // optional error to simulate critical path failure
		wantErr           bool
		wantCode          codes.Code
		wantVar           string
		wantPropName      string
		wantProvDcid      string
		wantProvUrl       string
		wantGranularities []string
		omitVar           string // variable that should be omitted from the final response due to mock fetch error
	}{
		{
			desc: "Successfully retrieve metadata across variable, provenance, and observation facets",
			req: &pbv2.GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"geoId/06"},
			},
			nodeData: map[string]*pbv2.LinkedGraph{
				"Count_Person": {
					Arcs: map[string]*pbv2.Nodes{
						"name": {Nodes: []*pb.EntityInfo{{Value: "Population"}}},
					},
				},
				"dc/base/USCensus_PEP": {
					Arcs: map[string]*pbv2.Nodes{
						"url": {Nodes: []*pb.EntityInfo{{Value: "https://census.gov"}}},
					},
				},
			},
			bulkData: map[string]*pb.StatVarSummary{
				"Count_Person": {
					ProvenanceSummary: map[string]*pb.StatVarSummary_ProvenanceSummary{
						"dc/base/USCensus_PEP": {
							ImportName: "USCensus_PEP",
							SeriesSummary: []*pb.StatVarSummary_SeriesSummary{
								{
									PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
										"Country": {},
										"State":   {},
										"County":  {},
									},
								},
							},
						},
					},
				},
			},
			obsData: map[string]*pb.Facet{
				"facet1": {ImportName: "USCensus_PEP", ProvenanceId: "dc/base/USCensus_PEP"},
			},
			wantVar:           "Count_Person",
			wantPropName:      "Population",
			wantProvDcid:      "dc/base/USCensus_PEP",
			wantProvUrl:       "https://census.gov",
			wantGranularities: []string{"Country", "County", "State"}, // sorted alphabetically
		},
		{
			desc: "Validation Failure: Missing variable dcids",
			req: &pbv2.GetVariableMetadataRequest{
				EntityDcids: []string{"geoId/06"},
			},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			desc: "Validation Failure: Missing entity dcids",
			req: &pbv2.GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person"},
			},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			desc: "Validation Failure: variable dcids exceed limit of 10",
			req: &pbv2.GetVariableMetadataRequest{
				VariableDcids: []string{"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11"},
				EntityDcids:   []string{"geoId/06"},
			},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			desc: "Validation Failure: entity dcids exceed limit of 10",
			req: &pbv2.GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "e10", "e11"},
			},
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			desc: "Graceful Degradation: Fail to fetch one variable, but successfully return other",
			req: &pbv2.GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person", "Median_Age_Person"}, // Median_Age_Person will fail graph fetch
				EntityDcids:   []string{"geoId/06"},
			},
			nodeData: map[string]*pbv2.LinkedGraph{
				"Count_Person": {
					Arcs: map[string]*pbv2.Nodes{
						"name": {Nodes: []*pb.EntityInfo{{Value: "Population"}}},
					},
				},
				"dc/base/USCensus_PEP": {
					Arcs: map[string]*pbv2.Nodes{
						"url": {Nodes: []*pb.EntityInfo{{Value: "https://census.gov"}}},
					},
				},
			},
			bulkData: map[string]*pb.StatVarSummary{
				"Count_Person": {
					ProvenanceSummary: map[string]*pb.StatVarSummary_ProvenanceSummary{
						"dc/base/USCensus_PEP": {ImportName: "USCensus_PEP"},
					},
				},
			},
			obsData: map[string]*pb.Facet{
				"facet1": {ImportName: "USCensus_PEP", ProvenanceId: "dc/base/USCensus_PEP"},
			},
			wantVar:      "Count_Person",
			wantPropName: "Population",
			wantProvDcid: "dc/base/USCensus_PEP",
			wantProvUrl:  "https://census.gov",
			omitVar:      "Median_Age_Person",
		},
		{
			desc: "Critical Path Failure: observations query fails",
			req: &pbv2.GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person"},
				EntityDcids:   []string{"geoId/06"},
			},
			obsErr:   status.Error(codes.Internal, "Spanner connection lost"),
			wantErr:  true,
			wantCode: codes.Internal,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mock := &mockVMMixerServer{
				nodeData: tc.nodeData,
				bulkData: tc.bulkData,
				obsData:  tc.obsData,
				obsErr:   tc.obsErr,
			}
			svc := NewService(mock, NewCache(mock))

			got, err := svc.GetVariableMetadata(context.Background(), tc.req)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
				stat, ok := status.FromError(err)
				if !ok || stat.Code() != tc.wantCode {
					t.Fatalf("Expected error code %v, got %v: %v", tc.wantCode, stat.Code(), err)
				}
				return
			}

			if err != nil {
				t.Fatalf("GetVariableMetadata failed: %v", err)
			}

			if got == nil || got.GetStatus() != StatusSuccess {
				t.Fatalf("GetVariableMetadata returned non-success response: %v", got)
			}

			// 1. Assert skipped variables (graceful degradation)
			if tc.omitVar != "" {
				if _, exists := got.GetVariables()[tc.omitVar]; exists {
					t.Errorf("Expected variable %s to be omitted due to fetch failure, but it was present", tc.omitVar)
				}
			}

			vMeta, ok := got.GetVariables()[tc.wantVar]
			if !ok {
				t.Fatalf("Missing expected variable metadata for %s", tc.wantVar)
			}

			// 2. Assert root headers are correctly mapped
			if vMeta.GetName() != tc.wantPropName {
				t.Errorf("Expected root name %q, got: %q", tc.wantPropName, vMeta.GetName())
			}

			// 3. Assert properties does not contain redundant name
			if _, nameExists := vMeta.GetProperties().GetFields()["name"]; nameExists {
				t.Error("Expected properties Struct to exclude redundant 'name' key")
			}

			// 4. Assert provenance is resolved at root map level
			prov, ok := got.GetProvenances()[tc.wantProvDcid]
			if !ok {
				t.Fatalf("Missing expected root provenance metadata for %s", tc.wantProvDcid)
			}
			if prov.GetProperties().GetFields()["url"].GetStringValue() != tc.wantProvUrl {
				t.Errorf("Expected provenance url %q, got: %v", tc.wantProvUrl, prov.GetProperties().GetFields()["url"].GetStringValue())
			}

			// 5. Assert structured facets and scopes
			if len(vMeta.GetFacets()) != 1 {
				t.Fatalf("Expected exactly 1 facet, got: %d", len(vMeta.GetFacets()))
			}

			facet := vMeta.GetFacets()[0]
			if facet.GetProvenanceId() != tc.wantProvDcid {
				t.Errorf("Expected facet provenanceId %q, got: %q", tc.wantProvDcid, facet.GetProvenanceId())
			}
			if facet.GetObsCount() != 54 {
				t.Errorf("Expected obsCount 54, got: %d", facet.GetObsCount())
			}

			dateRange := facet.GetDateRange()
			if dateRange.GetStart() != "2010" {
				t.Errorf("Expected start date '2010', got: %q", dateRange.GetStart())
			}
			if dateRange.GetEnd() != "2020" {
				t.Errorf("Expected end date '2020', got: %q", dateRange.GetEnd())
			}

			scope := facet.GetScope()
			coverage := scope.GetEntityCoverage()
			if len(coverage) != 1 || coverage[0] != "geoId/06" {
				t.Errorf("Expected entityCoverage ['geoId/06'], got: %v", coverage)
			}

			// 6. Assert mock PlaceTypeSummary geographic granularities
			if len(tc.wantGranularities) > 0 {
				granularities := scope.GetEntityGranularity()
				if len(granularities) != len(tc.wantGranularities) {
					t.Errorf("Expected granularities count %d, got: %d (%v)", len(tc.wantGranularities), len(granularities), granularities)
				} else {
					for i, g := range granularities {
						if g != tc.wantGranularities[i] {
							t.Errorf("Expected granularity at index %d to be %q, got: %q", i, tc.wantGranularities[i], g)
						}
					}
				}
			}
		})
	}
}
