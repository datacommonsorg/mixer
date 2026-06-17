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
)

type mockVMMixerServer struct {
	Mixer
	nodeData map[string]*pbv2.LinkedGraph
	bulkData map[string]*pb.StatVarSummary
	obsData  map[string]*pb.Facet
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
		desc          string
		req           *pbv2.GetVariableMetadataRequest
		nodeData      map[string]*pbv2.LinkedGraph
		bulkData      map[string]*pb.StatVarSummary
		obsData       map[string]*pb.Facet
		wantVar       string
		wantPropName  string
		wantProvDcid  string
		wantProvUrl   string
		wantFacetName string
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
						"dc/base/USCensus_PEP": {ImportName: "USCensus_PEP"},
					},
				},
			},
			obsData: map[string]*pb.Facet{
				"facet1": {ImportName: "USCensus_PEP", ProvenanceId: "dc/base/USCensus_PEP"},
			},

			wantVar:       "Count_Person",
			wantPropName:  "Population",
			wantProvDcid:  "dc/base/USCensus_PEP",
			wantProvUrl:   "https://census.gov",
			wantFacetName: "USCensus_PEP",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mock := &mockVMMixerServer{
				nodeData: tc.nodeData,
				bulkData: tc.bulkData,
				obsData:  tc.obsData,
			}
			svc := NewService(mock, NewCache(mock))

			got, err := svc.GetVariableMetadata(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("GetVariableMetadata failed: %v", err)
			}

			if got == nil || got.GetStatus() != StatusSuccess {
				t.Fatalf("GetVariableMetadata returned non-success response: %v", got)
			}

			vMeta, ok := got.GetVariables()[tc.wantVar]
			if !ok {
				t.Fatalf("Missing expected variable metadata for %s", tc.wantVar)
			}

			if vMeta.GetProperties()["name"].GetValues()[0].GetValue() != tc.wantPropName {
				t.Errorf("Expected property name %q, got: %v", tc.wantPropName, vMeta.GetProperties())
			}

			prov, ok := vMeta.GetProvenances()[tc.wantProvDcid]
			if !ok {
				t.Fatalf("Missing expected provenance metadata for %s", tc.wantProvDcid)
			}
			if prov.GetProperties()["url"].GetValues()[0].GetValue() != tc.wantProvUrl {
				t.Errorf("Expected provenance url %q, got: %v", tc.wantProvUrl, prov.GetProperties())
			}

			eMeta, ok := vMeta.GetPerEntityMetadata()["geoId/06"]
			if !ok {
				t.Fatalf("Missing expected entity metadata for geoId/06")
			}
			fSum, ok := eMeta.GetFacetSeriesSummaries()["facet1"]
			if !ok || fSum == nil {
				t.Fatalf("Missing expected facet series summary for facet1")
			}
			if fSum.GetFacet().GetImportName() != tc.wantFacetName {
				t.Errorf("Expected per-entity facet importName %q, got: %v", tc.wantFacetName, fSum.GetFacet())
			}
			if fSum.GetObsCount() != 54 {
				t.Errorf("Expected obsCount 54, got: %v", fSum.GetObsCount())
			}
			if fSum.GetEarliestDate() != "2010" {
				t.Errorf("Expected earliestDate '2010', got: %v", fSum.GetEarliestDate())
			}
			if fSum.GetLatestDate() != "2020" {
				t.Errorf("Expected latestDate '2020', got: %v", fSum.GetLatestDate())
			}
		})
	}
}
