// Copyright 2024 Google LLC
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

// Package merger provides function to merge V2 API responses.
package merger

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestMergeResolve(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		r1   *pbv2.ResolveResponse
		r2   *pbv2.ResolveResponse
		want *pbv2.ResolveResponse
	}{
		{
			&pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node: "node1",
						Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
							{Dcid: "id1.1"},
							{Dcid: "id1.3"},
						},
					},
				},
			},
			&pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node: "node1",
						Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
							{Dcid: "id1.2"},
						},
					},
					{
						Node: "node2",
						Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
							{Dcid: "id2.1"},
						},
					},
				},
			},
			&pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node: "node1",
						Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
							{Dcid: "id1.1"},
							{Dcid: "id1.3"},
							{Dcid: "id1.2"},
						},
					},
					{
						Node: "node2",
						Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
							{Dcid: "id2.1"},
						},
					},
				},
			},
		},
	} {
		got := MergeResolve(c.r1, c.r2)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeResolve(%v, %v) got diff: %s", c.r1, c.r2, diff)
		}
	}
}

func TestMergeNode(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		local                *pbv2.NodeResponse
		remote               *pbv2.NodeResponse
		want                 *pbv2.NodeResponse
		localPaginationInfo  *pbv1.PaginationInfo
		remotePaginationInfo *pbv1.PaginationInfo
		wantPaginationInfo   *pbv1.PaginationInfo
	}{
		{
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": nil,
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1"},
								},
							},
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1"},
								},
							},
						},
					},
				},
			},
			nil,
			nil,
			nil,
		},
		{
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1"},
								},
							},
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1.2": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1.2"},
								},
							},
						},
					},
					"dcid2": {
						Arcs: map[string]*pbv2.Nodes{
							"prop2": {
								Nodes: []*pb.EntityInfo{
									{Value: "val2"},
								},
							},
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1"},
								},
							},
							"prop1.2": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1.2"},
								},
							},
						},
					},
					"dcid2": {
						Arcs: map[string]*pbv2.Nodes{
							"prop2": {
								Nodes: []*pb.EntityInfo{
									{Value: "val2"},
								},
							},
						},
					},
				},
			},
			&pbv1.PaginationInfo{
				CursorGroups: []*pbv1.CursorGroup{
					{
						Keys: []string{"key1"},
						Cursors: []*pbv1.Cursor{
							{
								ImportGroup: 1,
								Page:        1,
								Item:        5,
							},
						},
					},
				},
			},
			&pbv1.PaginationInfo{
				CursorGroups: []*pbv1.CursorGroup{
					{
						Keys: []string{"key2"},
						Cursors: []*pbv1.Cursor{
							{
								ImportGroup: 2,
								Page:        2,
								Item:        10,
							},
						},
					},
				},
			},
			&pbv1.PaginationInfo{
				CursorGroups: []*pbv1.CursorGroup{
					{
						Keys: []string{"key1"},
						Cursors: []*pbv1.Cursor{
							{
								ImportGroup: 1,
								Page:        1,
								Item:        5,
							},
						},
					},
				},
				RemotePaginationInfo: &pbv1.PaginationInfo{
					CursorGroups: []*pbv1.CursorGroup{
						{
							Keys: []string{"key2"},
							Cursors: []*pbv1.Cursor{
								{
									ImportGroup: 2,
									Page:        2,
									Item:        10,
								},
							},
						},
					},
				},
			},
		},
		{
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1.2": {},
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1.2": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1.2"},
								},
							},
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1.2": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1.2"},
								},
							},
						},
					},
				},
			},
			&pbv1.PaginationInfo{},
			&pbv1.PaginationInfo{
				CursorGroups: []*pbv1.CursorGroup{
					{
						Keys: []string{"key2"},
						Cursors: []*pbv1.Cursor{
							{
								ImportGroup: 2,
								Page:        2,
								Item:        10,
							},
						},
					},
				},
			},
			&pbv1.PaginationInfo{
				CursorGroups: []*pbv1.CursorGroup{},
				RemotePaginationInfo: &pbv1.PaginationInfo{
					CursorGroups: []*pbv1.CursorGroup{
						{
							Keys: []string{"key2"},
							Cursors: []*pbv1.Cursor{
								{
									ImportGroup: 2,
									Page:        2,
									Item:        10,
								},
							},
						},
					},
				},
			},
		},
		// Ensure that props with same name from the same dcid are merged
		{
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1"},
								},
							},
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": {
								Nodes: []*pb.EntityInfo{
									{Value: "val2"},
								},
							},
						},
					},
				},
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop1": {
								Nodes: []*pb.EntityInfo{
									{Value: "val1"},
									{Value: "val2"},
								},
							},
						},
					},
				},
			},
			nil,
			nil,
			nil,
		},
	} {
		var err error

		c.local.NextToken, err = util.EncodeProto(c.localPaginationInfo)
		if err != nil {
			t.Errorf("EncodeProto(%v) = %s", c.localPaginationInfo, err)
			continue
		}
		c.remote.NextToken, err = util.EncodeProto(c.remotePaginationInfo)
		if err != nil {
			t.Errorf("EncodeProto(%v) = %s", c.remotePaginationInfo, err)
			continue
		}
		c.want.NextToken, err = util.EncodeProto(c.wantPaginationInfo)
		if err != nil {
			t.Errorf("EncodeProto(%v) = %s", c.wantPaginationInfo, err)
			continue
		}

		got, err := MergeNode(c.local, c.remote)
		if err != nil {
			t.Errorf("MergeNode(%v, %v) = %s", c.local, c.remote, err)
			continue
		}
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeNode(%v, %v) got diff: %s", c.local, c.remote, diff)
		}
	}
}

func TestMergeEvent(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		e1   *pbv2.EventResponse
		e2   *pbv2.EventResponse
		want *pbv2.EventResponse
	}{
		{
			&pbv2.EventResponse{
				EventCollection: &pbv1.EventCollection{
					Events: []*pbv1.EventCollection_Event{
						{
							Dcid:         "event1",
							Places:       []string{"place1", "place2"},
							ProvenanceId: "prov1",
						},
					},
					ProvenanceInfo: map[string]*pbv1.EventCollection_ProvenanceInfo{
						"prov1": {ImportName: "import1"},
					},
				},
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2021"},
				},
			},
			&pbv2.EventResponse{
				EventCollection: &pbv1.EventCollection{
					Events: []*pbv1.EventCollection_Event{
						{
							Dcid:         "event2",
							Places:       []string{"place3", "place4"},
							ProvenanceId: "prov2",
						},
					},
					ProvenanceInfo: map[string]*pbv1.EventCollection_ProvenanceInfo{
						"prov2": {ImportName: "import2"},
					},
				},
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2022"},
				},
			},
			&pbv2.EventResponse{
				EventCollection: &pbv1.EventCollection{
					Events: []*pbv1.EventCollection_Event{
						{
							Dcid:         "event1",
							Places:       []string{"place1", "place2"},
							ProvenanceId: "prov1",
						},
						{
							Dcid:         "event2",
							Places:       []string{"place3", "place4"},
							ProvenanceId: "prov2",
						},
					},
					ProvenanceInfo: map[string]*pbv1.EventCollection_ProvenanceInfo{
						"prov2": {ImportName: "import2"},
						"prov1": {ImportName: "import1"},
					},
				},
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2021", "2022"},
				},
			},
		},
		{
			&pbv2.EventResponse{
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2021", "2022", "2023"},
				},
			},
			&pbv2.EventResponse{
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2022", "2023", "2024"},
				},
			},
			&pbv2.EventResponse{
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2021", "2022", "2023", "2024"},
				},
			},
		},
		{
			&pbv2.EventResponse{},
			&pbv2.EventResponse{
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2022", "2023", "2024"},
				},
			},
			&pbv2.EventResponse{
				EventCollectionDate: &pbv1.EventCollectionDate{
					Dates: []string{"2022", "2023", "2024"},
				},
			},
		},
	} {
		got := MergeEvent(c.e1, c.e2)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeEvent(%v, %v) got diff: %s", c.e1, c.e2, diff)
		}
	}
}

func TestMergeObservation(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		o1   *pbv2.ObservationResponse
		o2   *pbv2.ObservationResponse
		want *pbv2.ObservationResponse
	}{
		{
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet1",
										Observations: []*pb.PointStat{
											{
												Date:  "2021",
												Value: proto.Float64(45.4),
											},
										},
									},
									{
										FacetId: "facet2",
										Observations: []*pb.PointStat{
											{
												Date:  "2022",
												Value: proto.Float64(12.2),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(66.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet1",
										Observations: []*pb.PointStat{
											{
												Date:  "2021",
												Value: proto.Float64(45.4),
											},
										},
									},
									{
										FacetId: "facet2",
										Observations: []*pb.PointStat{
											{
												Date:  "2022",
												Value: proto.Float64(12.2),
											},
										},
									},
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(66.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			&pbv2.ObservationResponse{},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(66.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(66.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		got := MergeObservation(c.o1, c.o2)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeObservation(%v, %v) got diff: %s", c.o1, c.o2, diff)
		}
	}
}

func TestMergeMultiObservation(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		allResp []*pbv2.ObservationResponse
		want    *pbv2.ObservationResponse
	}{
		{[]*pbv2.ObservationResponse{
			{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet1",
										Observations: []*pb.PointStat{
											{
												Date:  "2021",
												Value: proto.Float64(45.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet2",
										Observations: []*pb.PointStat{
											{
												Date:  "2022",
												Value: proto.Float64(66.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(7.28),
											},
										},
									},
								},
							},
						},
					},
				},
			}},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet1",
										Observations: []*pb.PointStat{
											{
												Date:  "2021",
												Value: proto.Float64(45.4),
											},
										},
									},
									{
										FacetId: "facet2",
										Observations: []*pb.PointStat{
											{
												Date:  "2022",
												Value: proto.Float64(66.4),
											},
										},
									},
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(7.28),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		got := MergeMultiObservation(c.allResp)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeMultiObservation(%v) got diff: %s", c.allResp, diff)
		}
	}
}

func TestMergeBulkVariableInfoResponse(t *testing.T) {
	cmpOpts := cmp.Options{protocmp.Transform()}
	for _, tc := range []struct {
		desc      string
		primary   *pbv1.BulkVariableInfoResponse
		secondary *pbv1.BulkVariableInfoResponse
		want      *pbv1.BulkVariableInfoResponse
	}{{
		desc: "primary only",
		primary: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
						},
					},
				},
			},
		},
		want: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
						},
					},
				},
			},
		},
	}, {
		desc: "secondary only",
		secondary: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v2",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"TR1": {PlaceCount: 11},
							"TR2": {PlaceCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
						},
					},
				},
			},
		},
		want: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v2",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"TR1": {PlaceCount: 11},
							"TR2": {PlaceCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
						},
					},
				},
			},
		},
	}, {
		desc: "combined",
		primary: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
						},
					},
				},
			},
		},
		secondary: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v2",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"TR1": {PlaceCount: 11},
							"TR2": {PlaceCount: 12},
						},
					},
				},
				{
					Node: "v3",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
						},
					},
				},
			},
		},
		want: &pbv1.BulkVariableInfoResponse{
			Data: []*pbv1.VariableInfoResponse{
				{
					Node: "v1",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T1": {PlaceCount: 1},
							"T2": {PlaceCount: 2},
						},
					},
				},
				{
					Node: "v2",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"T3": {PlaceCount: 3},
							"T4": {PlaceCount: 4},
						},
					},
				},
				{
					Node: "v3",
					Info: &pb.StatVarSummary{
						PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
							"TR3": {PlaceCount: 13},
							"TR4": {PlaceCount: 14},
						},
					},
				},
			},
		},
	}} {
		got := MergeBulkVariableInfoResponse(tc.primary, tc.secondary)
		if diff := cmp.Diff(got, tc.want, cmpOpts); diff != "" {
			t.Errorf("%s: got diff: %s", tc.desc, diff)
		}
	}
}

func TestMergeSearchStatVarResponse(t *testing.T) {
	cmpOpts := cmp.Options{protocmp.Transform()}
	for _, tc := range []struct {
		desc      string
		primary   *pb.SearchStatVarResponse
		secondary *pb.SearchStatVarResponse
		want      *pb.SearchStatVarResponse
	}{{
		desc: "primary only",
		primary: &pb.SearchStatVarResponse{
			StatVars: []*pb.EntityInfo{
				{
					Name: "sv1",
					Dcid: "svid1",
				},
				{
					Name: "sv2",
					Dcid: "svid2",
				},
			},
			Matches: []string{"match1", "match2"},
		},
		want: &pb.SearchStatVarResponse{
			StatVars: []*pb.EntityInfo{
				{
					Name: "sv1",
					Dcid: "svid1",
				},
				{
					Name: "sv2",
					Dcid: "svid2",
				},
			},
			Matches: []string{"match1", "match2"},
		},
	}, {
		desc: "secondary only",
		secondary: &pb.SearchStatVarResponse{
			StatVars: []*pb.EntityInfo{
				{
					Name: "sv1",
					Dcid: "svid1",
				},
				{
					Name: "sv2",
					Dcid: "svid2",
				},
			},
			Matches: []string{"match1", "match2"},
		},
		want: &pb.SearchStatVarResponse{
			StatVars: []*pb.EntityInfo{
				{
					Name: "sv1",
					Dcid: "svid1",
				},
				{
					Name: "sv2",
					Dcid: "svid2",
				},
			},
			Matches: []string{"match1", "match2"},
		},
	}, {
		desc: "combined",
		primary: &pb.SearchStatVarResponse{
			StatVars: []*pb.EntityInfo{
				{
					Name: "sv1",
					Dcid: "svid1",
				},
				{
					Name: "sv2",
					Dcid: "svid2",
				},
			},
			Matches: []string{"match1", "match2"},
		},
		secondary: &pb.SearchStatVarResponse{
			StatVars: []*pb.EntityInfo{
				{
					Name: "sv3",
					Dcid: "svid3",
				},
				{
					Name: "sv4",
					Dcid: "svid4",
				},
			},
			Matches: []string{"match1", "match3"},
		},
		want: &pb.SearchStatVarResponse{
			StatVars: []*pb.EntityInfo{
				{
					Name: "sv1",
					Dcid: "svid1",
				},
				{
					Name: "sv2",
					Dcid: "svid2",
				},
				{
					Name: "sv3",
					Dcid: "svid3",
				},
				{
					Name: "sv4",
					Dcid: "svid4",
				},
			},
			Matches: []string{"match1", "match2", "match3"},
		},
	}} {
		got := MergeSearchStatVarResponse(tc.primary, tc.secondary)
		if diff := cmp.Diff(got, tc.want, cmpOpts); diff != "" {
			t.Errorf("%s: got diff: %s", tc.desc, diff)
		}
	}
}
