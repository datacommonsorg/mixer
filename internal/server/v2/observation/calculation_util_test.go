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

package observation

import (
	"go/token"
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/statvar/formula"
	"google.golang.org/protobuf/proto"
)

func TestFindObservationResponseHoles(t *testing.T) {
	for _, c := range []struct {
		inputReq  *pbv2.ObservationRequest
		inputResp *pbv2.ObservationResponse
		want      map[string]*pbv2.DcidOrExpression
	}{
		{
			&pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{Dcids: []string{
					"Count_Person",
					"Count_Farm",
				}},
				Entity: &pbv2.DcidOrExpression{Dcids: []string{
					"geoId/01",
					"geoId/02",
				}},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {ByEntity: map[string]*pbv2.EntityObservation{
						"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{{FacetId: "1"}}},
						"geoId/02": {OrderedFacets: []*pbv2.FacetObservation{}},
					}},
					"Count_Farm": {ByEntity: map[string]*pbv2.EntityObservation{
						"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{}},
						"geoId/02": {OrderedFacets: []*pbv2.FacetObservation{{FacetId: "2"}}},
					}},
				},
			},
			map[string]*pbv2.DcidOrExpression{
				"Count_Person": {Dcids: []string{"geoId/02"}},
				"Count_Farm":   {Dcids: []string{"geoId/01"}},
			},
		},
		{
			&pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{Dcids: []string{
					"Count_Person",
					"Count_Farm",
				}},
				Entity: &pbv2.DcidOrExpression{Expression: "country/USA<-containedInPlace+{typeOf:State}"},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {ByEntity: map[string]*pbv2.EntityObservation{"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{}}}},
					"Count_Farm":   {ByEntity: map[string]*pbv2.EntityObservation{}},
				},
			},
			map[string]*pbv2.DcidOrExpression{
				"Count_Farm": {Expression: "country/USA<-containedInPlace+{typeOf:State}"},
			},
		},
	} {
		got, err := findObservationResponseHoles(c.inputReq, c.inputResp)
		if err != nil {
			t.Errorf("error running TestFindObservationResponseHoles: %s", err)
			continue
		}
		if ok := reflect.DeepEqual(got, c.want); !ok {
			t.Errorf("findObservationResponseHoles(%v, %v) = %v, want %v",
				c.inputReq, c.inputResp, got, c.want)
		}
	}
}

func TestFilterObsByASTNode(t *testing.T) {
	sampleInputResp := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			"Count_Person": {ByEntity: map[string]*pbv2.EntityObservation{
				"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{
					{
						FacetId: "1",
						Observations: []*pb.PointStat{{
							Date:  "1",
							Value: proto.Float64(1),
						}},
					},
					{
						FacetId: "2",
						Observations: []*pb.PointStat{{
							Date:  "2",
							Value: proto.Float64(2),
						}},
					},
				}},
			}},
		},
		Facets: map[string]*pb.Facet{
			"1": {
				ObservationPeriod: "P1M",
			},
			"2": {
				MeasurementMethod: "US_Census",
				ObservationPeriod: "P1Y",
			},
		},
	}
	for _, c := range []struct {
		inputResp *pbv2.ObservationResponse
		node      *formula.ASTNode
		want      *pbv2.VariableObservation
	}{{
		sampleInputResp,
		&formula.ASTNode{StatVar: "Count_Person"},
		&pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{
				"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{
					{
						FacetId: "1",
						Observations: []*pb.PointStat{{
							Date:  "1",
							Value: proto.Float64(1),
						}},
					},
					{
						FacetId: "2",
						Observations: []*pb.PointStat{{
							Date:  "2",
							Value: proto.Float64(2),
						}},
					},
				}},
			},
		},
	},
		{
			sampleInputResp,
			&formula.ASTNode{
				StatVar: "Count_Person",
				Facet: &pb.Facet{
					MeasurementMethod: "US_Census",
					ObservationPeriod: "P1Y",
				},
			},
			&pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{
						{
							FacetId: "2",
							Observations: []*pb.PointStat{{
								Date:  "2",
								Value: proto.Float64(2),
							}},
						},
					}},
				},
			},
		},
	} {
		got := filterObsByASTNode(c.inputResp, c.node)
		if ok := reflect.DeepEqual(got, c.want); !ok {
			t.Errorf("filterObsByASTNode(%v, %v) = %v, want %v",
				c.inputResp, c.node, got, c.want)
		}
	}
}

func TestMergePointStat(t *testing.T) {
	inputX := []*pb.PointStat{{
		Date:  "1",
		Value: proto.Float64(6),
	}}
	inputY := []*pb.PointStat{{
		Date:  "1",
		Value: proto.Float64(2),
	}}
	for _, c := range []struct {
		x    []*pb.PointStat
		y    []*pb.PointStat
		op   token.Token
		want []*pb.PointStat
	}{
		{
			[]*pb.PointStat{
				{
					Date:  "1",
					Value: proto.Float64(1),
				},
				{
					Date:  "3",
					Value: proto.Float64(3),
				},
				{
					Date:  "4",
					Value: proto.Float64(4),
				},
				{
					Date:  "5",
					Value: proto.Float64(5),
				},
				{
					Date:  "8",
					Value: proto.Float64(8),
				},
			},
			[]*pb.PointStat{
				{
					Date:  "0",
					Value: proto.Float64(0),
				},
				{
					Date:  "2",
					Value: proto.Float64(2),
				},
				{
					Date:  "3",
					Value: proto.Float64(3),
				},
				{
					Date:  "5",
					Value: proto.Float64(5),
				},
				{
					Date:  "6",
					Value: proto.Float64(6),
				},
				{
					Date:  "7",
					Value: proto.Float64(7),
				},
				{
					Date:  "9",
					Value: proto.Float64(9),
				},
			},
			token.ADD,
			[]*pb.PointStat{
				{
					Date:  "3",
					Value: proto.Float64(6),
				},
				{
					Date:  "5",
					Value: proto.Float64(10),
				},
			},
		},
		{
			inputX,
			inputY,
			token.SUB,
			[]*pb.PointStat{{
				Date:  "1",
				Value: proto.Float64(4),
			}},
		},
		{
			inputX,
			inputY,
			token.MUL,
			[]*pb.PointStat{{
				Date:  "1",
				Value: proto.Float64(12),
			}},
		},
		{
			inputX,
			inputY,
			token.QUO,
			[]*pb.PointStat{{
				Date:  "1",
				Value: proto.Float64(3),
			}},
		},
	} {
		got, err := mergePointStat(c.x, c.y, c.op)
		if err != nil {
			t.Errorf("error running TestMergePointStat: %s", err)
			continue
		}
		if ok := reflect.DeepEqual(got, c.want); !ok {
			t.Errorf("mergePointStat(%v, %v, %v) = %v, want %v",
				c.x, c.y, c.op, got, c.want)
		}
	}
}

func TestEvalExpr(t *testing.T) {
	for _, c := range []struct {
		inputExpr string
		leafData  map[string]*formula.ASTNode
		inputResp *pbv2.ObservationResponse
		want      *pbv2.VariableObservation
	}{
		{
			"(SV_1 - SV_2) / SV_3",
			map[string]*formula.ASTNode{
				"SV_1": {StatVar: "SV_1"},
				"SV_2": {StatVar: "SV_2"},
				"SV_3": {StatVar: "SV_3"},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"SV_1": {ByEntity: map[string]*pbv2.EntityObservation{
						"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{{
							FacetId: "facetId1",
							Observations: []*pb.PointStat{{
								Date:  "1",
								Value: proto.Float64(10),
							}},
						}}},
					}},
					"SV_2": {ByEntity: map[string]*pbv2.EntityObservation{
						"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{{
							FacetId: "facetId1",
							Observations: []*pb.PointStat{{
								Date:  "1",
								Value: proto.Float64(4),
							}},
						}}},
					}},
					"SV_3": {ByEntity: map[string]*pbv2.EntityObservation{
						"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{{
							FacetId: "facetId1",
							Observations: []*pb.PointStat{{
								Date:  "1",
								Value: proto.Float64(2),
							}},
						}}},
					}},
				},
				Facets: map[string]*pb.Facet{
					"facetId1": {
						ObservationPeriod: "P1Y",
					},
				},
			},
			&pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{
					"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{{
						FacetId: "facetId1",
						Observations: []*pb.PointStat{{
							Date:  "1",
							Value: proto.Float64(3),
						}},
						EarliestDate: "1",
						LatestDate:   "1",
						ObsCount:     1,
					}}},
				},
			},
		},
	} {
		f, err := formula.NewVariableFormula(c.inputExpr)
		if err != nil {
			t.Errorf("error running TestEvalExpr: %s", err)
			continue
		}
		got, err := evalExpr(f.Expr, c.leafData, c.inputResp)
		if err != nil {
			t.Errorf("error running TestEvalExpr: %s", err)
			continue
		}
		if ok := reflect.DeepEqual(got, c.want); !ok {
			t.Errorf("evalExpr(%v, %v, %v) = %v, want %v",
				c.inputExpr, c.leafData, c.inputResp, got, c.want)
		}
	}
}
