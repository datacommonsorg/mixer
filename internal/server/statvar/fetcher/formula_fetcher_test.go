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

package fetcher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
)

// maxTestNodeCalls bounds the Node calls in a test, so a page loop that never
// advances its token fails instead of hanging.
const maxTestNodeCalls = 10

// twoPageNodeSource returns the first page of a property's response for a
// request without a next token, and the last page otherwise.
type twoPageNodeSource struct {
	datasource.DataSource
	pagesByProperty map[string][2]*pbv2.NodeResponse
	calls           int
}

func (s *twoPageNodeSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	s.calls++
	if s.calls > maxTestNodeCalls {
		return nil, errors.New("too many Node calls")
	}
	pages, ok := s.pagesByProperty[req.GetProperty()]
	if !ok {
		return nil, fmt.Errorf("unexpected property %q", req.GetProperty())
	}
	page := pages[0]
	if req.GetNextToken() != "" {
		page = pages[1]
	}
	// Callers merge pages into the responses they get, so return a copy.
	return proto.Clone(page).(*pbv2.NodeResponse), nil
}

func TestFetchSVFormulasReadsEveryPage(t *testing.T) {
	nextToken, err := util.EncodeProto(&pbv2.Pagination{
		Info: []*pbv2.Pagination_DataSourceInfo{{
			Id: "test",
			DataSourceInfo: &pbv2.Pagination_DataSourceInfo_SpannerInfo{
				SpannerInfo: &pbv2.SpannerInfo{Offset: 1},
			},
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProto() error = %v", err)
	}
	graph := func(arcs map[string][]*pb.EntityInfo) *pbv2.LinkedGraph {
		g := &pbv2.LinkedGraph{Arcs: map[string]*pbv2.Nodes{}}
		for prop, nodes := range arcs {
			g.Arcs[prop] = &pbv2.Nodes{Nodes: nodes}
		}
		return g
	}

	source := &twoPageNodeSource{pagesByProperty: map[string][2]*pbv2.NodeResponse{
		"<-typeOf": {
			{
				Data: map[string]*pbv2.LinkedGraph{
					StatisticalCalculation: graph(map[string][]*pb.EntityInfo{typeOf: {{Dcid: "calc1"}}}),
				},
				NextToken: nextToken,
			},
			{
				Data: map[string]*pbv2.LinkedGraph{
					StatisticalCalculation: graph(map[string][]*pb.EntityInfo{typeOf: {{Dcid: "calc2"}}}),
				},
			},
		},
		"->[outputProperty, inputPropertyExpression]": {
			{
				Data: map[string]*pbv2.LinkedGraph{
					"calc1": graph(map[string][]*pb.EntityInfo{
						outputProperty:          {{Dcid: "sv1"}},
						inputPropertyExpression: {{Value: "expr1"}},
					}),
					// calc2's input expression is on the next page.
					"calc2": graph(map[string][]*pb.EntityInfo{outputProperty: {{Dcid: "sv2"}}}),
				},
				NextToken: nextToken,
			},
			{
				Data: map[string]*pbv2.LinkedGraph{
					"calc2": graph(map[string][]*pb.EntityInfo{inputPropertyExpression: {{Value: "expr2"}}}),
				},
			},
		},
	}}
	ds := datasources.NewDataSources([]datasource.DataSource{source}, nil)

	got, err := FetchSVFormulas(context.Background(), ds)
	if err != nil {
		t.Fatalf("FetchSVFormulas() error = %v", err)
	}
	want := map[string][]string{"sv1": {"expr1"}, "sv2": {"expr2"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("FetchSVFormulas() mismatch (-want +got):\n%s", diff)
	}
}
