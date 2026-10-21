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
	"sync"
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
// request without a next token, and the last page otherwise. It is not safe for
// concurrent calls, so keep its node lists within nodefetcher's chunk size
// (fetchAllChunkSize, 200), where requests are fetched sequentially.
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
	nextToken := testNextToken(t)
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

// testNextToken returns a valid Node pagination token for fake sources to
// return.
func testNextToken(t *testing.T) string {
	t.Helper()
	token, err := util.EncodeProto(&pbv2.Pagination{
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
	return token
}

// chunkedFormulaSource answers the StatisticalCalculation lookups for calcs.
// It is safe for the concurrent calls that chunked fetching makes. Each
// properties request comes back in two pages: outputs first, then input
// expressions.
type chunkedFormulaSource struct {
	datasource.DataSource
	calcs     []string
	nextToken string
	// maxCalls bounds the Node calls, so a page loop that never advances its
	// token fails instead of hanging.
	maxCalls int

	mu    sync.Mutex
	calls int
	// largestRequest is the most nodes seen in one properties request.
	largestRequest int
}

func (s *chunkedFormulaSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	s.mu.Lock()
	s.calls++
	calls := s.calls
	s.mu.Unlock()
	if calls > s.maxCalls {
		return nil, errors.New("too many Node calls")
	}

	switch req.GetProperty() {
	case "<-" + typeOf:
		nodes := make([]*pb.EntityInfo, 0, len(s.calcs))
		for _, calc := range s.calcs {
			nodes = append(nodes, &pb.EntityInfo{Dcid: calc})
		}
		return &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{
			StatisticalCalculation: {Arcs: map[string]*pbv2.Nodes{typeOf: {Nodes: nodes}}},
		}}, nil
	case "->[" + outputProperty + ", " + inputPropertyExpression + "]":
		s.mu.Lock()
		s.largestRequest = max(s.largestRequest, len(req.GetNodes()))
		s.mu.Unlock()
		firstPage := req.GetNextToken() == ""
		resp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
		for _, calc := range req.GetNodes() {
			arcs := map[string]*pbv2.Nodes{}
			if firstPage {
				arcs[outputProperty] = &pbv2.Nodes{Nodes: []*pb.EntityInfo{{Dcid: "sv/" + calc}}}
			} else {
				arcs[inputPropertyExpression] = &pbv2.Nodes{Nodes: []*pb.EntityInfo{{Value: "expr/" + calc}}}
			}
			resp.Data[calc] = &pbv2.LinkedGraph{Arcs: arcs}
		}
		if firstPage {
			resp.NextToken = s.nextToken
		}
		return resp, nil
	default:
		return nil, fmt.Errorf("unexpected property %q", req.GetProperty())
	}
}

// A long node list is split into chunks, like datasource.NodeFetchAll does, and
// every page of every chunk is still read before formulas are matched.
func TestFetchSVFormulasChunksLargeNodeLists(t *testing.T) {
	// More nodes than one chunk holds (nodefetcher's fetchAllChunkSize, 200), so
	// the properties request is split.
	const numCalcs = 450
	calcs := make([]string, numCalcs)
	want := make(map[string][]string, numCalcs)
	for i := range calcs {
		calcs[i] = fmt.Sprintf("calc%03d", i)
		want["sv/"+calcs[i]] = []string{"expr/" + calcs[i]}
	}
	source := &chunkedFormulaSource{
		calcs:     calcs,
		nextToken: testNextToken(t),
		// One request per page per node is the most chunking could ever need.
		maxCalls: 2*numCalcs + 1,
	}
	ds := datasources.NewDataSources([]datasource.DataSource{source}, nil)

	got, err := FetchSVFormulas(context.Background(), ds)
	if err != nil {
		t.Fatalf("FetchSVFormulas() error = %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("FetchSVFormulas() mismatch (-want +got):\n%s", diff)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.largestRequest >= numCalcs {
		t.Errorf("largest properties request had %d nodes, want fewer than %d (chunked)", source.largestRequest, numCalcs)
	}
}

// Nodes with a nil graph, a nil arc, or only one of the two properties add
// nothing, and must not panic.
func TestFormulasFromNodeResponseSkipsIncompleteNodes(t *testing.T) {
	for _, tc := range []struct {
		desc string
		resp *pbv2.NodeResponse
		want map[string][]string
	}{
		{
			desc: "nil response",
			want: map[string][]string{},
		},
		{
			desc: "incomplete nodes",
			resp: &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{
				"calc1": {Arcs: map[string]*pbv2.Nodes{
					outputProperty:          {Nodes: []*pb.EntityInfo{{Dcid: "sv1"}}},
					inputPropertyExpression: {Nodes: []*pb.EntityInfo{{Value: "expr1"}}},
				}},
				"nilGraph": nil,
				"nilArc": {Arcs: map[string]*pbv2.Nodes{
					outputProperty:          nil,
					inputPropertyExpression: {Nodes: []*pb.EntityInfo{{Value: "expr2"}}},
				}},
				"noInput": {Arcs: map[string]*pbv2.Nodes{
					outputProperty: {Nodes: []*pb.EntityInfo{{Dcid: "sv3"}}},
				}},
			}},
			want: map[string][]string{"sv1": {"expr1"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, formulasFromNodeResponse(tc.resp)); diff != "" {
				t.Errorf("formulasFromNodeResponse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
