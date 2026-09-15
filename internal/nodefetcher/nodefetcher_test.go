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

package nodefetcher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// makeNodeResponse helper generates the standard test response graph.
func makeNodeResponse(nextToken string, childDcids ...string) *pbv2.NodeResponse {
	var nodes []*pb.EntityInfo
	for _, dcid := range childDcids {
		nodes = append(nodes, &pb.EntityInfo{Dcid: dcid})
	}
	return &pbv2.NodeResponse{
		NextToken: nextToken,
		Data: map[string]*pbv2.LinkedGraph{
			"geoId/06": {
				Arcs: map[string]*pbv2.Nodes{
					"containedInPlace": {Nodes: nodes},
				},
			},
		},
	}
}

func TestNodeFetchAllFunc(t *testing.T) {
	ctx := context.Background()
	req := &pbv2.NodeRequest{Nodes: []string{"geoId/06"}}

	cases := []struct {
		name      string
		responses []*pbv2.NodeResponse
		errs      []error
		want      *pbv2.NodeResponse
		wantErr   bool
		errStr    string
	}{
		{
			name: "SinglePage",
			responses: []*pbv2.NodeResponse{
				makeNodeResponse("", "geoId/06001"),
			},
			errs: []error{nil},
			want: makeNodeResponse("", "geoId/06001"),
		},
		{
			name: "MultiPage",
			responses: []*pbv2.NodeResponse{
				makeNodeResponse("token1", "geoId/06001"),
				makeNodeResponse("token2", "geoId/06002"),
				makeNodeResponse("", "geoId/06003"),
			},
			errs: []error{nil, nil, nil},
			want: makeNodeResponse("", "geoId/06001", "geoId/06002", "geoId/06003"),
		},
		{
			// Spanner pages by edge-row offset, so an entity straddling the page
			// boundary gets some of its arcs on one page and the rest on the next.
			name: "EntitySplitAcrossPages",
			responses: []*pbv2.NodeResponse{
				{
					NextToken: "token1",
					Data: map[string]*pbv2.LinkedGraph{
						"country/MSR": {Arcs: map[string]*pbv2.Nodes{
							"name": {Nodes: []*pb.EntityInfo{{Value: "Montserrat"}}},
						}},
					},
				},
				{
					Data: map[string]*pbv2.LinkedGraph{
						"country/MSR": {Arcs: map[string]*pbv2.Nodes{
							"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "AdministrativeArea1"}, {Dcid: "Country"}}},
						}},
					},
				},
			},
			errs: []error{nil, nil},
			want: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/MSR": {Arcs: map[string]*pbv2.Nodes{
						"name":   {Nodes: []*pb.EntityInfo{{Value: "Montserrat"}}},
						"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "AdministrativeArea1"}, {Dcid: "Country"}}},
					}},
				},
			},
		},
		{
			name:      "FetchError",
			responses: []*pbv2.NodeResponse{nil},
			errs:      []error{errors.New("fetch error")},
			wantErr:   true,
			errStr:    "fetch error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			fetch := func(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
				if calls >= len(tc.responses) {
					return nil, errors.New("too many calls")
				}
				resp := tc.responses[calls]
				err := tc.errs[calls]
				calls++
				return resp, err
			}

			got, err := NodeFetchAllFunc(ctx, fetch, req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NodeFetchAllFunc() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !strings.Contains(err.Error(), tc.errStr) {
					t.Errorf("Expected error containing '%s', got '%v'", tc.errStr, err)
				}
				return
			}

			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("NodeFetchAllFunc() mismatch (-want +got):\n%s", diff)
			}

			if calls != len(tc.responses) {
				t.Errorf("Expected %d calls, got %d", len(tc.responses), calls)
			}
		})
	}
}

// chunkRecorder returns one name arc per requested node and records which nodes
// each chunk asked for. Safe to call from parallel chunk fetches.
type chunkRecorder struct {
	mu                sync.Mutex
	requestedNodeSets [][]string

	// splitChunkContaining makes the chunk holding this node span two pages.
	splitChunkContaining string

	// failChunkContaining makes the chunk holding this node return an error.
	failChunkContaining string
}

func (r *chunkRecorder) fetch(_ context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	nodes := req.GetNodes()

	r.mu.Lock()
	r.requestedNodeSets = append(r.requestedNodeSets, slices.Clone(nodes))
	r.mu.Unlock()

	if r.failChunkContaining != "" && slices.Contains(nodes, r.failChunkContaining) {
		return nil, errors.New("chunk fetch failed")
	}

	if r.splitChunkContaining != "" && slices.Contains(nodes, r.splitChunkContaining) {
		if req.GetNextToken() == "" {
			return namedNodeResponse("page2", nodes[:1]...), nil
		}
		return namedNodeResponse("", nodes[1:]...), nil
	}

	return namedNodeResponse("", nodes...), nil
}

// chunkSizes returns the recorded request sizes, sorted, since chunks can finish
// in any order.
func (r *chunkRecorder) chunkSizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()

	sizes := make([]int, 0, len(r.requestedNodeSets))
	for _, set := range r.requestedNodeSets {
		sizes = append(sizes, len(set))
	}
	sort.Ints(sizes)
	return sizes
}

// namedNodeResponse builds a response giving each dcid a single name arc.
func namedNodeResponse(nextToken string, dcids ...string) *pbv2.NodeResponse {
	resp := &pbv2.NodeResponse{
		NextToken: nextToken,
		Data:      make(map[string]*pbv2.LinkedGraph),
	}
	for _, dcid := range dcids {
		resp.Data[dcid] = &pbv2.LinkedGraph{
			Arcs: map[string]*pbv2.Nodes{
				"name": {Nodes: []*pb.EntityInfo{{Value: dcid + "-name"}}},
			},
		}
	}
	return resp
}

// sequentialDcids builds count dcids in the order the request will be chunked.
func sequentialDcids(count int) []string {
	dcids := make([]string, 0, count)
	for i := range count {
		dcids = append(dcids, fmt.Sprintf("dcid/%04d", i))
	}
	return dcids
}

func TestNodeFetchAllChunkedFunc(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name                 string
		nodeCount            int
		splitChunkContaining string
		failChunkContaining  string
		wantChunkSizes       []int
		wantErr              string
	}{
		{
			name:           "BelowThresholdIsNotChunked",
			nodeCount:      10,
			wantChunkSizes: []int{10},
		},
		{
			name:           "AtThresholdIsNotChunked",
			nodeCount:      fetchAllChunkSize,
			wantChunkSizes: []int{fetchAllChunkSize},
		},
		{
			name:           "AboveThresholdSplitsIntoChunks",
			nodeCount:      450,
			wantChunkSizes: []int{50, 200, 200},
		},
		{
			name:                 "ChunkSpanningMultiplePagesIsFullyFetched",
			nodeCount:            450,
			splitChunkContaining: "dcid/0000",
			wantChunkSizes:       []int{50, 200, 200, 200},
		},
		{
			name:                "ChunkErrorPropagates",
			nodeCount:           450,
			failChunkContaining: "dcid/0000",
			wantErr:             "chunk fetch failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dcids := sequentialDcids(tc.nodeCount)
			recorder := &chunkRecorder{
				splitChunkContaining: tc.splitChunkContaining,
				failChunkContaining:  tc.failChunkContaining,
			}

			got, err := NodeFetchAllChunkedFunc(ctx, recorder.fetch, &pbv2.NodeRequest{Nodes: dcids})

			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("NodeFetchAllChunkedFunc() error = nil, want error containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("NodeFetchAllChunkedFunc() error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NodeFetchAllChunkedFunc() unexpected error: %v", err)
			}

			want := namedNodeResponse("", dcids...)
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("NodeFetchAllChunkedFunc() mismatch (-want +got):\n%s", diff)
			}

			if diff := cmp.Diff(tc.wantChunkSizes, recorder.chunkSizes()); diff != "" {
				t.Errorf("chunk sizes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
