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

package datasource

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockDataSource struct {
	DataSource
	responses []*pbv2.NodeResponse
	errs      []error
	calls     int
}

func (m *mockDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	if m.calls >= len(m.responses) {
		return nil, errors.New("too many calls")
	}
	resp := m.responses[m.calls]
	err := m.errs[m.calls]
	m.calls++
	return resp, err
}

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

func TestNodeFetchAll(t *testing.T) {
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
			name: "FetchError",
			responses: []*pbv2.NodeResponse{
				makeNodeResponse("token1", "geoId/06001"),
				nil,
			},
			errs:    []error{nil, errors.New("fetch error")},
			wantErr: true,
			errStr:  "fetch error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockDS := &mockDataSource{
				responses: tc.responses,
				errs:      tc.errs,
			}

			got, err := NodeFetchAll(ctx, mockDS, req, 10)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NodeFetchAll() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !strings.Contains(err.Error(), tc.errStr) {
					t.Errorf("Expected error containing '%s', got '%v'", tc.errStr, err)
				}
				return
			}

			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("NodeFetchAll() mismatch (-want +got):\n%s", diff)
			}

			if mockDS.calls != len(tc.responses) {
				t.Errorf("Expected %d calls, got %d", len(tc.responses), mockDS.calls)
			}
		})
	}
}
