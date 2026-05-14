package datasource

import (
	"context"
	"errors"
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
		// Test: Single page response.
		// Situation: The data source returns a response with no NextToken.
		// Expectation: The helper returns the response directly without further calls.
		{
			name: "SinglePage",
			responses: []*pbv2.NodeResponse{
				makeNodeResponse("", "geoId/06001"),
			},
			errs: []error{nil},
			want: makeNodeResponse("", "geoId/06001"),
		},
		// Test: Multi-page response.
		// Situation: The data source returns 3 pages of data, linked by NextToken.
		// Expectation: The helper loops 3 times, fetches all pages, and merges the nodes correctly.
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
		// Test: Error on initial call.
		// Situation: The data source fails on the very first call.
		// Expectation: The helper immediately returns the error.
		{
			name:      "InitialError",
			responses: []*pbv2.NodeResponse{nil},
			errs:      []error{errors.New("initial error")},
			wantErr:   true,
			errStr:    "initial error",
		},
		// Test: Error on subsequent call.
		// Situation: The data source succeeds on the first call but fails on the second.
		// Expectation: The helper returns the error encountered on the second call.
		{
			name: "SubsequentError",
			responses: []*pbv2.NodeResponse{
				makeNodeResponse("token1", "geoId/06001"),
				nil,
			},
			errs:    []error{nil, errors.New("subsequent error")},
			wantErr: true,
			errStr:  "subsequent error",
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
				if err.Error() != tc.errStr {
					t.Errorf("Expected error '%s', got '%v'", tc.errStr, err)
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
