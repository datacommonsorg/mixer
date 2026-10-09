package datasources

import (
	"context"
	"math"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockDataSource struct {
	datasource.DataSource
	id   string
	typ  datasource.DataSourceType
	resp *pbv2.NodeResponse
}

func (m *mockDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	return m.resp, nil
}

func (m *mockDataSource) Id() string                      { return m.id }
func (m *mockDataSource) Type() datasource.DataSourceType { return m.typ }

func TestFederatedNodeWithDanglingEdges(t *testing.T) {
	ctx := context.Background()

	// Local data source simulates Spanner returning an unresolved dangling edge.
	localDS := &mockDataSource{
		id:  "local_spanner",
		typ: datasource.TypeSpanner,
		resp: &pbv2.NodeResponse{
			Data: map[string]*pbv2.LinkedGraph{
				"geoId/06": {
					Arcs: map[string]*pbv2.Nodes{
						"containedInPlace": {
							Nodes: []*pb.EntityInfo{
								{
									Dcid: "country/USA",
									// Type thing to simulate current unresolved response structure.
									Types: []string{"Thing"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Remote data source simulates the remote mixer returning the fully hydrated edge.
	// Note: Currently, the merger deduplicates by Dcid and keeps the first encountered
	// node's fields. Thus, if the local source is first, its unresolved version will be kept.
	remoteDS := &mockDataSource{
		id:  "remote_mixer",
		typ: datasource.TypeRemote,
		resp: &pbv2.NodeResponse{
			Data: map[string]*pbv2.LinkedGraph{
				"geoId/06": {
					Arcs: map[string]*pbv2.Nodes{
						"containedInPlace": {
							Nodes: []*pb.EntityInfo{
								{
									Dcid:  "country/USA",
									Name:  "United States",
									Types: []string{"Country"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Create federated data source.
	federatedDS := NewDataSources([]datasource.DataSource{localDS, remoteDS}, remoteDS)

	req := &pbv2.NodeRequest{
		Nodes:    []string{"geoId/06"},
		Property: "->containedInPlace",
	}

	got, err := federatedDS.Node(ctx, req, DefaultPageSize)
	if err != nil {
		t.Fatalf("federatedDS.Node() error = %v", err)
	}

	want := &pbv2.NodeResponse{
		Data: map[string]*pbv2.LinkedGraph{
			"geoId/06": {
				Arcs: map[string]*pbv2.Nodes{
					"containedInPlace": {
						Nodes: []*pb.EntityInfo{
							{
								Dcid: "country/USA",
								// The merger keeps the first encountered node by Dcid.
								Types: []string{"Thing"},
							},
						},
					},
				},
			},
		},
	}

	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}
	if diff := cmp.Diff(want, got, cmpOpts); diff != "" {
		t.Errorf("Federated Node() mismatch (-want +got):\n%s", diff)
	}
}

func TestPageSizeFromLimit(t *testing.T) {
	// Distinct default and max values, so a swapped branch fails the test.
	const defaultSize, maxSize = 100, 1000
	for _, tc := range []struct {
		desc     string
		limit    int32
		want     int
		wantCode codes.Code
	}{
		{desc: "unset uses default", limit: 0, want: defaultSize},
		{desc: "smallest page", limit: 1, want: 1},
		{desc: "between default and max is honored", limit: 500, want: 500},
		{desc: "max is honored", limit: maxSize, want: maxSize},
		{desc: "above max is lowered to max", limit: maxSize + 1, want: maxSize},
		{desc: "largest int32 is lowered to max", limit: math.MaxInt32, want: maxSize},
		{desc: "negative is rejected", limit: -1, wantCode: codes.InvalidArgument},
		{desc: "smallest int32 is rejected", limit: math.MinInt32, wantCode: codes.InvalidArgument},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := pageSizeFromLimit(tc.limit, defaultSize, maxSize)
			if gotCode := status.Code(err); gotCode != tc.wantCode {
				t.Fatalf("pageSizeFromLimit(%d) error code = %v, want %v (err: %v)", tc.limit, gotCode, tc.wantCode, err)
			}
			if got != tc.want {
				t.Errorf("pageSizeFromLimit(%d) = %d, want %d", tc.limit, got, tc.want)
			}
		})
	}
}

func TestNodePageSize(t *testing.T) {
	for _, tc := range []struct {
		limit int32
		want  int
	}{
		{limit: 0, want: DefaultPageSize},
		{limit: 10, want: 10},
		{limit: MaxPageSize + 1, want: MaxPageSize},
	} {
		got, err := NodePageSize(tc.limit)
		if err != nil {
			t.Fatalf("NodePageSize(%d) unexpected error: %v", tc.limit, err)
		}
		if got != tc.want {
			t.Errorf("NodePageSize(%d) = %d, want %d", tc.limit, got, tc.want)
		}
	}
	if _, err := NodePageSize(-1); status.Code(err) != codes.InvalidArgument {
		t.Errorf("NodePageSize(-1) error = %v, want code %v", err, codes.InvalidArgument)
	}
}
