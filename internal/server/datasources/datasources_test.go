package datasources

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/google/go-cmp/cmp"
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
