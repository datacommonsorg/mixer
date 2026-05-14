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
	nodeFunc func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error)
}

func (m *mockDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	return m.nodeFunc(ctx, req, pageSize)
}

func TestNodeFetchAll(t *testing.T) {
	ctx := context.Background()

	// Test: Single page response.
	// Situation: The data source returns a response with no NextToken.
	// Expectation: The helper returns the response directly without further calls.
	t.Run("SinglePage", func(t *testing.T) {
		mockDS := &mockDataSource{
			nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
				return &pbv2.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"geoId/06": {
							Arcs: map[string]*pbv2.Nodes{
								"containedInPlace": {
									Nodes: []*pb.EntityInfo{{Dcid: "geoId/06001"}},
								},
							},
						},
					},
				}, nil
			},
		}

		req := &pbv2.NodeRequest{Nodes: []string{"geoId/06"}}
		resp, err := NodeFetchAll(ctx, mockDS, req, 10)
		if err != nil {
			t.Fatalf("NodeFetchAll failed: %v", err)
		}

		expected := &pbv2.NodeResponse{
			Data: map[string]*pbv2.LinkedGraph{
				"geoId/06": {
					Arcs: map[string]*pbv2.Nodes{
						"containedInPlace": {
							Nodes: []*pb.EntityInfo{{Dcid: "geoId/06001"}},
						},
					},
				},
			},
		}

		if diff := cmp.Diff(expected, resp, protocmp.Transform()); diff != "" {
			t.Errorf("NodeFetchAll mismatch (-want +got):\n%s", diff)
		}
	})

	// Test: Multi-page response.
	// Situation: The data source returns 3 pages of data, linked by NextToken.
	// Expectation: The helper loops 3 times, fetches all pages, and merges the nodes correctly.
	t.Run("MultiPage", func(t *testing.T) {
		calls := 0
		mockDS := &mockDataSource{
			nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
				calls++
				switch calls {
				case 1:
					return &pbv2.NodeResponse{
						Data: map[string]*pbv2.LinkedGraph{
							"geoId/06": {
								Arcs: map[string]*pbv2.Nodes{
									"containedInPlace": {
										Nodes: []*pb.EntityInfo{{Dcid: "geoId/06001"}},
									},
								},
							},
						},
						NextToken: "token1",
					}, nil
				case 2:
					if req.NextToken != "token1" {
						return nil, errors.New("unexpected nextToken")
					}
					return &pbv2.NodeResponse{
						Data: map[string]*pbv2.LinkedGraph{
							"geoId/06": {
								Arcs: map[string]*pbv2.Nodes{
									"containedInPlace": {
										Nodes: []*pb.EntityInfo{{Dcid: "geoId/06002"}},
									},
								},
							},
						},
						NextToken: "token2",
					}, nil
				case 3:
					if req.NextToken != "token2" {
						return nil, errors.New("unexpected nextToken")
					}
					return &pbv2.NodeResponse{
						Data: map[string]*pbv2.LinkedGraph{
							"geoId/06": {
								Arcs: map[string]*pbv2.Nodes{
									"containedInPlace": {
										Nodes: []*pb.EntityInfo{{Dcid: "geoId/06003"}},
									},
								},
							},
						},
					}, nil
				default:
					return nil, errors.New("too many calls")
				}
			},
		}

		req := &pbv2.NodeRequest{Nodes: []string{"geoId/06"}}
		resp, err := NodeFetchAll(ctx, mockDS, req, 10)
		if err != nil {
			t.Fatalf("NodeFetchAll failed: %v", err)
		}

		expected := &pbv2.NodeResponse{
			Data: map[string]*pbv2.LinkedGraph{
				"geoId/06": {
					Arcs: map[string]*pbv2.Nodes{
						"containedInPlace": {
							Nodes: []*pb.EntityInfo{
								{Dcid: "geoId/06001"},
								{Dcid: "geoId/06002"},
								{Dcid: "geoId/06003"},
							},
						},
					},
				},
			},
		}

		if diff := cmp.Diff(expected, resp, protocmp.Transform()); diff != "" {
			t.Errorf("NodeFetchAll mismatch (-want +got):\n%s", diff)
		}
		if calls != 3 {
			t.Errorf("Expected 3 calls, got %d", calls)
		}
	})

	// Test: Error on initial call.
	// Situation: The data source fails on the very first call.
	// Expectation: The helper immediately returns the error.
	t.Run("InitialError", func(t *testing.T) {
		mockDS := &mockDataSource{
			nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
				return nil, errors.New("initial error")
			},
		}

		req := &pbv2.NodeRequest{Nodes: []string{"geoId/06"}}
		_, err := NodeFetchAll(ctx, mockDS, req, 10)
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
		if err.Error() != "initial error" {
			t.Errorf("Expected 'initial error', got '%v'", err)
		}
	})

	// Test: Error on subsequent call.
	// Situation: The data source succeeds on the first call but fails on the second.
	// Expectation: The helper returns the error encountered on the second call.
	t.Run("SubsequentError", func(t *testing.T) {
		calls := 0
		mockDS := &mockDataSource{
			nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
				calls++
				if calls == 1 {
					return &pbv2.NodeResponse{
						Data: map[string]*pbv2.LinkedGraph{
							"geoId/06": {
								Arcs: map[string]*pbv2.Nodes{
									"containedInPlace": {
										Nodes: []*pb.EntityInfo{{Dcid: "geoId/06001"}},
									},
								},
							},
						},
						NextToken: "token1",
					}, nil
				}
				return nil, errors.New("subsequent error")
			},
		}

		req := &pbv2.NodeRequest{Nodes: []string{"geoId/06"}}
		_, err := NodeFetchAll(ctx, mockDS, req, 10)
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
		if err.Error() != "subsequent error" {
			t.Errorf("Expected 'subsequent error', got '%v'", err)
		}
	})
}
