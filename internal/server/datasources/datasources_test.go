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

package datasources

import (
	"context"
	"slices"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

// mockSource implements datasource.DataSource for testing.
type mockSource struct {
	datasource.DataSource
	id              string
	sourceType      datasource.DataSourceType
	nodeFunc        func(context.Context, *pbv2.NodeRequest, int) (*pbv2.NodeResponse, error)
	observationFunc func(context.Context, *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error)
}

func (m *mockSource) Id() string { return m.id }
func (m *mockSource) Type() datasource.DataSourceType { return m.sourceType }
func (m *mockSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	if m.nodeFunc != nil {
		return m.nodeFunc(ctx, req, pageSize)
	}
	return &pbv2.NodeResponse{}, nil
}
func (m *mockSource) Observation(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	if m.observationFunc != nil {
		return m.observationFunc(ctx, req)
	}
	return &pbv2.ObservationResponse{}, nil
}

// Test main path: Expression + Remote Mixer
// Verify that Spanner receives resolved DCIDs and Remote receives the original expression.
func TestObservation_ExpressionExpansion(t *testing.T) {
	ctx := context.Background()

	var spannerReceivedReq *pbv2.ObservationRequest
	mockSpanner := &mockSource{
		id:         "mock-spanner",
		sourceType: datasource.TypeSpanner,
		observationFunc: func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			spannerReceivedReq = req
			return &pbv2.ObservationResponse{}, nil
		},
	}

	var remoteReceivedReq *pbv2.ObservationRequest
	mockRemote := &mockSource{
		id:         "mock-remote",
		sourceType: datasource.TypeRemote,
		nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
			return &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"geoId/06": {
						Arcs: map[string]*pbv2.Nodes{
							"<-containedInPlace+{typeOf:County}": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "geoId/06001"},
								},
							},
						},
					},
				},
			}, nil
		},
		observationFunc: func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			remoteReceivedReq = req
			return &pbv2.ObservationResponse{}, nil
		},
	}

	ds := NewDataSources([]datasource.DataSource{mockSpanner, mockRemote}, mockRemote)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity"},
	}

	_, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	// Verify Spanner received resolved DCIDs
	if spannerReceivedReq == nil {
		t.Fatal("Spanner did not receive request")
	}
	if spannerReceivedReq.Entity.Expression != "" {
		t.Errorf("Spanner received expression, expected empty: %s", spannerReceivedReq.Entity.Expression)
	}
	if !slices.Contains(spannerReceivedReq.Entity.Dcids, "geoId/06001") {
		t.Errorf("Spanner did not receive resolved DCID: %v", spannerReceivedReq.Entity.Dcids)
	}

	// Verify Remote received original expression
	if remoteReceivedReq == nil {
		t.Fatal("Remote did not receive request")
	}
	if remoteReceivedReq.Entity.Expression != "geoId/06<-containedInPlace+{typeOf:County}" {
		t.Errorf("Remote received unexpected expression: %s", remoteReceivedReq.Entity.Expression)
	}
	if len(remoteReceivedReq.Entity.Dcids) != 0 {
		t.Errorf("Remote received DCIDs, expected none: %v", remoteReceivedReq.Entity.Dcids)
	}
}

// Test path: Expression + No Remote Mixer
// Verify that Spanner receives the expression (one-shot) and no resolution step is taken.
func TestObservation_ExpressionNoRemote(t *testing.T) {
	ctx := context.Background()

	var spannerReceivedReq *pbv2.ObservationRequest
	mockSpanner := &mockSource{
		id:         "mock-spanner",
		sourceType: datasource.TypeSpanner,
		observationFunc: func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			spannerReceivedReq = req
			return &pbv2.ObservationResponse{}, nil
		},
	}

	ds := NewDataSources([]datasource.DataSource{mockSpanner}, nil)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity"},
	}

	_, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if spannerReceivedReq == nil {
		t.Fatal("Spanner did not receive request")
	}
	if spannerReceivedReq.Entity.Expression != "geoId/06<-containedInPlace+{typeOf:County}" {
		t.Errorf("Spanner received unexpected expression: %s", spannerReceivedReq.Entity.Expression)
	}
}

// Test edge case: Expression Resolution Returns Empty
// Verify that if ds.Node returns no entities, we return an empty ObservationResponse immediately without calling Spanner.
func TestObservation_ExpressionEmptyResolution(t *testing.T) {
	ctx := context.Background()

	mockSpanner := &mockSource{
		id:         "mock-spanner",
		sourceType: datasource.TypeSpanner,
		observationFunc: func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			t.Fatal("Spanner should not be called when resolution returns empty")
			return &pbv2.ObservationResponse{}, nil
		},
	}

	mockRemote := &mockSource{
		id:         "mock-remote",
		sourceType: datasource.TypeRemote,
		nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
			return &pbv2.NodeResponse{}, nil // Empty response
		},
	}

	ds := NewDataSources([]datasource.DataSource{mockSpanner, mockRemote}, mockRemote)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity"},
	}

	resp, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Expected non-nil response")
	}
	if len(resp.ByVariable) != 0 {
		t.Errorf("Expected empty response, got: %v", resp.ByVariable)
	}
}

// Test path: Expression + Remote Mixer + Non-Spanner Local Source
// Verify that non-remote sources receive resolved DCIDs.
func TestObservation_ExpressionExpansion_NonSpanner(t *testing.T) {
	ctx := context.Background()

	var remoteReceivedReq *pbv2.ObservationRequest
	mockRemote := &mockSource{
		id:         "mock-remote",
		sourceType: datasource.TypeRemote,
		nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
			return &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"geoId/06": {
						Arcs: map[string]*pbv2.Nodes{
							"<-containedInPlace+{typeOf:County}": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "geoId/06001"},
								},
							},
						},
					},
				},
			}, nil
		},
		observationFunc: func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			remoteReceivedReq = req
			return &pbv2.ObservationResponse{}, nil
		},
	}

	var sqlReceivedReq *pbv2.ObservationRequest
	mockSQL := &mockSource{
		id:         "mock-sql",
		sourceType: datasource.TypeSQL,
		observationFunc: func(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
			sqlReceivedReq = req
			return &pbv2.ObservationResponse{}, nil
		},
	}

	ds := NewDataSources([]datasource.DataSource{mockSQL, mockRemote}, mockRemote)

	req := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Select:   []string{"variable", "entity"},
	}

	_, err := ds.Observation(ctx, req)
	if err != nil {
		t.Fatalf("Observation failed: %v", err)
	}

	// Verify SQL received resolved DCIDs
	if sqlReceivedReq == nil {
		t.Fatal("SQL did not receive request")
	}
	if sqlReceivedReq.Entity.Expression != "" {
		t.Errorf("SQL received expression, expected empty: %s", sqlReceivedReq.Entity.Expression)
	}
	if !slices.Contains(sqlReceivedReq.Entity.Dcids, "geoId/06001") {
		t.Errorf("SQL did not receive resolved DCID: %v", sqlReceivedReq.Entity.Dcids)
	}

	// Verify Remote received original expression
	if remoteReceivedReq == nil {
		t.Fatal("Remote did not receive request")
	}
	if remoteReceivedReq.Entity.Expression != "geoId/06<-containedInPlace+{typeOf:County}" {
		t.Errorf("Remote received unexpected expression: %s", remoteReceivedReq.Entity.Expression)
	}
}
