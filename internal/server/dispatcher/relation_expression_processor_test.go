// Copyright 2026 Google LLC
// ... standard license ...

package dispatcher

import (
	"context"
	"fmt"
	"slices"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"

	"google.golang.org/protobuf/proto"
)

type mockSource struct {
	datasource.DataSource // Embed to avoid implementing all methods
	id                    string
	sourceType            datasource.DataSourceType
	nodeFunc              func(context.Context, *pbv2.NodeRequest, int) (*pbv2.NodeResponse, error)
}

func (m *mockSource) Type() datasource.DataSourceType { return m.sourceType }
func (m *mockSource) Id() string                      { return m.id }
func (m *mockSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	if m.nodeFunc != nil {
		return m.nodeFunc(ctx, req, pageSize)
	}
	return &pbv2.NodeResponse{}, nil
}

func TestRelationExpressionProcessor_PreProcess(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name               string
		requestType        RequestType
		initialRequest     proto.Message
		hasRemoteMixer     bool
		mockNodeResponse   *pbv2.NodeResponse
		mockNodeError      error
		expectedOutcome    Outcome
		expectedErr        bool
		expectNodeCalled   bool
		expectedContextKey bool
		expectedDCIDs      []string
	}{
		// Test: Happy Path.
		// Situation: An observation request with an expression is processed with a source available.
		// Expected Outcome: The processor should parse the expression, call the source, and store the resolved DCIDs in the context.
		{
			name:        "Happy Path: Expression + Remote Mixer",
			requestType: TypeObservation,
			initialRequest: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
			},
			hasRemoteMixer: true,
			mockNodeResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"geoId/06": {
						Arcs: map[string]*pbv2.Nodes{
							"containedInPlace+": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "geoId/06001"},
									{Dcid: "geoId/06003"},
								},
							},
						},
					},
				},
			},
			expectedOutcome:    Continue,
			expectNodeCalled:   true,
			expectedContextKey: true,
			expectedDCIDs:      []string{"geoId/06001", "geoId/06003"},
		},
		// Test: No-op for non-observation requests.
		// Situation: A request of type Node (not Observation) is passed to the processor.
		// Expected Outcome: The processor should ignore the request and return Continue without calling the source or setting context.
		{
			name:        "No-op: Non-observation request",
			requestType: TypeNode,
			initialRequest: &pbv2.NodeRequest{
				Nodes:    []string{"geoId/06"},
				Property: "<-containedInPlace",
			},
			hasRemoteMixer:     true,
			expectedOutcome:    Continue,
			expectNodeCalled:   false,
			expectedContextKey: false,
		},
		// Test: No-op for standard observation requests.
		// Situation: An observation request without an expression (only DCIDs) is processed.
		// Expected Outcome: The processor should ignore the request and return Continue without calling the source or setting context.
		{
			name:        "No-op: Observation without expression",
			requestType: TypeObservation,
			initialRequest: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Dcids: []string{"geoId/06001"}},
			},
			hasRemoteMixer:     true,
			expectedOutcome:    Continue,
			expectNodeCalled:   false,
			expectedContextKey: false,
		},
		// Test: No-op when no source is configured.
		// Situation: An observation request with an expression is processed, but the processor has no source configured.
		// Expected Outcome: The processor should ignore the request and return Continue without setting context.
		{
			name:        "No-op: No Remote Mixer",
			requestType: TypeObservation,
			initialRequest: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
			},
			hasRemoteMixer:     false,
			expectedOutcome:    Continue,
			expectNodeCalled:   false,
			expectedContextKey: false,
		},
		// Test: Fallback on failure.
		// Situation: The call to the source fails with an error.
		// Expected Outcome: The processor should log a warning but return Continue without setting context, allowing fallback to local execution.
		{
			name:        "Fallback: Remote call fails, proceed without expansion",
			requestType: TypeObservation,
			initialRequest: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
			},
			hasRemoteMixer:     true,
			mockNodeError:      fmt.Errorf("remote failed"),
			expectedErr:        false,
			expectedOutcome:    Continue,
			expectNodeCalled:   true,
			expectedContextKey: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeCalled := false
			var capturedReq *pbv2.NodeRequest

			mockRemote := &mockSource{
				id:         "mock-remote",
				sourceType: datasource.TypeRemote,
				nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
					nodeCalled = true
					capturedReq = req
					return test.mockNodeResponse, test.mockNodeError
				},
			}

			var remoteSource datasource.DataSource
			if test.hasRemoteMixer {
				remoteSource = mockRemote
			}
			processor := NewRelationExpressionProcessor(remoteSource)

			rc := &RequestContext{
				Context:        ctx,
				Type:           test.requestType,
				CurrentRequest: test.initialRequest,
			}

			outcome, err := processor.PreProcess(rc)

			if (err != nil) != test.expectedErr {
				t.Fatalf("PreProcess() error = %v, expectedErr %v", err, test.expectedErr)
			}

			if outcome != test.expectedOutcome {
				t.Errorf("PreProcess() outcome = %v, expected %v", outcome, test.expectedOutcome)
			}

			if nodeCalled != test.expectNodeCalled {
				t.Errorf("Node called = %v, expected %v", nodeCalled, test.expectNodeCalled)
			}

			if nodeCalled && test.expectNodeCalled {
				expectedProp := "<-containedInPlace+{typeOf:County}"
				if capturedReq.Property != expectedProp {
					t.Errorf("NodeRequest property = %v, want %v", capturedReq.Property, expectedProp)
				}
				expectedNodes := []string{"geoId/06"}
				if !slices.Equal(capturedReq.Nodes, expectedNodes) {
					t.Errorf("NodeRequest nodes = %v, want %v", capturedReq.Nodes, expectedNodes)
				}
			}

			if test.expectedContextKey {
				val := rc.Value(RelationExpressionExpandedEntities)
				if val == nil {
					t.Errorf("Expected context key %v to be set", RelationExpressionExpandedEntities)
				} else {
					dcids, ok := val.([]string)
					if !ok {
						t.Errorf("Context value is not []string: %T", val)
					} else if !slices.Equal(dcids, test.expectedDCIDs) {
						t.Errorf("Context DCIDs = %v, want %v", dcids, test.expectedDCIDs)
					}
				}
			} else {
				val := rc.Value(RelationExpressionExpandedEntities)
				if val != nil {
					t.Errorf("Expected context key %v to NOT be set, got %v", RelationExpressionExpandedEntities, val)
				}
			}
		})
	}
}
