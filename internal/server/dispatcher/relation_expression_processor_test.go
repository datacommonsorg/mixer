// Copyright 2026 Google LLC
// ... standard license ...

package dispatcher

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			processor := NewRelationExpressionProcessor(remoteSource, 10000)

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

func sdmxContainedInPlaceComponentConstraint(ancestor, childPlaceType string) *sdmxpb.SdmxComponentConstraint {
	return &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			datacommons.PropertyContainedInPlace: {
				Predicates: []*sdmxpb.SdmxPredicate{{Value: ancestor}},
				Transitive: true,
			},
			datacommons.PropertyTypeOf: {Predicates: []*sdmxpb.SdmxPredicate{{Value: childPlaceType}}},
		},
	}
}

func sdmxDataQueryWithContainedInPlace(
	componentToContainedInPlace map[string]*sdmxpb.SdmxComponentConstraint,
) *sdmxpb.SdmxDataQuery {
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		datacommons.ComponentVariableMeasured: sdmxComponentConstraint("Count_Person"),
	}
	for component, constraint := range componentToContainedInPlace {
		constraints[component] = constraint
	}
	return &sdmxpb.SdmxDataQuery{Constraints: constraints}
}

func sdmxNodeResponse(subject string, dcids []string, nextToken string) *pbv2.NodeResponse {
	nodes := make([]*pb.EntityInfo, 0, len(dcids))
	for _, dcid := range dcids {
		nodes = append(nodes, &pb.EntityInfo{Dcid: dcid})
	}
	return &pbv2.NodeResponse{
		Data: map[string]*pbv2.LinkedGraph{
			subject: {
				Arcs: map[string]*pbv2.Nodes{
					"containedInPlace+": {Nodes: nodes},
				},
			},
		},
		NextToken: nextToken,
	}
}

func TestRelationExpressionProcessorPreProcessSdmxDataDeduplicatesRelationsAndPages(t *testing.T) {
	relation := datacommons.ContainedInPlaceConstraint{Ancestor: "northamerica", ChildPlaceType: "Country"}
	var calls atomic.Int32
	remoteSource := &mockSource{
		nodeFunc: func(_ context.Context, req *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
			calls.Add(1)
			if got, want := req.GetNodes(), []string{relation.Ancestor}; !slices.Equal(got, want) {
				return nil, fmt.Errorf("Node() nodes = %v, want %v", got, want)
			}
			if got, want := req.GetProperty(), "<-containedInPlace+{typeOf:Country}"; got != want {
				return nil, fmt.Errorf("Node() property = %q, want %q", got, want)
			}
			switch req.GetNextToken() {
			case "":
				return sdmxNodeResponse(relation.Ancestor, []string{"country/USA", "country/CAN"}, "next"), nil
			case "next":
				return sdmxNodeResponse(relation.Ancestor, []string{"country/CAN", "", "country/MEX"}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected next token %q", req.GetNextToken())
			}
		},
	}
	processor := NewRelationExpressionProcessor(remoteSource, 10)
	rc := &RequestContext{
		Context: context.Background(),
		Type:    TypeSdmxData,
		CurrentRequest: sdmxDataQueryWithContainedInPlace(map[string]*sdmxpb.SdmxComponentConstraint{
			"sourceCountry":      sdmxContainedInPlaceComponentConstraint(relation.Ancestor, relation.ChildPlaceType),
			"destinationCountry": sdmxContainedInPlaceComponentConstraint(relation.Ancestor, relation.ChildPlaceType),
		}),
	}

	outcome, err := processor.PreProcess(rc)
	if err != nil {
		t.Fatalf("PreProcess() error = %v", err)
	}
	if outcome != Continue {
		t.Fatalf("PreProcess() outcome = %v, want %v", outcome, Continue)
	}
	if got, want := calls.Load(), int32(2); got != want {
		t.Fatalf("Node() calls = %d, want %d pages for one unique relation", got, want)
	}
	want := SdmxContainedInPlaceToRemoteDCIDs{
		relation: {"country/CAN", "country/MEX", "country/USA"},
	}
	got := SdmxContainedInPlaceToRemoteDCIDsFromContext(rc.Context)
	if !slices.Equal(got[relation], want[relation]) || len(got) != len(want) {
		t.Fatalf("remote expansions = %v, want %v", got, want)
	}
}

func TestRelationExpressionProcessorPreProcessSdmxDataLimit(t *testing.T) {
	relation := datacommons.ContainedInPlaceConstraint{Ancestor: "northamerica", ChildPlaceType: "Country"}
	for _, tc := range []struct {
		name     string
		dcids    []string
		wantCode codes.Code
	}{
		{name: "exactly at limit", dcids: []string{"country/CAN", "country/USA"}, wantCode: codes.OK},
		{name: "duplicate does not exceed limit", dcids: []string{"country/CAN", "country/USA", "country/CAN"}, wantCode: codes.OK},
		{name: "unique value exceeds limit", dcids: []string{"country/CAN", "country/USA", "country/MEX"}, wantCode: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remoteSource := &mockSource{
				nodeFunc: func(_ context.Context, _ *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
					return sdmxNodeResponse(relation.Ancestor, tc.dcids, ""), nil
				},
			}
			processor := NewRelationExpressionProcessor(remoteSource, 2)
			rc := &RequestContext{
				Context: context.Background(),
				Type:    TypeSdmxData,
				CurrentRequest: sdmxDataQueryWithContainedInPlace(map[string]*sdmxpb.SdmxComponentConstraint{
					"sourceCountry": sdmxContainedInPlaceComponentConstraint(relation.Ancestor, relation.ChildPlaceType),
				}),
			}

			_, err := processor.PreProcess(rc)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("PreProcess() code = %v, want %v; err = %v", got, tc.wantCode, err)
			}
			if tc.wantCode == codes.InvalidArgument {
				if !strings.Contains(status.Convert(err).Message(), "choose a narrower ancestor or child place type") {
					t.Fatalf("PreProcess() message = %q, want narrowing guidance", status.Convert(err).Message())
				}
				if got := SdmxContainedInPlaceToRemoteDCIDsFromContext(rc.Context); got != nil {
					t.Fatalf("remote expansions = %v, want nil after limit error", got)
				}
			}
		})
	}
}

func TestRelationExpressionProcessorPreProcessSdmxDataPreservesPartialRemoteSuccess(t *testing.T) {
	success := datacommons.ContainedInPlaceConstraint{Ancestor: "northamerica", ChildPlaceType: "Country"}
	failure := datacommons.ContainedInPlaceConstraint{Ancestor: "europe", ChildPlaceType: "Country"}
	remoteSource := &mockSource{
		nodeFunc: func(_ context.Context, req *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
			if req.GetNodes()[0] == failure.Ancestor {
				return nil, fmt.Errorf("remote failed")
			}
			return sdmxNodeResponse(success.Ancestor, []string{"country/USA"}, ""), nil
		},
	}
	processor := NewRelationExpressionProcessor(remoteSource, 10)
	rc := &RequestContext{
		Context: context.Background(),
		Type:    TypeSdmxData,
		CurrentRequest: sdmxDataQueryWithContainedInPlace(map[string]*sdmxpb.SdmxComponentConstraint{
			"sourceCountry":      sdmxContainedInPlaceComponentConstraint(success.Ancestor, success.ChildPlaceType),
			"destinationCountry": sdmxContainedInPlaceComponentConstraint(failure.Ancestor, failure.ChildPlaceType),
		}),
	}

	if _, err := processor.PreProcess(rc); err != nil {
		t.Fatalf("PreProcess() error = %v", err)
	}
	got := SdmxContainedInPlaceToRemoteDCIDsFromContext(rc.Context)
	if len(got) != 1 || !slices.Equal(got[success], []string{"country/USA"}) {
		t.Fatalf("remote expansions = %v, want only successful relation", got)
	}
	if _, ok := got[failure]; ok {
		t.Fatalf("remote expansions include failed relation: %v", got)
	}
}

func TestRelationExpressionProcessorPreProcessSdmxDataLimitsConcurrency(t *testing.T) {
	componentToContainedInPlace := map[string]*sdmxpb.SdmxComponentConstraint{}
	for i := range 4 {
		componentToContainedInPlace[fmt.Sprintf("place%d", i)] = sdmxContainedInPlaceComponentConstraint(fmt.Sprintf("ancestor%d", i), "Country")
	}

	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	remoteSource := &mockSource{
		nodeFunc: func(_ context.Context, req *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				maximum := maxActive.Load()
				if current <= maximum || maxActive.CompareAndSwap(maximum, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			return sdmxNodeResponse(req.GetNodes()[0], nil, ""), nil
		},
	}
	processor := NewRelationExpressionProcessor(remoteSource, 10)
	rc := &RequestContext{
		Context:        context.Background(),
		Type:           TypeSdmxData,
		CurrentRequest: sdmxDataQueryWithContainedInPlace(componentToContainedInPlace),
	}

	done := make(chan error, 1)
	go func() {
		_, err := processor.PreProcess(rc)
		done <- err
	}()
	for range maxConcurrentSdmxContainedInPlaceExpansions {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent remote expansions")
		}
	}
	select {
	case <-started:
		t.Fatal("started more than three remote expansions concurrently")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PreProcess() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote expansions to finish")
	}
	if got, want := maxActive.Load(), int32(maxConcurrentSdmxContainedInPlaceExpansions); got != want {
		t.Fatalf("maximum concurrent Node calls = %d, want %d", got, want)
	}
}

func TestRelationExpressionProcessorPreProcessSdmxDataStopsQueuedExpansionsAfterLimit(t *testing.T) {
	componentToContainedInPlace := map[string]*sdmxpb.SdmxComponentConstraint{}
	for i := range 4 {
		componentToContainedInPlace[fmt.Sprintf("place%d", i)] = sdmxContainedInPlaceComponentConstraint(fmt.Sprintf("ancestor%d", i), "Country")
	}

	started := make(chan string, maxConcurrentSdmxContainedInPlaceExpansions)
	releaseLimit := make(chan struct{})
	var queuedRelationCalled atomic.Bool
	remoteSource := &mockSource{
		nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, _ int) (*pbv2.NodeResponse, error) {
			ancestor := req.GetNodes()[0]
			if ancestor == "ancestor3" {
				queuedRelationCalled.Store(true)
				return sdmxNodeResponse(ancestor, nil, ""), nil
			}
			started <- ancestor
			if ancestor == "ancestor0" {
				<-releaseLimit
				return sdmxNodeResponse(ancestor, []string{"place1", "place2", "place3"}, ""), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	processor := NewRelationExpressionProcessor(remoteSource, 2)
	rc := &RequestContext{
		Context:        context.Background(),
		Type:           TypeSdmxData,
		CurrentRequest: sdmxDataQueryWithContainedInPlace(componentToContainedInPlace),
	}

	done := make(chan error, 1)
	go func() {
		_, err := processor.PreProcess(rc)
		done <- err
	}()
	for range maxConcurrentSdmxContainedInPlaceExpansions {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for initial remote expansions")
		}
	}
	close(releaseLimit)

	select {
	case err := <-done:
		if got, want := status.Code(err), codes.InvalidArgument; got != want {
			t.Fatalf("PreProcess() code = %v, want %v; err = %v", got, want, err)
		}
		if !strings.Contains(status.Convert(err).Message(), "choose a narrower ancestor or child place type") {
			t.Fatalf("PreProcess() message = %q, want narrowing guidance", status.Convert(err).Message())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote expansions to stop")
	}
	if queuedRelationCalled.Load() {
		t.Fatal("queued relation called Node() after expansion limit canceled the group")
	}
}
