// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	"github.com/datacommonsorg/mixer/internal/metrics"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Initializes an in-memory metric reader for testing.
func setupMetricReader(t *testing.T) *metric.ManualReader {
	t.Helper()
	// The metrics package uses a sync.Once to initialize its global instruments.
	// This needs to be reset for each test to ensure that the instruments are
	// registered with the new meter provider created for each test.
	metrics.ResetForTest()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(provider)
	return reader
}

// setUpSlogCapture sets up a test-specific slog logger that writes to a buffer.
// It returns the buffer and a cleanup function to restore the original logger.
func setUpSlogCapture() (*bytes.Buffer, func()) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)
	logger := slog.New(handler)
	originalLogger := slog.Default()
	slog.SetDefault(logger)
	return &buf, func() { slog.SetDefault(originalLogger) }
}

func TestMaybeMirrorV3_Percentage(t *testing.T) {
	ctx := context.Background()
	req := &pbv2.NodeRequest{
		Nodes: []string{"test"},
	}
	resp := &pbv2.NodeResponse{}

	for _, tc := range []struct {
		name           string
		mirrorFraction float64
		shouldMirror   bool
	}{
		{"0 percent", 0, false},
		{"100 percent", 1.0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{
				flags: &featureflags.Flags{
					V3MirrorFraction: tc.mirrorFraction,
				},
			}
			mirrorCallCount := 0
			var mirroredReqs []proto.Message
			skipCacheHeaderValues := make(chan bool, 2)
			var mu sync.Mutex

			v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
				mu.Lock()
				defer mu.Unlock()
				mirroredReqs = append(mirroredReqs, req)
				md, _ := metadata.FromIncomingContext(ctx)
				v := md.Get(string(util.XSkipCache))
				skipCache := len(v) > 0 && v[0] == "true"
				skipCacheHeaderValues <- skipCache
				mirrorCallCount++
				return &pbv2.NodeResponse{}, nil
			}

			var mirrorWg sync.WaitGroup
			s.maybeMirrorV3(ctx, req, resp, 0, v3Call, GetV2NodeCmpOpts(), &mirrorWg)
			mirrorWg.Wait()

			if tc.shouldMirror {
				if mirrorCallCount != 2 {
					t.Errorf("expected 2 mirror calls, but got %d", mirrorCallCount)
				}
				if !proto.Equal(req, mirroredReqs[0]) || !proto.Equal(req, mirroredReqs[1]) {
					t.Errorf("mirrored request was not equal to the original request")
				}
				if <-skipCacheHeaderValues {
					t.Errorf("expected the first call to allow cache usage")
				}
				if !<-skipCacheHeaderValues {
					t.Errorf("expected the second call to skip the cache")
				}
			} else {
				// Give the goroutine a chance to run if it was incorrectly started.
				time.Sleep(100 * time.Millisecond)
				if mirrorCallCount != 0 {
					t.Errorf("expected no mirror call, but it was called")
				}
			}
		})
	}
}

func TestMaybeMirrorV3_IgnoreSubsequentPages(t *testing.T) {
	ctx := context.Background()
	s := &Server{
		flags: &featureflags.Flags{
			V3MirrorFraction: 1.0, // Mirroring is on
		},
	}
	req := &pbv2.NodeRequest{NextToken: "some_token"}
	resp := &pbv2.NodeResponse{}

	mirrorCallCount := 0
	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		mirrorCallCount++
		return &pbv2.NodeResponse{}, nil
	}

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, req, resp, 0, v3Call, GetV2NodeCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	if mirrorCallCount > 0 {
		t.Errorf("mirroring should only include the first page of paginated requests")
	}
}

func TestMaybeMirrorV3_LatencyMetric(t *testing.T) {
	ctx := context.Background()
	s := &Server{
		flags: &featureflags.Flags{
			V3MirrorFraction: 1.0, // Mirroring is on
		},
	}
	reader := setupMetricReader(t)
	req := &pbv2.NodeRequest{}
	resp := &pbv2.NodeResponse{}

	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return &pbv2.NodeResponse{}, nil
	}

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, req, resp, 0, v3Call, GetV2NodeCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "datacommons.mixer.v3_latency_diff" {
				found = true
				hist, ok := m.Data.(metricdata.Histogram[int64])
				if !ok {
					t.Fatalf("metric is not a histogram")
				}
				if len(hist.DataPoints) != 2 {
					t.Fatalf("expected 2 datapoints for latency, got %d", len(hist.DataPoints))
				}
				for _, dp := range hist.DataPoints {
					foundAttr := false
					for _, attr := range dp.Attributes.ToSlice() {
						if attr.Key == "rpc.headers.skip_cache" {
							foundAttr = true
							break
						}
					}
					if !foundAttr {
						t.Error("latency metric missing 'rpc.headers.skip_cache' attribute")
					}
				}
				break
			}
		}
	}
	if !found {
		t.Error("datacommons.mixer.v3_latency_diff metric not found")
	}
}

func TestMaybeMirrorV3_ObservationResponseMismatch(t *testing.T) {
	ctx := context.Background()
	s := &Server{v3MirrorFraction: 1.0} // Mirroring is on
	reader := setupMetricReader(t)

	facet1 := &pb.Facet{
		ImportName:        "test-import1",
		MeasurementMethod: "test-method1",
	}
	facet2 := &pb.Facet{
		ImportName:        "test-import2",
		MeasurementMethod: "test-method2",
	}
	v2Req := &pbv2.ObservationRequest{}
	v2Resp := &pbv2.ObservationResponse{
		Facets: map[string]*pb.Facet{
			"v2_facet_id_1": facet1,
			"v2_facet_id_2": facet2,
		},
	}
	v3Resp := &pbv2.ObservationResponse{
		Facets: map[string]*pb.Facet{
			"v3_facet_id_2": facet1,
			// facet2 is not included, which is an actual diff.
			"v3_facet_id_1": facet1,
		},
	}

	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return v3Resp, nil
	}

	buf, cleanup := setUpSlogCapture()
	defer cleanup()

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, v2Req, v2Resp, 0, v3Call, GetV2ObservationCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	logOutput := buf.String()
	if strings.Count(logOutput, "V3 mirrored call had a different response") != 2 {
		t.Errorf("log output should contain 2 diff warnings, but got: %q", logOutput)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	mismatchCount := int64(0)
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "datacommons.mixer.v3_response_mismatches" {
				found = true
				sum, _ := m.Data.(metricdata.Sum[int64])
				if len(sum.DataPoints) == 1 {
					mismatchCount = sum.DataPoints[0].Value
				}
				break
			}
		}
	}

	if !found {
		t.Error("datacommons.mixer.v3_response_mismatches metric not found")
	}
	if mismatchCount != 2 {
		t.Errorf("mismatch count: got %d, want 2", mismatchCount)
	}
}

func TestMaybeMirrorV3_NodeResponseMismatch(t *testing.T) {
	ctx := context.Background()
	s := &Server{
		flags: &featureflags.Flags{
			V3MirrorFraction: 1.0, // Mirroring is on
		},
	}
	reader := setupMetricReader(t)

	v2Req := &pbv2.NodeRequest{Nodes: []string{"test"}}
	v2Resp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"test": {}}}
	v3Resp := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{"test_diff": {}}}

	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return v3Resp, nil
	}

	buf, cleanup := setUpSlogCapture()
	defer cleanup()

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, v2Req, v2Resp, 0, v3Call, GetV2NodeCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	logOutput := buf.String()
	if strings.Count(logOutput, "V3 mirrored call had a different response") != 2 {
		t.Errorf("log output should contain 2 diff warnings, but got: %q", logOutput)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	mismatchCount := int64(0)
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "datacommons.mixer.v3_response_mismatches" {
				found = true
				sum, _ := m.Data.(metricdata.Sum[int64])
				if len(sum.DataPoints) == 1 {
					mismatchCount = sum.DataPoints[0].Value
				}
				break
			}
		}
	}

	if !found {
		t.Error("datacommons.mixer.v3_response_mismatches metric not found")
	}
	if mismatchCount != 2 {
		t.Errorf("mismatch count: got %d, want 2", mismatchCount)
	}
}

func TestMaybeMirrorV3_ResponseMatch(t *testing.T) {
	ctx := context.Background()
	s := &Server{
		flags: &featureflags.Flags{
			V3MirrorFraction: 1.0, // Mirroring is on
		},
	}
	reader := setupMetricReader(t)

	v2Req := &pbv2.ResolveRequest{
		Nodes:    []string{"test_node"},
		Property: "<-prop1->prop2",
	}
	v2Resp := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node: "test_node",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{
						Dcid: "resolved_id_1",
					},
				},
			},
		},
	}
	// v3 response is identical to v2
	v3Resp := proto.Clone(v2Resp).(*pbv2.ResolveResponse)

	// A WaitGroup is needed because the V3 call is made in a separate goroutine
	// to avoid blocking the original V2 response.
	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return v3Resp, nil
	}

	buf, cleanup := setUpSlogCapture()
	defer cleanup()

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, v2Req, v2Resp, 0, v3Call, GetV2ResolveCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	logOutput := buf.String()
	if logOutput != "" {
		t.Errorf("log output should be empty when responses match, but got %q", logOutput)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	mismatchCount := int64(0)
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "datacommons.mixer.v3_response_mismatches" {
				found = true
				sum, _ := m.Data.(metricdata.Sum[int64])
				if len(sum.DataPoints) == 1 {
					mismatchCount = sum.DataPoints[0].Value
				}
				break
			}
		}
	}

	// Mismatch metric should not be recorded if responses match.
	if found && mismatchCount != 0 {
		t.Errorf("mismatch count: got %d, want 0", mismatchCount)
	}
}

func TestMaybeMirrorV3_V3Error(t *testing.T) {
	ctx := context.Background()
	s := &Server{
		flags: &featureflags.Flags{
			V3MirrorFraction: 1.0, // Mirroring is on
		},
	}
	reader := setupMetricReader(t)

	v2Req := &pbv2.NodeRequest{Nodes: []string{"test"}}
	v2Resp := &pbv2.NodeResponse{}

	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return nil, status.Error(codes.Internal, "V3 API error")
	}

	buf, cleanup := setUpSlogCapture()
	defer cleanup()

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, v2Req, v2Resp, 0, v3Call, GetV2NodeCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	logOutput := buf.String()
	if strings.Count(logOutput, "V3 mirrored call failed") != 2 {
		t.Errorf("log output should contain 2 error warnings, but got: %q", logOutput)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	errorCount := int64(0)
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "datacommons.mixer.v3_mirror_errors" {
				found = true
				sum, _ := m.Data.(metricdata.Sum[int64])
				if len(sum.DataPoints) == 1 {
					dp := sum.DataPoints[0]
					errorCount = dp.Value
					hasCodeAttr := false
					for _, attr := range dp.Attributes.ToSlice() {
						if attr.Key == "rpc.grpc.status_code" && attr.Value.AsString() == codes.Internal.String() {
							hasCodeAttr = true
							break
						}
					}
					if !hasCodeAttr {
						t.Errorf("metric missing 'rpc.grpc.status_code' attribute with value %s", codes.Internal.String())
					}
				}
				break
			}
		}
	}

	if !found {
		t.Error("datacommons.mixer.v3_mirror_errors metric not found")
	}
	if errorCount != 2 {
		t.Errorf("error count: got %d, want 2", errorCount)
	}
}

func TestMaybeMirrorV3_NodeIgnoresNextTokenMismatch(t *testing.T) {
	ctx := context.Background()
	s := &Server{v3MirrorFraction: 1.0} // Mirroring is on

	v2Req := &pbv2.NodeRequest{Nodes: []string{"test"}}
	v2Resp := &pbv2.NodeResponse{
		Data:      map[string]*pbv2.LinkedGraph{"test": {}},
		NextToken: "v2_token",
	}
	v3Resp := &pbv2.NodeResponse{
		Data:      map[string]*pbv2.LinkedGraph{"test": {}},
		NextToken: "v3_token",
	}

	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return v3Resp, nil
	}

	buf, cleanup := setUpSlogCapture()
	defer cleanup()

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, v2Req, v2Resp, 0, v3Call, GetV2NodeCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	logOutput := buf.String()
	if logOutput != "" {
		t.Errorf("log output should be empty when only next_token differs, but got %q", logOutput)
	}
}

func TestMaybeMirrorV3_ObservationIgnoresFacetIdsAndMapOrder(t *testing.T) {
	ctx := context.Background()
	s := &Server{v3MirrorFraction: 1.0} // Mirroring is on

	facet1 := &pb.Facet{
		ImportName:        "test-import1",
		MeasurementMethod: "test-method1",
	}
	facet2 := &pb.Facet{
		ImportName:        "test-import2",
		MeasurementMethod: "test-method2",
	}
	v2Req := &pbv2.ObservationRequest{}
	v2Resp := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			"Count_Person": {
				ByEntity: map[string]*pbv2.EntityObservation{
					"country/USA": {
						OrderedFacets: []*pbv2.FacetObservation{
							{FacetId: "v2_facet_id_1"},
							{FacetId: "v2_facet_id_2"},
						},
					},
				},
			},
		},
		Facets: map[string]*pb.Facet{
			"v2_facet_id_1": facet1,
			"v2_facet_id_2": facet2,
		},
	}
	v3Resp := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{
			"Count_Person": {
				ByEntity: map[string]*pbv2.EntityObservation{
					"country/USA": {
						OrderedFacets: []*pbv2.FacetObservation{
							{FacetId: "v3_facet_id_2"},
							{FacetId: "v3_facet_id_1"},
						},
					},
				},
			},
		},
		Facets: map[string]*pb.Facet{
			"v3_facet_id_2": facet2,
			"v3_facet_id_1": facet1,
		},
	}

	v3Call := func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return v3Resp, nil
	}

	buf, cleanup := setUpSlogCapture()
	defer cleanup()

	var mirrorWg sync.WaitGroup
	s.maybeMirrorV3(ctx, v2Req, v2Resp, 0, v3Call, GetV2ObservationCmpOpts(), &mirrorWg)
	mirrorWg.Wait()

	logOutput := buf.String()
	if logOutput != "" {
		t.Errorf("log output should be empty when responses only differ by facet IDs or facet map ordering, but got %q", logOutput)
	}
}
