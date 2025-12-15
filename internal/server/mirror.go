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
	"context"
	"log/slog"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/datacommonsorg/mixer/internal/metrics"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// slowQueryThreshold is the latency difference threshold above which the query will be logged for further investigation.
const slowQueryThreshold = 10 * time.Second

// maybeMirrorV3 decides whether to send a mirror version of an API request to
// the V3 API based on mirroring percentage and request characteristics. For
// instance, only the first page of paginated requests is a candidate for mirroring.
// Optionally passing a WaitGroup allows tests to wait for fire-and-forget calls.
func (s *Server) maybeMirrorV3(
	ctx context.Context,
	originalReq proto.Message,
	originalResp proto.Message,
	originalLatency time.Duration,
	v3Call func(ctx context.Context, req proto.Message) (proto.Message, error),
	cmpOpts []cmp.Option,
	v3WaitGroup ...*sync.WaitGroup,
) {
	// For requests with pagination, only mirror the first page.
	if req, ok := originalReq.(interface{ GetNextToken() string }); ok {
		if req.GetNextToken() != "" {
			return
		}
	}

	if s.flags.V3MirrorFraction > 0 && rand.Float64() < s.flags.V3MirrorFraction {
		var wg *sync.WaitGroup
		if len(v3WaitGroup) > 0 {
			wg = v3WaitGroup[0]
		}
		s.mirrorV3(ctx, originalReq, originalResp, originalLatency, v3Call, cmpOpts, wg)
	}
}

func (s *Server) mirrorV3(
	ctx context.Context,
	originalReq proto.Message,
	originalResp proto.Message,
	originalLatency time.Duration,
	v3Call func(ctx context.Context, req proto.Message) (proto.Message, error),
	cmpOpts []cmp.Option,
	v3WaitGroup *sync.WaitGroup,
) {
	if v3WaitGroup != nil {
		v3WaitGroup.Add(1)
	}
	// This is run in a separate goroutine to not block the response to the original
	// request.
	go func() {
		if v3WaitGroup != nil {
			defer v3WaitGroup.Done()
		}
		// Create a new context for this goroutine, so it does not get canceled
		// with the original request.
		mirrorCtx := metrics.NewContext(ctx)

		// First call, without skipping cache
		s.doMirror(mirrorCtx, originalReq, originalResp, originalLatency, v3Call, cmpOpts, false /* skipCache */)
		// Second call, skipping cache.
		// Must be run second so that the cache isn't always warm.
		s.doMirror(mirrorCtx, originalReq, originalResp, originalLatency, v3Call, cmpOpts, true /* skipCache */)
	}()
}

// doMirror mirrors an existing API request to its V3 equivalent, compares latency,
// and logs any differences in the response.
// Metrics are used to record the latency difference and count how often
// the responses differ.
func (s *Server) doMirror(
	ctx context.Context,
	originalReq proto.Message,
	originalResp proto.Message,
	originalLatency time.Duration,
	v3Call func(ctx context.Context, req proto.Message) (proto.Message, error),
	cmpOpts []cmp.Option,
	skipCache bool,
) {
	reqClone := proto.Clone(originalReq)

	v3StartTime := time.Now()
	var v3Resp proto.Message
	var v3Err error
	var v3Ctx context.Context
	if skipCache {
		v3Ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs(string(util.XSkipCache), "true"))
	} else {
		v3Ctx = context.Background()
	}
	v3Resp, v3Err = v3Call(v3Ctx, reqClone)
	v3Latency := time.Since(v3StartTime)

	latencyDiff := v3Latency - originalLatency
	metrics.RecordV3LatencyDiff(ctx, latencyDiff, skipCache)

	rpcMethod := reflect.TypeOf(originalReq).Elem().Name()
	if latencyDiff >= slowQueryThreshold {
slog.Warn("V3 mirrored call is significantly slower than V2", "method", rpcMethod, "request", originalReq.String(), "skipCache", skipCache, "latencyDiff", latencyDiff)
	}

	if v3Err != nil {
		slog.Warn("V3 mirrored call failed", "method", rpcMethod, "skipCache", skipCache, "error", v3Err)
		metrics.RecordV3MirrorError(ctx, v3Err)
		return
	}

	if diff := cmp.Diff(originalResp, v3Resp, cmpOpts...); diff != "" {
		slog.Warn("V3 mirrored call had a different response", "method", rpcMethod, "skipCache", skipCache, "diff", diff)
		metrics.RecordV3Mismatch(ctx)
	}
}

func GetV2ResolveCmpOpts() []cmp.Option {
	return []cmp.Option{
		protocmp.Transform(),
	}
}

func GetV2NodeCmpOpts() []cmp.Option {
	return []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&pbv2.NodeResponse{}, "next_token"),
	}
}

func GetV2ObservationCmpOpts() []cmp.Option {
	// A custom comparer for ObservationResponse.
	// This gives us full control over the comparison logic.
	observationComparer := cmp.Comparer(func(x, y *pbv2.ObservationResponse) bool {
		// 1. Compare the `Facets` map by values only.
		xVals := []*pb.Facet{}
		for _, v := range x.GetFacets() {
			xVals = append(xVals, v)
		}
		yVals := []*pb.Facet{}
		for _, v := range y.GetFacets() {
			yVals = append(yVals, v)
		}
		// Sort slices to compare them as multisets.
		sortProtos := cmpopts.SortSlices(func(a, b *pb.Facet) bool {
			return util.GetFacetID(a) < util.GetFacetID(b)
		})
		if !cmp.Equal(xVals, yVals, protocmp.Transform(), sortProtos) {
			return false
		}

		// 2. Compare the rest of the fields, ignoring the `facets` field
		//    which we have already compared.
		return cmp.Equal(x, y,
			protocmp.Transform(),
			protocmp.IgnoreFields(x, "facets"),
			protocmp.IgnoreFields(&pbv2.FacetObservation{}, "facet_id"))
	})

	return []cmp.Option{
		observationComparer,
	}
}
