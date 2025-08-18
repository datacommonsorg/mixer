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
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/datacommonsorg/mixer/internal/metrics"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// maybeMirrorV3 decides whether to send a mirror version of an API request to
// the V3 API based on mirroring percentage and request characteristics. For
// instance, only the first page of paginated requests is a candidate for mirroring.
func (s *Server) maybeMirrorV3(
	ctx context.Context,
	originalReq proto.Message,
	originalResp proto.Message,
	originalLatency time.Duration,
	v3Call func(ctx context.Context, req proto.Message) (proto.Message, error),
) {
	// For requests with pagination, only mirror the first page.
	if req, ok := originalReq.(interface{ GetNextToken() string }); ok {
		if req.GetNextToken() != "" {
			return
		}
	}

	if s.v3MirrorPercent > 0 && rand.Intn(100) < s.v3MirrorPercent {
		s.mirrorV3(ctx, originalReq, originalResp, originalLatency, v3Call)
	}
}

func (s *Server) mirrorV3(
	ctx context.Context,
	originalReq proto.Message,
	originalResp proto.Message,
	originalLatency time.Duration,
	v3Call func(ctx context.Context, req proto.Message) (proto.Message, error),
) {
	s.mirrorV3Internal(ctx, originalReq, originalResp, originalLatency, v3Call, false /* skipCache */)
	s.mirrorV3Internal(ctx, originalReq, originalResp, originalLatency, v3Call, true /* skipCache */)
}

// mirrorV3Internal mirrors an existing API request to its V3 equivalent, compares latency,
// and logs any differences in the response.
// Metrics are used to record the latency difference and count how often
// the responses differ.
// This is run in a separate goroutine to not block the response to the original
// request.
func (s *Server) mirrorV3Internal(
	ctx context.Context,
	originalReq proto.Message,
	originalResp proto.Message,
	originalLatency time.Duration,
	v3Call func(ctx context.Context, req proto.Message) (proto.Message, error),
	skipCache bool,
) {
	reqClone := proto.Clone(originalReq)

	go func() {
		v3StartTime := time.Now()
		var v3Resp proto.Message
		var v3Err error
		var v3Ctx context.Context
		if skipCache {
			v3Ctx = metadata.NewOutgoingContext(context.Background(), metadata.Pairs(string(util.XSkipCache), "true"))
		} else {
			v3Ctx = context.Background()
		}
		v3Resp, v3Err = v3Call(v3Ctx, reqClone)
		v3Latency := time.Since(v3StartTime)

		latencyDiff := v3Latency - originalLatency
		metrics.RecordV3LatencyDiff(ctx, latencyDiff, skipCache)

		rpcMethod := reflect.TypeOf(originalReq).Elem().Name()
		if v3Err != nil {
			log.Printf("V3 mirrored call failed. V3 Method: %s, skipCache: %t, Error: %v", rpcMethod, skipCache, v3Err)
			metrics.RecordV3MirrorError(ctx, v3Err)
			return
		}

		if diff := cmp.Diff(originalResp, v3Resp, protocmp.Transform()); diff != "" {
			log.Printf("V3 mirrored call had a different response. V3 Method: %s, skipCache: %t, Diff: %s", rpcMethod, skipCache, diff)
			metrics.RecordV3Mismatch(ctx)
		}
	}()
}
