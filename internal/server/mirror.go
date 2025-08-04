// Copyright 2024 Google LLC
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

package server

import (
	"context"
	"log"
	"reflect"
	"time"

	"github.com/datacommonsorg/mixer/internal/metrics"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// mirrorV2 mirrors a V2 request to its V3 equivalent, compares latency,
// and logs any differences in the response.
// Metrics are used to record the latency difference and count how often
// the responses differ.
// This is run in a separate goroutine to not block the main V2 response.
func (s *Server) mirrorV2(
	ctx context.Context,
	v2Request proto.Message,
	v2Response proto.Message,
	v2Latency time.Duration,
	v3Call func(ctx context.Context, req proto.Message) (proto.Message, error),
) {
	reqClone := proto.Clone(v2Request)

	go func() {
		v3StartTime := time.Now()
		v3Resp, v3Err := v3Call(context.Background(), reqClone)
		v3Latency := time.Since(v3StartTime)

		latencyDiff := v3Latency - v2Latency
		metrics.RecordV3LatencyDiff(ctx, latencyDiff)

		rpcMethod := reflect.TypeOf(v2Request).Elem().Name()
		if v3Err != nil {
			log.Printf("V3 mirrored call failed. Method: %s, Error: %v", rpcMethod, v3Err)
			return
		}
		log.Printf("%s V2 Latency: %v, V3 Mirrored Latency: %v", rpcMethod, v2Latency, v3Latency)

		if diff := cmp.Diff(v2Response, v3Resp, protocmp.Transform()); diff != "" {
			log.Printf("V3 mirrored call had a different response. Method: %s. Diff: %s", rpcMethod, diff)
			metrics.RecordV3Mismatch(ctx)
		}
	}()
}
