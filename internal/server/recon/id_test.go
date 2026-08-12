// Copyright 2026 Google LLC
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

package recon

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResolveIds_InvalidInput(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		req  *pb.ResolveIdsRequest
	}{
		{
			name: "NilRequest",
			req:  nil,
		},
		{
			name: "EmptyInProp",
			req:  &pb.ResolveIdsRequest{InProp: "", OutProp: "dcid", Ids: []string{"geoId/06"}},
		},
		{
			name: "EmptyOutProp",
			req:  &pb.ResolveIdsRequest{InProp: "wikidataId", OutProp: "", Ids: []string{"geoId/06"}},
		},
		{
			name: "EmptyIds",
			req:  &pb.ResolveIdsRequest{InProp: "wikidataId", OutProp: "dcid", Ids: nil},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveIds(ctx, tc.req, nil)
			if err == nil {
				t.Fatalf("ResolveIds(%s) expected error, got nil", tc.name)
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("ResolveIds(%s) returned non-gRPC error: %v", tc.name, err)
			}
			if st.Code() != codes.InvalidArgument {
				t.Errorf("ResolveIds(%s) code = %v, want %v", tc.name, st.Code(), codes.InvalidArgument)
			}
		})
	}
}
