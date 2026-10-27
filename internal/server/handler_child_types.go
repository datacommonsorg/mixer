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

package server

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxChildTypesAncestors caps the ancestors per ChildTypes request. The agent service sends at most one
// per resolved entity (20 per call); the cap bounds load from the publicly routable internal endpoint.
const maxChildTypesAncestors = 100

// ChildTypes returns the types of entities contained in each requested ancestor, for in-process
// consumers such as the agent service. It is served by the dispatcher's data sources (Spanner
// LinkedEdge, and remote data sources for custom DC instances).
func (s *Server) ChildTypes(ctx context.Context, in *pbv2.ChildTypesRequest) (*pbv2.ChildTypesResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if len(in.GetAncestors()) == 0 {
		return &pbv2.ChildTypesResponse{}, nil
	}
	if len(in.GetAncestors()) > maxChildTypesAncestors {
		return nil, status.Errorf(codes.InvalidArgument,
			"too many ancestors (%d): request at most %d per call", len(in.GetAncestors()), maxChildTypesAncestors)
	}
	if s.dispatcher == nil {
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}
	return s.dispatcher.ChildTypes(ctx, in)
}

// V2InternalChildTypes handles internal child type requests from remote data sources.
// Note: /v2/internal path prefix is used to ensure remote inter-instance calls succeed across
// existing proxy policies (e.g. Apigee).
func (s *Server) V2InternalChildTypes(ctx context.Context, in *pbv2.ChildTypesRequest) (*pbv2.ChildTypesResponse, error) {
	return s.ChildTypes(ctx, in)
}
