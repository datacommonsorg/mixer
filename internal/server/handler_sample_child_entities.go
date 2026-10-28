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

const (
	// maxSampleChildEntitiesAncestors caps the ancestors per SampleChildEntities request. The agent service
	// sends one; the cap bounds load from the publicly routable internal endpoint.
	maxSampleChildEntitiesAncestors = 100
	// maxSampleChildEntitiesLimit caps the children sampled per ancestor.
	maxSampleChildEntitiesLimit = 100
)

// SampleChildEntities returns, for each requested ancestor, a sample of its child entities of one type, for
// in-process consumers such as the agent service. It is served by the dispatcher's data sources (Spanner
// LinkedEdge, and remote data sources for custom DC instances).
func (s *Server) SampleChildEntities(ctx context.Context, in *pbv2.SampleChildEntitiesRequest) (*pbv2.SampleChildEntitiesResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if len(in.GetAncestors()) > maxSampleChildEntitiesAncestors {
		return nil, status.Errorf(codes.InvalidArgument,
			"too many ancestors (%d): request at most %d per call", len(in.GetAncestors()), maxSampleChildEntitiesAncestors)
	}
	// An empty child_type is rejected rather than queried: in custom DCs, LinkedEdge rows with an empty
	// child_type mark children whose type was unknown at import time, not a type to sample.
	if in.GetChildType() == "" {
		return nil, status.Error(codes.InvalidArgument, "child_type cannot be empty")
	}
	if limit := in.GetLimitPerAncestor(); limit < 1 || limit > maxSampleChildEntitiesLimit {
		return nil, status.Errorf(codes.InvalidArgument,
			"limit_per_ancestor must be between 1 and %d, got %d", maxSampleChildEntitiesLimit, limit)
	}
	if len(in.GetAncestors()) == 0 {
		return &pbv2.SampleChildEntitiesResponse{}, nil
	}
	if s.dispatcher == nil {
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}
	return s.dispatcher.SampleChildEntities(ctx, in)
}

// V2InternalSampleChildEntities handles internal child entity sample requests from remote data sources.
// Note: /v2/internal path prefix is used to ensure remote inter-instance calls succeed across existing proxy
// policies (e.g. Apigee).
func (s *Server) V2InternalSampleChildEntities(ctx context.Context, in *pbv2.SampleChildEntitiesRequest) (*pbv2.SampleChildEntitiesResponse, error) {
	return s.SampleChildEntities(ctx, in)
}
