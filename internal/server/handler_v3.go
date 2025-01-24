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

// Package server is the main server
package server

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/v3/observation"
)

// V3Node implements API for mixer.V3Node.
func (s *Server) V3Node(ctx context.Context, in *pbv2.NodeRequest) (
	*pbv2.NodeResponse, error,
) {
	return s.dispatcher.Node(ctx, in)
}

// V3Observation implements API for mixer.V3Observation.
func (s *Server) V3Observation(ctx context.Context, in *pbv2.ObservationRequest) (
	*pbv2.ObservationResponse, error,
) {
	return s.dispatcher.Observation(ctx, in)
}

// V3NodeSearch implements API for mixer.V3NodeSearch.
func (s *Server) V3NodeSearch(ctx context.Context, in *pbv2.NodeSearchRequest) (
	*pbv2.NodeSearchResponse, error,
) {
	return s.dispatcher.NodeSearch(ctx, in)
}

// V3Resolve implements API for mixer.V3Resolve.
func (s *Server) V3Resolve(ctx context.Context, in *pbv2.ResolveRequest) (
	*pbv2.ResolveResponse, error,
) {
	return s.dispatcher.Resolve(ctx, in)
}
