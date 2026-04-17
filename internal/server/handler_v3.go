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
	"encoding/json"
	"log/slog"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// V3Node implements API for mixer.V3Node.
func (s *Server) V3Node(ctx context.Context, in *pbv2.NodeRequest) (
	*pbv2.NodeResponse, error,
) {
	return s.dispatcher.Node(ctx, in, datasources.DefaultPageSize)
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

// V3Event implements API for mixer.V3Event.
func (s *Server) V3Event(ctx context.Context, in *pbv2.EventRequest) (
	*pbv2.EventResponse, error,
) {
	return s.dispatcher.Event(ctx, in)
}

// V3Sparql implements API for mixer.V3Sparql.
func (s *Server) V3Sparql(ctx context.Context, in *pb.SparqlRequest) (
	*pb.QueryResponse, error,
) {
	return s.dispatcher.Sparql(ctx, in)
}

// V3BulkVariableInfo implements API for mixer.V3BulkVariableInfo.
func (s *Server) V3BulkVariableInfo(ctx context.Context, in *pbv1.BulkVariableInfoRequest) (
	*pbv1.BulkVariableInfoResponse, error,
) {
	return s.dispatcher.BulkVariableInfo(ctx, in)
}

// V3BulkVariableGroupInfo implements API for mixer.V3BulkVariableGroupInfo.
func (s *Server) V3BulkVariableGroupInfo(ctx context.Context, in *pbv1.BulkVariableGroupInfoRequest) (
	*pbv1.BulkVariableGroupInfoResponse, error,
) {
	return s.dispatcher.BulkVariableGroupInfo(ctx, in)
}



// V3SdmxData handles SDMX Data requests.
func (s *Server) V3SdmxData(ctx context.Context, in *pbv3.SdmxDataRequest) (
	*pbv3.SdmxDataResponse, error,
) {
	// Parse constraints JSON string into map
	constraints := map[string]string{}
	if in.C != "" {
		if err := json.Unmarshal([]byte(in.C), &constraints); err != nil {
			slog.Error("Failed to parse constraints JSON for SDMX request", "error", err, "input", in.C)
			return nil, status.Error(codes.InvalidArgument, "Invalid constraints format. Please provide a valid JSON object.")
		}
	}

	// Validation Gate: At least one anchor component required or variableMeasured
	if constraints[datasource.DimVariableMeasured] == "" && len(constraints) == 0 {
		slog.Error("SDMX request missing required constraints", "input", in.C)
		return nil, status.Error(codes.InvalidArgument, "At least one constraint or variableMeasured is required.")
	}

	resp, err := s.dispatcher.SdmxData(ctx, in, constraints)
	if err != nil {
		slog.Error("Failed to handle SDMX data request in dispatcher", "error", err)
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}
	return resp, nil
}
