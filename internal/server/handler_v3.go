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
	"log/slog"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbsvc "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	restv2 "github.com/datacommonsorg/mixer/internal/sdmx/rest/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/sdmx"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const sdmxJSONStatContentType = "application/json; charset=utf-8"

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

// V3FilterStatVarsByEntity implements API for mixer.V3FilterStatVarsByEntity.
func (s *Server) V3FilterStatVarsByEntity(ctx context.Context, in *pb.FilterStatVarsByEntityRequest) (
	*pb.FilterStatVarsByEntityResponse, error,
) {
	return s.dispatcher.FilterStatVarsByEntity(ctx, in)
}

// V3SdmxData handles SDMX Data requests.
func (s *Server) V3SdmxData(in *pbv3.SdmxRestRequest, stream pbsvc.Mixer_V3SdmxDataServer) error {
	ctx := stream.Context()
	if !s.flags.EnableSDMXDataApi {
		return status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}

	if err := restv2.ValidateDataAccept(ctx); err != nil {
		return err
	}

	originalURI, err := restv2.OriginalURIFromMetadata(ctx)
	if err != nil {
		return err
	}

	request, err := restv2.ParseDataRequest(in.GetTail(), originalURI)
	if err != nil {
		slog.Error("Failed to parse SDMX data request", "error", err, "tail", in.GetTail())
		return err
	}

	query, err := sdmxDataQueryFromREST(request)
	if err != nil {
		return err
	}
	if len(query.Constraints) == 0 {
		slog.Error("SDMX request missing required constraints", "tail", in.GetTail())
		return status.Error(codes.InvalidArgument, "At least one constraint or variableMeasured is required.")
	}

	res, err := s.dispatcher.SdmxData(ctx, query)
	if err != nil {
		slog.Error("Failed to handle SDMX data request in dispatcher", "error", err)
		return status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}

	if res == nil || len(res.Observations) == 0 {
		return stream.Send(&httpbody.HttpBody{
			ContentType: sdmxJSONStatContentType,
			Data:        []byte("{}"),
		})
	}

	formatter := &sdmx.JSONStatFormatter{}
	payload, err := formatter.Format(res.Observations)
	if err != nil {
		slog.Error("Failed to format SDMX response", "error", err)
		return status.Error(codes.Internal, "Internal mapping error occurred.")
	}

	return stream.Send(&httpbody.HttpBody{
		ContentType: sdmxJSONStatContentType,
		Data:        []byte(payload),
	})
}

func sdmxDataQueryFromREST(request *restv2.DataRequest) (*pb.SdmxDataQuery, error) {
	constraints := map[string]*pb.ConstraintList{}
	for componentID, values := range request.Constraints {
		constraintID, err := restv2.InternalConstraintComponentID(componentID)
		if err != nil {
			return nil, err
		}
		if _, exists := constraints[constraintID]; exists {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate SDMX component filter %q", constraintID)
		}
		constraints[constraintID] = &pb.ConstraintList{Values: values}
	}
	return &pb.SdmxDataQuery{Constraints: constraints}, nil
}
