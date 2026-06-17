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
	csvv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/csv/v2"
	jsonv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/json/v2"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
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

// V3FilterStatVarsByEntity implements API for mixer.V3FilterStatVarsByEntity.
func (s *Server) V3FilterStatVarsByEntity(ctx context.Context, in *pb.FilterStatVarsByEntityRequest) (
	*pb.FilterStatVarsByEntityResponse, error,
) {
	return s.dispatcher.FilterStatVarsByEntity(ctx, in)
}

// V3SdmxData handles SDMX Data requests.
func (s *Server) V3SdmxData(in *pbv3.SdmxRestRequest, stream pbsvc.Mixer_V3SdmxDataServer) error {
	ctx := stream.Context()
	logSDMX := restv2.ShouldLogSDMX(ctx)
	if !s.flags.EnableSDMXDataApi {
		return status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}

	originalURI, err := restv2.OriginalURIFromMetadata(ctx)
	if err != nil {
		if logSDMX {
			slog.Info("SDMX data request URI failed", "tail", in.GetTail(), "error", err)
		}
		return err
	}

	request, err := restv2.ParseDataRequest(in.GetTail(), originalURI)
	if err != nil {
		if logSDMX {
			slog.Info("SDMX data request parse failed", "original_uri", originalURI, "tail", in.GetTail(), "error", err)
		}
		slog.Error("Failed to parse SDMX data request", "error", err, "tail", in.GetTail())
		return err
	}
	if logSDMX {
		slog.Info("SDMX data request parsed", "original_uri", originalURI, "tail", in.GetTail(), "path", request.Path, "constraints", request.Constraints)
	}

	responseFormat, err := restv2.DataResponseFormatFromDataRequest(ctx, request)
	if err != nil {
		return err
	}

	query, err := sdmxDataQueryFromREST(request)
	if err != nil {
		if logSDMX {
			slog.Info("SDMX data dispatcher request failed", "original_uri", originalURI, "tail", in.GetTail(), "constraints", request.Constraints, "error", err)
		}
		return err
	}
	if logSDMX {
		slog.Info("SDMX data dispatcher request", "original_uri", originalURI, "tail", in.GetTail(), "constraints", query.Constraints)
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

	observations := []*pb.SdmxObservation(nil)
	if res != nil {
		observations = res.Observations
	}

	if responseFormat == restv2.DataResponseFormatCSV {
		formatter := &csvv2.CSVFormatter{StructureID: sdmxDataStructureID(request.Path)}
		payload, err := formatter.Format(observations)
		if err != nil {
			slog.Error("Failed to format SDMX CSV response", "error", err)
			return status.Error(codes.Internal, "Internal mapping error occurred.")
		}
		return stream.Send(&httpbody.HttpBody{
			ContentType: sdmx.CSVContentType,
			Data:        []byte(payload),
		})
	}

	if len(observations) == 0 {
		return stream.Send(&httpbody.HttpBody{
			ContentType: sdmx.JSONStatContentType,
			Data:        []byte("{}"),
		})
	}

	formatter := &sdmx.JSONStatFormatter{}
	payload, err := formatter.Format(observations)
	if err != nil {
		slog.Error("Failed to format SDMX response", "error", err)
		return status.Error(codes.Internal, "Internal mapping error occurred.")
	}

	return stream.Send(&httpbody.HttpBody{
		ContentType: sdmx.JSONStatContentType,
		Data:        []byte(payload),
	})
}

// V3SdmxAvailability handles SDMX Availability requests.
func (s *Server) V3SdmxAvailability(ctx context.Context, in *pbv3.SdmxRestRequest) (*httpbody.HttpBody, error) {
	logSDMX := restv2.ShouldLogSDMX(ctx)
	if !s.flags.EnableSDMXDataApi {
		return nil, status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}

	if _, err := restv2.AvailabilityResponseFormatFromMetadata(ctx); err != nil {
		return nil, err
	}

	originalURI, err := restv2.OriginalURIFromMetadata(ctx)
	if err != nil {
		if logSDMX {
			slog.Info("SDMX availability request URI failed", "tail", in.GetTail(), "error", err)
		}
		return nil, err
	}

	request, err := restv2.ParseAvailabilityRequest(in.GetTail(), originalURI)
	if err != nil {
		if logSDMX {
			slog.Info("SDMX availability request parse failed", "original_uri", originalURI, "tail", in.GetTail(), "error", err)
		}
		return nil, err
	}
	if logSDMX {
		slog.Info("SDMX availability request parsed", "original_uri", originalURI, "tail", in.GetTail(), "path", request.Path, "constraints", request.Constraints)
	}

	query, err := sdmxAvailabilityQueryFromREST(request)
	if err != nil {
		if logSDMX {
			slog.Info("SDMX availability dispatcher request failed", "original_uri", originalURI, "tail", in.GetTail(), "component", request.Path.ComponentID, "constraints", request.Constraints, "error", err)
		}
		return nil, err
	}
	if logSDMX {
		slog.Info("SDMX availability dispatcher request", "original_uri", originalURI, "tail", in.GetTail(), "component", query.ComponentId, "constraints", query.Constraints)
	}

	res, err := s.dispatcher.SdmxAvailability(ctx, query)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
			return nil, err
		}
		slog.Error("Failed to handle SDMX availability request in dispatcher", "error", err)
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}

	values := []string(nil)
	if res != nil {
		values = res.GetValues()
	}
	formatter := &jsonv2.AvailabilityJSONFormatter{
		AgencyID:   request.Path.AgencyID,
		ResourceID: request.Path.ResourceID,
		Version:    request.Path.Version,
	}
	payload, err := formatter.Format(request.Path.ComponentID, values)
	if err != nil {
		slog.Error("Failed to format SDMX availability response", "error", err)
		return nil, status.Error(codes.Internal, "Internal mapping error occurred.")
	}

	return &httpbody.HttpBody{
		ContentType: sdmx.StructureJSONType,
		Data:        []byte(payload),
	}, nil
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

func sdmxAvailabilityQueryFromREST(request *restv2.AvailabilityRequest) (*pb.SdmxAvailabilityQuery, error) {
	componentID, err := restv2.InternalAvailabilityComponentID(request.Path.ComponentID)
	if err != nil {
		return nil, err
	}
	constraints := map[string]*pb.ConstraintList{}
	for filterID, values := range request.Constraints {
		constraintID, err := restv2.InternalAvailabilityConstraintComponentID(filterID)
		if err != nil {
			return nil, err
		}
		if _, exists := constraints[constraintID]; exists {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate SDMX component filter %q", constraintID)
		}
		constraints[constraintID] = &pb.ConstraintList{Values: values}
	}
	return &pb.SdmxAvailabilityQuery{ComponentId: componentID, Constraints: constraints}, nil
}

func sdmxDataStructureID(path restv2.ResourcePath) string {
	agencyID := path.AgencyID
	if agencyID == "" {
		agencyID = sdmx.DataAgencyID
	}
	resourceID := path.ResourceID
	if resourceID == "" {
		resourceID = sdmx.DataResourceID
	}
	version := path.Version
	if version == "" {
		version = sdmx.DataVersion
	}
	return agencyID + ":" + resourceID + "(" + version + ")"
}
