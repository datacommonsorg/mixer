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

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbsvc "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/service"
	sdmxgrpc "github.com/datacommonsorg/mixer/internal/server/sdmx/transport/grpc"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// V3SdmxData handles SDMX Data requests.
func (s *Server) V3SdmxData(in *sdmxpb.SdmxRestRequest, stream pbsvc.Mixer_V3SdmxDataServer) error {
	if !s.flags.EnableSDMXDataApi {
		return status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}
	if in == nil {
		return status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	req, err := sdmxgrpc.NewRequest(stream.Context(), in.GetTail())
	if err != nil {
		return err
	}
	response, err := service.New(s.dispatcher).Data(stream.Context(), req)
	if err != nil {
		return err
	}
	return stream.Send(&httpbody.HttpBody{
		ContentType: response.ContentType,
		Data:        response.Body,
	})
}

// V3SdmxAvailability handles SDMX Availability requests.
func (s *Server) V3SdmxAvailability(ctx context.Context, in *sdmxpb.SdmxRestRequest) (*httpbody.HttpBody, error) {
	if !s.flags.EnableSDMXDataApi {
		return nil, status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	req, err := sdmxgrpc.NewRequest(ctx, in.GetTail())
	if err != nil {
		return nil, err
	}
	response, err := service.New(s.dispatcher).Availability(ctx, req)
	if err != nil {
		return nil, err
	}
	return &httpbody.HttpBody{
		ContentType: response.ContentType,
		Data:        response.Body,
	}, nil
}

// SdmxData provides structured SDMX data to in-process consumers.
func (s *Server) SdmxData(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error) {
	if !s.flags.EnableSDMXDataApi {
		return nil, status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.dispatcher == nil {
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}
	return s.dispatcher.SdmxData(ctx, in)
}

// SdmxAvailability provides structured SDMX availability to in-process consumers.
func (s *Server) SdmxAvailability(ctx context.Context, in *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error) {
	if !s.flags.EnableSDMXDataApi {
		return nil, status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.dispatcher == nil {
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}
	return s.dispatcher.SdmxAvailability(ctx, in)
}
