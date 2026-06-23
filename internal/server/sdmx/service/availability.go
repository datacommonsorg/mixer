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

package service

import (
	"context"
	"log/slog"

	sdmxformat "github.com/datacommonsorg/mixer/internal/server/sdmx/format"
	sdmxjsonv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/format/sdmxjson/v2"
	restv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/rest/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) Availability(ctx context.Context, request Request) (*Response, error) {
	if _, err := restv2.AvailabilityResponseFormatFromAccept(request.Accept); err != nil {
		return nil, err
	}

	restRequest, err := restv2.ParseAvailabilityRequest(request.Tail, request.OriginalURI)
	if err != nil {
		if request.LogSDMX {
			slog.Info("SDMX availability request parse failed", "original_uri", request.OriginalURI, "tail", request.Tail, "error", err)
		}
		return nil, err
	}
	if request.LogSDMX {
		slog.Info("SDMX availability request parsed", "original_uri", request.OriginalURI, "tail", request.Tail, "path", restRequest.Path, "constraints", restRequest.Constraints)
	}

	query, err := availabilityQueryFromREST(restRequest)
	if err != nil {
		if request.LogSDMX {
			slog.Info("SDMX availability dispatcher request failed", "original_uri", request.OriginalURI, "tail", request.Tail, "component", restRequest.Path.ComponentID, "constraints", restRequest.Constraints, "error", err)
		}
		return nil, err
	}
	if request.LogSDMX {
		slog.Info("SDMX availability dispatcher request", "original_uri", request.OriginalURI, "tail", request.Tail, "component", query.ComponentId, "constraints", query.Constraints)
	}
	if s.dispatcher == nil {
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
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
	formatter := &sdmxjsonv2.AvailabilityJSONFormatter{
		AgencyID:   restRequest.Path.AgencyID,
		ResourceID: restRequest.Path.ResourceID,
		Version:    restRequest.Path.Version,
	}
	payload, err := formatter.Format(restRequest.Path.ComponentID, values)
	if err != nil {
		slog.Error("Failed to format SDMX availability response", "error", err)
		return nil, status.Error(codes.Internal, "Internal mapping error occurred.")
	}

	return &Response{
		ContentType: sdmxformat.StructureJSONType,
		Body:        []byte(payload),
	}, nil
}
