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
	csvv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/format/csv/v2"
	jsonstatv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/format/jsonstat/v2"
	restv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/rest/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) Data(ctx context.Context, request Request) (*Response, error) {
	restRequest, err := restv2.ParseDataRequest(request.Tail, request.OriginalURI)
	if err != nil {
		if request.LogSDMX {
			slog.Info("SDMX data request parse failed", "original_uri", request.OriginalURI, "tail", request.Tail, "error", err)
		}
		slog.Error("Failed to parse SDMX data request", "error", err, "tail", request.Tail)
		return nil, err
	}
	if request.LogSDMX {
		slog.Info("SDMX data request parsed", "original_uri", request.OriginalURI, "tail", request.Tail, "path", restRequest.Path, "constraints", restRequest.Constraints)
	}

	responseFormat, err := restv2.DataResponseFormatFromDataRequest(restRequest, request.Accept)
	if err != nil {
		return nil, err
	}

	query, err := dataQueryFromREST(restRequest)
	if err != nil {
		if request.LogSDMX {
			slog.Info("SDMX data dispatcher request failed", "original_uri", request.OriginalURI, "tail", request.Tail, "constraints", restRequest.Constraints, "error", err)
		}
		return nil, err
	}
	if request.LogSDMX {
		slog.Info("SDMX data dispatcher request", "original_uri", request.OriginalURI, "tail", request.Tail, "constraints", query.Constraints)
	}
	if len(query.Constraints) == 0 {
		slog.Error("SDMX request missing required constraints", "tail", request.Tail)
		return nil, status.Error(codes.InvalidArgument, "At least one constraint or variableMeasured is required.")
	}
	if s.dispatcher == nil {
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}

	res, err := s.dispatcher.SdmxData(ctx, query)
	if err != nil {
		if isSdmxClientError(err) {
			return nil, err
		}
		slog.Error("Failed to handle SDMX data request in dispatcher", "error", err)
		return nil, status.Error(codes.Internal, "Internal server error occurred while processing the request.")
	}
	if res == nil || res.GetShape() == nil {
		slog.Error("SDMX data result missing shape")
		return nil, status.Error(codes.Internal, "Internal mapping error occurred.")
	}

	if responseFormat == restv2.DataResponseFormatCSV {
		formatter := &csvv2.CSVFormatter{StructureID: dataStructureID(restRequest.Path)}
		payload, err := formatter.Format(res)
		if err != nil {
			slog.Error("Failed to format SDMX CSV response", "error", err)
			return nil, status.Error(codes.Internal, "Internal mapping error occurred.")
		}
		return &Response{
			ContentType: sdmxformat.CSVContentType,
			Body:        []byte(payload),
		}, nil
	}

	formatter := &jsonstatv2.JSONStatFormatter{}
	payload, err := formatter.Format(res)
	if err != nil {
		slog.Error("Failed to format SDMX response", "error", err)
		return nil, status.Error(codes.Internal, "Internal mapping error occurred.")
	}

	return &Response{
		ContentType: sdmxformat.JSONStatContentType,
		Body:        []byte(payload),
	}, nil
}
