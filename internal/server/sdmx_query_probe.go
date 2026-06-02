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
	"encoding/json"
	"net/url"
	"strings"

	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	sdmxOriginalPathMetadataKey = "x-envoy-original-path"
	maxSDMXProbeOriginalPathLen = 16 * 1024
	maxSDMXProbeQueryParams     = 256
)

type sdmxQueryProbeResponse struct {
	Path        string              `json:"path"`
	PathParams  map[string]string   `json:"pathParams"`
	QueryParams map[string][]string `json:"queryParams"`
	Constraints map[string][]string `json:"constraints"`
	Headers     map[string][]string `json:"headers,omitempty"`
}

// V3SdmxQueryProbe returns the SDMX query parameters parsed from Envoy's
// forwarded original path. It is a temporary endpoint for validating Envoy and
// ESPv2 behavior before implementing the public SDMX REST surface.
func (s *Server) V3SdmxQueryProbe(ctx context.Context, in *pbv3.SdmxQueryProbeRequest) (
	*httpbody.HttpBody, error,
) {
	if !s.flags.EnableSDMXDataApi {
		return nil, status.Error(codes.Unimplemented, "SDMX API is not enabled")
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "missing incoming gRPC metadata")
	}

	originalPaths := md.Get(sdmxOriginalPathMetadataKey)
	if len(originalPaths) == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"missing %s metadata; call this probe through Envoy or ESPv2",
			sdmxOriginalPathMetadataKey,
		)
	}

	path, queryParams, constraints, err := parseSDMXProbeOriginalPath(originalPaths[0])
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(&sdmxQueryProbeResponse{
		Path: path,
		PathParams: map[string]string{
			"context":    in.GetContext(),
			"agencyID":   in.GetAgencyId(),
			"resourceID": in.GetResourceId(),
			"version":    in.GetVersion(),
			"key":        in.GetKey(),
		},
		QueryParams: queryParams,
		Constraints: constraints,
		Headers:     sdmxProbeHeaders(md),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to encode SDMX query probe response")
	}

	return &httpbody.HttpBody{
		ContentType: "application/json; charset=utf-8",
		Data:        payload,
	}, nil
}

func parseSDMXProbeOriginalPath(originalPath string) (
	string, map[string][]string, map[string][]string, error,
) {
	if len(originalPath) > maxSDMXProbeOriginalPathLen {
		return "", nil, nil, status.Error(codes.InvalidArgument, "SDMX request URI is too long")
	}

	parsedURL, err := url.ParseRequestURI(originalPath)
	if err != nil {
		return "", nil, nil, status.Error(codes.InvalidArgument, "invalid SDMX request URI")
	}

	queryParams, constraints, err := parseSDMXProbeRawQuery(parsedURL.RawQuery)
	if err != nil {
		return "", nil, nil, err
	}
	return parsedURL.Path, queryParams, constraints, nil
}

func parseSDMXProbeRawQuery(rawQuery string) (
	map[string][]string, map[string][]string, error,
) {
	queryParams := map[string][]string{}
	constraints := map[string][]string{}
	if rawQuery == "" {
		return queryParams, constraints, nil
	}

	pairs := strings.Split(rawQuery, "&")
	if len(pairs) > maxSDMXProbeQueryParams {
		return nil, nil, status.Error(codes.InvalidArgument, "too many SDMX query parameters")
	}

	for _, pair := range pairs {
		if pair == "" {
			continue
		}

		rawName, rawValue, _ := strings.Cut(pair, "=")
		name, err := url.PathUnescape(rawName)
		if err != nil {
			return nil, nil, status.Error(codes.InvalidArgument, "invalid SDMX query parameter name")
		}
		// SDMX uses "+" as an operator character, not as form-encoded space.
		value, err := url.PathUnescape(rawValue)
		if err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "invalid value for SDMX query parameter %q", name)
		}

		queryParams[name] = append(queryParams[name], redactSDMXProbeValue(name, value))
		if componentID, ok := sdmxConstraintComponentID(name); ok {
			constraints[componentID] = append(constraints[componentID], value)
		}
	}
	return queryParams, constraints, nil
}

func sdmxConstraintComponentID(name string) (string, bool) {
	if !strings.HasPrefix(name, "c[") || !strings.HasSuffix(name, "]") {
		return "", false
	}
	componentID := name[2 : len(name)-1]
	return componentID, componentID != ""
}

func redactSDMXProbeValue(name, value string) string {
	switch strings.ToLower(name) {
	case "key", "api_key", "api-key", "apikey", "access_token":
		return "[REDACTED]"
	default:
		return value
	}
}

func sdmxProbeHeaders(md metadata.MD) map[string][]string {
	headers := map[string][]string{}
	for _, name := range []string{"accept", "accept-language", "if-modified-since"} {
		if values := md.Get(name); len(values) > 0 {
			headers[name] = values
		}
	}
	return headers
}
