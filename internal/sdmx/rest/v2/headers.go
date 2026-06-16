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

package restv2

import (
	"context"
	"mime"
	"strings"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	originalURIHeader = "x-dc-original-uri"
	envoyPathHeader   = "x-envoy-original-path"
)

type DataResponseFormat int

const (
	DataResponseFormatJSONStat DataResponseFormat = iota
	DataResponseFormatCSV
)

// OriginalURIFromMetadata returns the trusted request target before transcoding.
func OriginalURIFromMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.InvalidArgument, "missing SDMX request URI")
	}

	for _, key := range []string{originalURIHeader, envoyPathHeader} {
		values := md.Get(key)
		if len(values) > 0 && values[0] != "" {
			return values[0], nil
		}
	}
	return "", status.Error(codes.InvalidArgument, "missing SDMX request URI")
}

// DataResponseFormatFromMetadata selects the SDMX data response format.
func DataResponseFormatFromMetadata(ctx context.Context) (DataResponseFormat, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return DataResponseFormatJSONStat, nil
	}
	for _, key := range []string{"accept", "grpcgateway-accept"} {
		for _, value := range md.Get(key) {
			format, found, err := dataResponseFormatFromAccept(value)
			if err != nil {
				return DataResponseFormatJSONStat, err
			}
			if found {
				return format, nil
			}
		}
	}
	return DataResponseFormatJSONStat, nil
}

// ValidateDataAccept rejects SDMX wire formats that are not implemented yet.
func ValidateDataAccept(ctx context.Context) error {
	_, err := DataResponseFormatFromMetadata(ctx)
	return err
}

func dataResponseFormatFromAccept(value string) (DataResponseFormat, bool, error) {
	for _, item := range strings.Split(value, ",") {
		mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(item))
		if err != nil {
			continue
		}
		switch strings.ToLower(mediaType) {
		case "application/vnd.sdmx.data+csv":
			if err := validateCSVAcceptParams(params); err != nil {
				return DataResponseFormatJSONStat, true, err
			}
			return DataResponseFormatCSV, true, nil
		case "application/vnd.sdmx.data+json":
			return DataResponseFormatJSONStat, true, status.Error(codes.Unimplemented, "SDMX JSON responses are not implemented yet")
		}
	}
	return DataResponseFormatJSONStat, false, nil
}

func validateCSVAcceptParams(params map[string]string) error {
	for key, value := range params {
		switch strings.ToLower(key) {
		case "version":
			if value != "2.0.0" {
				return status.Errorf(codes.Unimplemented, "SDMX CSV version %q is not implemented yet", value)
			}
		case "q":
			continue
		default:
			return status.Errorf(codes.Unimplemented, "SDMX CSV response option %q is not implemented yet", key)
		}
	}
	return nil
}

// ShouldLogSDMX checks whether SDMX request debug logs are enabled.
func ShouldLogSDMX(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		headers := md.Get(util.XLogSDMX)
		return len(headers) > 0 && headers[0] == "true"
	}
	return false
}
