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
	"net/url"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataRequest struct {
	Path        ResourcePath
	Constraints map[string][]string
	Format      string
}

const (
	dataFormatJSONStat = "json-stat"
	dataFormatCSV      = "csv"
)

func ParseDataRequest(tail string, originalURI string) (*DataRequest, error) {
	parsedURI, err := url.ParseRequestURI(originalURI)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid SDMX request URI")
	}

	path, err := parseResourcePath(tail)
	if err != nil {
		return nil, err
	}
	params, err := parseRawQuery(parsedURI.RawQuery)
	if err != nil {
		return nil, err
	}

	constraints := map[string][]string{}
	format := ""
	for _, param := range params {
		ok, err := parseComponentFilter(param, constraints)
		if err != nil {
			return nil, err
		}
		if ok {
			continue
		}
		format, err = parseDataQueryParam(param, format)
		if err != nil {
			return nil, err
		}
	}

	if err := validateDataRequest(path, constraints); err != nil {
		return nil, err
	}

	return &DataRequest{Path: path, Constraints: constraints, Format: format}, nil
}

func DataResponseFormatFromDataRequest(request *DataRequest, accept []string) (DataResponseFormat, error) {
	switch request.Format {
	case "":
		return DataResponseFormatFromAccept(accept)
	case dataFormatCSV:
		return DataResponseFormatCSV, nil
	default:
		return DataResponseFormatUnknown, status.Errorf(codes.InvalidArgument, "unsupported SDMX data response format %q", request.Format)
	}
}

func parseDataQueryParam(param queryParam, format string) (string, error) {
	switch param.Name {
	case "format":
		if format != "" {
			return "", status.Error(codes.InvalidArgument, "duplicate SDMX format query parameter")
		}
		if param.Value != dataFormatCSV {
			return "", status.Errorf(codes.InvalidArgument, "unsupported SDMX data response format %q", param.Value)
		}
		return param.Value, nil
	default:
		return format, nil
	}
}
