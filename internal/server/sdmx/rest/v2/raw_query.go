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
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queryParam struct {
	Name  string
	Value string
}

func parseRawQuery(rawQuery string) ([]queryParam, error) {
	if rawQuery == "" {
		return nil, nil
	}

	var params []queryParam
	for _, pair := range strings.Split(rawQuery, "&") {
		if pair == "" {
			continue
		}

		rawName, rawValue, _ := strings.Cut(pair, "=")
		name, err := url.PathUnescape(rawName)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid SDMX query parameter name")
		}
		value, err := url.PathUnescape(rawValue)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid value for SDMX query parameter %q", name)
		}
		params = append(params, queryParam{Name: name, Value: value})
	}
	return params, nil
}
