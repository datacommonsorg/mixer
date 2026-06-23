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

package grpc

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/server/sdmx/service"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	originalURIHeader = "x-dc-original-uri"
	envoyPathHeader   = "x-envoy-original-path"
)

func NewRequest(ctx context.Context, tail string) service.Request {
	originalURI, _ := OriginalURI(ctx)
	return service.Request{
		Tail:        tail,
		OriginalURI: originalURI,
		Accept:      Accept(ctx),
		LogSDMX:     ShouldLogSDMX(ctx),
	}
}

func OriginalURI(ctx context.Context) (string, error) {
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

func Accept(ctx context.Context) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	values := []string{}
	values = append(values, md.Get("accept")...)
	values = append(values, md.Get("grpcgateway-accept")...)
	return values
}

func ShouldLogSDMX(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		headers := md.Get(util.XLogSDMX)
		return len(headers) > 0 && headers[0] == "true"
	}
	return false
}
