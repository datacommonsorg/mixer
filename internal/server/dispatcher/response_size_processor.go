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

package dispatcher

import (
	"fmt"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var responseSizeHints = map[RequestType]string{
	TypeObservation: "Try requesting a specific date or reducing the number of variables or entities.",
}

// ResponseSizeLimiterProcessor checks the size of the response and aborts if it exceeds the limit.
type ResponseSizeLimiterProcessor struct {
	limitBytes int
}

func NewResponseSizeLimiterProcessor(limitBytes int) *ResponseSizeLimiterProcessor {
	return &ResponseSizeLimiterProcessor{limitBytes: limitBytes}
}

func (p *ResponseSizeLimiterProcessor) PreProcess(rc *RequestContext) (Outcome, error) {
	return Continue, nil
}

func (p *ResponseSizeLimiterProcessor) PostProcess(rc *RequestContext) (Outcome, error) {
	if rc.CurrentResponse == nil {
		return Continue, nil
	}

	size := proto.Size(rc.CurrentResponse)
	if size > p.limitBytes {
		limitMB := float64(p.limitBytes) / 1024 / 1024

		// Safely attempt to serialize the request for debug.
		var reqPayload string
		if rc.OriginalRequest != nil {
			bytes, _ := protojson.MarshalOptions{
				UseProtoNames:   true,
				EmitUnpopulated: false,
			}.Marshal(rc.OriginalRequest)
			reqPayload = string(bytes)
			if len(reqPayload) > 1024 {
				reqPayload = reqPayload[:1024] + " ... [truncated]"
			}
		}

		slog.Error("Blocked large response payload",
			"requestType", rc.Type,
			"sizeBytes", size,
			"limitBytes", p.limitBytes,
			"request", reqPayload,
		)

		limitMsg := fmt.Sprintf("Response payload exceeds maximum allowed size of %.2f MB. Please narrow your request parameters.", limitMB)
		if hint, ok := responseSizeHints[rc.Type]; ok {
			limitMsg += " " + hint
		}
		limitError := status.New(codes.InvalidArgument, limitMsg)
		return Continue, limitError.Err()
	}

	return Continue, nil
}
