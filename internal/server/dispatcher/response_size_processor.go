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
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	bytesPerMB            = 1024 * 1024
	maxLoggedRequestBytes = 1024
	truncatedSuffix       = " ... [truncated]"
)

var (
	logMarshalOptions = protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}

	responseSizeHints = map[RequestType]string{
		TypeObservation: "Try requesting a specific date or reducing the number of variables or entities.",
	}
)

// ResponseSizeLimiterProcessor checks the serialized protobuf size of the
// response and aborts post-processing with codes.InvalidArgument if it exceeds
// limitBytes.
type ResponseSizeLimiterProcessor struct {
	limitBytes int
}

// NewResponseSizeLimiterProcessor creates a processor that enforces limitBytes
// on response payloads.
func NewResponseSizeLimiterProcessor(limitBytes int) *ResponseSizeLimiterProcessor {
	return &ResponseSizeLimiterProcessor{limitBytes: limitBytes}
}

// PreProcess is a no-op for ResponseSizeLimiterProcessor.
func (p *ResponseSizeLimiterProcessor) PreProcess(rc *RequestContext) (Outcome, error) {
	return Continue, nil
}

// PostProcess checks the size of rc.CurrentResponse and returns an
// InvalidArgument error if it exceeds p.limitBytes.
func (p *ResponseSizeLimiterProcessor) PostProcess(rc *RequestContext) (Outcome, error) {
	if rc == nil || rc.CurrentResponse == nil || p.limitBytes <= 0 {
		return Continue, nil
	}

	size := proto.Size(rc.CurrentResponse)
	if size <= p.limitBytes {
		return Continue, nil
	}

	req := rc.OriginalRequest
	if req == nil {
		req = rc.CurrentRequest
	}

	slog.Error("Blocked large response payload",
		"requestType", rc.Type,
		"sizeBytes", size,
		"limitBytes", p.limitBytes,
		"request", formatRequestForLog(req),
	)

	return Continue, status.Error(codes.InvalidArgument, buildLimitExceededMessage(rc.Type, p.limitBytes))
}

// formatRequestForLog serializes req to compact JSON for debug logging,
// truncating payloads larger than maxLoggedRequestBytes on a valid UTF-8 rune
// boundary without allocating a full-size string copy first.
func formatRequestForLog(req proto.Message) string {
	if req == nil {
		return ""
	}
	bytes, err := logMarshalOptions.Marshal(req)
	if err != nil {
		return ""
	}
	if len(bytes) <= maxLoggedRequestBytes {
		return string(bytes)
	}
	end := maxLoggedRequestBytes
	for end > 0 && !utf8.RuneStart(bytes[end]) {
		end--
	}
	return string(bytes[:end]) + truncatedSuffix
}

// buildLimitExceededMessage constructs the user-facing error message with any
// request-type-specific hint.
func buildLimitExceededMessage(reqType RequestType, limitBytes int) string {
	limitMB := float64(limitBytes) / bytesPerMB
	msg := fmt.Sprintf("Response payload exceeds maximum allowed size of %.2f MB. Please narrow your request parameters.", limitMB)
	if hint, ok := responseSizeHints[reqType]; ok {
		msg += " " + hint
	}
	return msg
}
