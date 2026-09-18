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
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestResponseSizeLimiterProcessor(t *testing.T) {
	exactBoundaryResp := &wrapperspb.BytesValue{Value: make([]byte, 500)}
	exactLimit := proto.Size(exactBoundaryResp)

	tests := []struct {
		name              string
		limitBytes        int
		reqType           RequestType
		originalReq       proto.Message
		currentReq        proto.Message
		currentResp       proto.Message
		nilRequestContext bool
		expectError       bool
		expectCode        codes.Code
		wantMsgSubstrings []string
		omitMsgSubstrings []string
	}{
		{
			name:        "Under limit - succeeds normally",
			limitBytes:  exactLimit,
			reqType:     TypeNode,
			originalReq: &wrapperspb.StringValue{Value: "test"},
			currentResp: &wrapperspb.BytesValue{Value: make([]byte, 50)},
			expectError: false,
		},
		{
			name:        "Exact boundary limit - succeeds",
			limitBytes:  exactLimit,
			reqType:     TypeNode,
			originalReq: &wrapperspb.StringValue{Value: "test"},
			currentResp: exactBoundaryResp,
			expectError: false,
		},
		{
			name:        "Over limit by 1 byte (TypeNode) - returns error without observation hint",
			limitBytes:  exactLimit - 1,
			reqType:     TypeNode,
			originalReq: &wrapperspb.StringValue{Value: "test"},
			currentResp: exactBoundaryResp,
			expectError: true,
			expectCode:  codes.InvalidArgument,
			wantMsgSubstrings: []string{
				"Response payload exceeds maximum allowed size of",
				"Please narrow your request parameters.",
			},
			omitMsgSubstrings: []string{
				responseSizeHints[TypeObservation],
			},
		},
		{
			name:        "Over limit (TypeObservation) - appends observation-specific hint",
			limitBytes:  exactLimit - 1,
			reqType:     TypeObservation,
			originalReq: &wrapperspb.StringValue{Value: strings.Repeat("x", maxLoggedRequestBytes+200)},
			currentResp: exactBoundaryResp,
			expectError: true,
			expectCode:  codes.InvalidArgument,
			wantMsgSubstrings: []string{
				"Response payload exceeds maximum allowed size of",
				responseSizeHints[TypeObservation],
			},
		},
		{
			name:        "Over limit with nil OriginalRequest - falls back to CurrentRequest",
			limitBytes:  exactLimit - 1,
			reqType:     TypeObservation,
			originalReq: nil,
			currentReq:  &wrapperspb.StringValue{Value: "fallback-current-request"},
			currentResp: exactBoundaryResp,
			expectError: true,
			expectCode:  codes.InvalidArgument,
			wantMsgSubstrings: []string{
				"Response payload exceeds maximum allowed size of",
				responseSizeHints[TypeObservation],
			},
		},
		{
			name:        "Nil CurrentResponse - no-op",
			limitBytes:  exactLimit,
			reqType:     TypeNode,
			originalReq: &wrapperspb.StringValue{Value: "test"},
			currentResp: nil,
			expectError: false,
		},
		{
			name:              "Nil RequestContext - no-op",
			limitBytes:        exactLimit,
			nilRequestContext: true,
			expectError:       false,
		},
		{
			name:        "Non-positive limitBytes - no-op",
			limitBytes:  0,
			reqType:     TypeNode,
			currentResp: exactBoundaryResp,
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			processor := NewResponseSizeLimiterProcessor(tc.limitBytes)

			var rc *RequestContext
			if !tc.nilRequestContext {
				rc = &RequestContext{
					Context:         context.Background(),
					Type:            tc.reqType,
					OriginalRequest: tc.originalReq,
					CurrentRequest:  tc.currentReq,
					CurrentResponse: tc.currentResp,
				}
			}

			preOutcome, preErr := processor.PreProcess(rc)
			if preErr != nil || preOutcome != Continue {
				t.Fatalf("PreProcess() = (%v, %v), want (%v, nil)", preOutcome, preErr, Continue)
			}

			outcome, err := processor.PostProcess(rc)
			if outcome != Continue {
				t.Errorf("PostProcess() outcome = %v, want %v", outcome, Continue)
			}

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				s, ok := status.FromError(err)
				if !ok {
					t.Fatalf("expected gRPC status error, got %v", err)
				}
				if s.Code() != tc.expectCode {
					t.Errorf("got code %v, want %v", s.Code(), tc.expectCode)
				}
				for _, substr := range tc.wantMsgSubstrings {
					if !strings.Contains(s.Message(), substr) {
						t.Errorf("error message %q missing expected substring %q", s.Message(), substr)
					}
				}
				for _, substr := range tc.omitMsgSubstrings {
					if substr != "" && strings.Contains(s.Message(), substr) {
						t.Errorf("error message %q unexpectedly contained %q", s.Message(), substr)
					}
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestFormatRequestForLog(t *testing.T) {
	if got := formatRequestForLog(nil); got != "" {
		t.Errorf("formatRequestForLog(nil) = %q, want empty string", got)
	}

	shortReq := &wrapperspb.StringValue{Value: "short"}
	if got := formatRequestForLog(shortReq); got != `"short"` {
		t.Errorf("formatRequestForLog(short) = %q, want %q", got, `"short"`)
	}

	longReq := &wrapperspb.StringValue{Value: strings.Repeat("a", maxLoggedRequestBytes+50)}
	gotLong := formatRequestForLog(longReq)
	if !strings.HasSuffix(gotLong, truncatedSuffix) {
		t.Errorf("formatRequestForLog(long) = %q, expected suffix %q", gotLong, truncatedSuffix)
	}
	wantLen := maxLoggedRequestBytes + len(truncatedSuffix)
	if len(gotLong) != wantLen {
		t.Errorf("len(formatRequestForLog(long)) = %d, want %d", len(gotLong), wantLen)
	}

	// Place a 3-byte UTF-8 rune ('日', 0xE6 0x97 0xA5) so that it straddles
	// maxLoggedRequestBytes (1024): protojson marshals StringValue as `"<val>"`,
	// so byte 0 is '"' and indices 1..1022 are 'a' (1022 bytes), placing '日'
	// at bytes 1023, 1024, 1025. Truncation at 1024 must back up to byte 1023.
	utf8BoundaryReq := &wrapperspb.StringValue{
		Value: strings.Repeat("a", maxLoggedRequestBytes-2) + "日本",
	}
	gotUTF8 := formatRequestForLog(utf8BoundaryReq)
	if !utf8.ValidString(gotUTF8) {
		t.Fatalf("formatRequestForLog(utf8BoundaryReq) produced invalid UTF-8 string: %q", gotUTF8)
	}
	if !strings.HasSuffix(gotUTF8, truncatedSuffix) {
		t.Errorf("formatRequestForLog(utf8BoundaryReq) = %q, expected suffix %q", gotUTF8, truncatedSuffix)
	}
	wantUTF8PrefixLen := maxLoggedRequestBytes - 1 // backed up 1 byte to start of '日'
	if len(gotUTF8) != wantUTF8PrefixLen+len(truncatedSuffix) {
		t.Errorf("len(formatRequestForLog(utf8BoundaryReq)) = %d, want %d", len(gotUTF8), wantUTF8PrefixLen+len(truncatedSuffix))
	}
}

type mockTrackingProcessor struct {
	postProcessFn func(rc *RequestContext) (Outcome, error)
}

func (m *mockTrackingProcessor) PreProcess(rc *RequestContext) (Outcome, error) {
	return Continue, nil
}

func (m *mockTrackingProcessor) PostProcess(rc *RequestContext) (Outcome, error) {
	if m.postProcessFn != nil {
		return m.postProcessFn(rc)
	}
	return Continue, nil
}

func TestResponseSizeLimiterProcessor_DispatcherPipelineOrder(t *testing.T) {
	// Simulate the processor order configured in cmd/main.go:
	// [0] outer cache processor (PostProcess runs last)
	// [1] ResponseSizeLimiterProcessor (PostProcess runs after enrichment, before cache write)
	// [2] inner enriching processor (PostProcess runs first, expanding the response)
	cacheVisited := false
	var cacheProc Processor = &mockTrackingProcessor{
		postProcessFn: func(rc *RequestContext) (Outcome, error) {
			cacheVisited = true
			return Continue, nil
		},
	}
	var sizeLimiter Processor = NewResponseSizeLimiterProcessor(100)
	var enrichProc Processor = &mockTrackingProcessor{
		postProcessFn: func(rc *RequestContext) (Outcome, error) {
			// Enrich response past the 100-byte limit.
			rc.CurrentResponse = &wrapperspb.BytesValue{Value: make([]byte, 200)}
			return Continue, nil
		},
	}

	d := NewDispatcher([]*Processor{&cacheProc, &sizeLimiter, &enrichProc}, nil)
	rc := newRequestContext(context.Background(), &wrapperspb.StringValue{Value: "req"}, TypeObservation)

	_, err := d.handle(rc, func(ctx context.Context, req proto.Message) (proto.Message, error) {
		// Initial handler response is small (under limit).
		return &wrapperspb.BytesValue{Value: make([]byte, 10)}, nil
	})

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("d.handle() error = %v, want InvalidArgument", err)
	}
	if cacheVisited {
		t.Errorf("outer cache processor PostProcess was executed despite response exceeding size limit")
	}
}
