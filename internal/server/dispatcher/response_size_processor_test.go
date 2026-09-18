// Copyright 2026 Google LLC
// ...

package dispatcher

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestResponseSizeLimiterProcessor(t *testing.T) {
	const limit = 500
	processor := NewResponseSizeLimiterProcessor(limit)

	tests := []struct {
		name         string
		responseSize int
		expectError  bool
		expectCode   codes.Code
	}{
		{
			name:         "Under limit - succeeds normally",
			responseSize: 50, // 50 < 500
			expectError:  false,
		},
		{
			name:         "Over limit - returns error",
			responseSize: 600, // 600 > 500
			expectError:  true,
			expectCode:   codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Mock a response of the desired size
			resp := &wrapperspb.BytesValue{
				Value: make([]byte, tc.responseSize),
			}
			
			rc := &RequestContext{
				Context:         context.Background(),
				Type:            TypeNode,
				OriginalRequest: &wrapperspb.StringValue{Value: "test"},
				CurrentResponse: resp,
			}

			outcome, err := processor.PostProcess(rc)

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if outcome != Continue {
					t.Errorf("expected outcome Continue, got %v", outcome)
				}
				s, ok := status.FromError(err)
				if !ok {
					t.Fatalf("expected gRPC status error, got %v", err)
				}
				if s.Code() != tc.expectCode {
					t.Errorf("got code %v, want %v", s.Code(), tc.expectCode)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if outcome != Continue {
					t.Errorf("expected outcome Continue, got %v", outcome)
				}
			}
		})
	}
}
