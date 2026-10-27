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

package agent

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWrapV2BackendError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "invalid argument keeps its message",
			err:      status.Error(codes.InvalidArgument, "unsupported target"),
			wantCode: codes.InvalidArgument,
			wantMsg:  "ctx: unsupported target",
		},
		{
			name:     "wrapped status errors are classified by their code",
			err:      fmt.Errorf("resolve: %w", status.Error(codes.InvalidArgument, "unsupported target")),
			wantCode: codes.InvalidArgument,
			wantMsg:  "ctx: unsupported target",
		},
		{
			name:     "transient errors get the retry message",
			err:      status.Error(codes.NotFound, "Session not found: projects/p/instances/i/databases/d/sessions/s"),
			wantCode: codes.NotFound,
			wantMsg:  "ctx: " + v2BackendRetryMessage,
		},
		{
			name:     "non-status errors become Internal with the retry message",
			err:      errors.New("spanner: connection reset"),
			wantCode: codes.Internal,
			wantMsg:  "ctx: " + v2BackendRetryMessage,
		},
		{
			name:     "deployment problems get the do-not-retry message",
			err:      status.Error(codes.FailedPrecondition, "Embeddings service client is not initialized"),
			wantCode: codes.FailedPrecondition,
			wantMsg:  "ctx: " + v2BackendUnsupportedMessage,
		},
		{
			name:     "backend credential errors become Internal with the do-not-retry message",
			err:      status.Error(codes.Unauthenticated, `auth: "invalid_grant" https://us-central1-aiplatform.googleapis.com/v1/projects/p`),
			wantCode: codes.Internal,
			wantMsg:  "ctx: " + v2BackendUnsupportedMessage,
		},
		{
			name:     "timeouts get the timeout message",
			err:      context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
			wantMsg:  "ctx: " + v2BackendTimeoutMessage,
		},
		{
			name:     "cancellation gets the timeout message",
			err:      context.Canceled,
			wantCode: codes.Canceled,
			wantMsg:  "ctx: " + v2BackendTimeoutMessage,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := status.Convert(wrapV2BackendError(tc.err, "ctx"))
			if st.Code() != tc.wantCode || st.Message() != tc.wantMsg {
				t.Errorf("wrapV2BackendError() = (%v, %q), want (%v, %q)", st.Code(), st.Message(), tc.wantCode, tc.wantMsg)
			}
		})
	}
}
