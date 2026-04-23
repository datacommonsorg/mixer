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
	"strings"
	"testing"

	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/featureflags"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestV3SdmxData_Validation(t *testing.T) {
	tests := []struct {
		name       string
		request    *pbv3.SdmxDataRequest
		enableSDMXDataApi bool
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name: "API not enabled",
			request: &pbv3.SdmxDataRequest{
				C: "{}",
			},
			enableSDMXDataApi: false,
			wantCode:   codes.Unimplemented,
			wantErrSub: "SDMX API is not enabled",
		},
		{
			name: "Invalid JSON in c",
			request: &pbv3.SdmxDataRequest{
				C: "{invalid json}",
			},
			enableSDMXDataApi: true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "Invalid constraints format. Please provide a valid JSON object.",
		},
		{
			name: "Empty constraints string",
			request: &pbv3.SdmxDataRequest{
				C: "",
			},
			enableSDMXDataApi: true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "At least one constraint or variableMeasured is required.",
		},
		{
			name: "Empty JSON object",
			request: &pbv3.SdmxDataRequest{
				C: "{}",
			},
			enableSDMXDataApi: true,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "At least one constraint or variableMeasured is required.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{flags: &featureflags.Flags{EnableSDMXDataApi: tt.enableSDMXDataApi}}
			_, err := server.V3SdmxData(context.Background(), tt.request)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}

			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("Expected gRPC status error, got: %v", err)
			}

			if st.Code() != tt.wantCode {
				t.Errorf("Got status code %v, want %v", st.Code(), tt.wantCode)
			}

			if !strings.Contains(st.Message(), tt.wantErrSub) {
				t.Errorf("Got error message %q, want it to contain %q", st.Message(), tt.wantErrSub)
			}
		})
	}
}
