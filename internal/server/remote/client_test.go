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

package remote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"google.golang.org/grpc/metadata"
)

func TestGetSurface(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{
			name: "nil context",
			ctx:  nil,
			want: "",
		},
		{
			name: "empty background context",
			ctx:  context.Background(),
			want: "",
		},
		{
			name: "context with x-surface header",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-surface", "mcp-server")),
			want: "mcp-server",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := getSurface(tc.ctx); got != tc.want {
				t.Errorf("getSurface(%v) = %q, want %q", tc.ctx, got, tc.want)
			}
		})
	}
}

func TestRemoteClient_Observation_SurfaceHeader(t *testing.T) {
	var receivedSurface string
	var receivedRemote string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSurface = r.Header.Get("X-Surface")
		receivedRemote = r.Header.Get("X-Remote")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	meta := &resource.Metadata{
		RemoteMixerDomain: ts.URL,
		RemoteMixerAPIKey: "test-api-key",
	}

	client, err := NewRemoteClient(meta)
	if err != nil {
		t.Fatalf("NewRemoteClient failed: %v", err)
	}

	rds := NewRemoteDataSource(client)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-surface", "test-surface-agent"))
	_, err = rds.Observation(ctx, &pbv2.ObservationRequest{})
	if err != nil {
		t.Fatalf("rds.Observation failed: %v", err)
	}

	if receivedSurface != "test-surface-agent" {
		t.Errorf("expected X-Surface header %q, got %q", "test-surface-agent", receivedSurface)
	}
	if receivedRemote != "true" {
		t.Errorf("expected X-Remote header %q, got %q", "true", receivedRemote)
	}
}

func TestRemoteClient_Sdmx_ErrorTolerance(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantNil    bool
	}{
		{
			name:       "success 200",
			statusCode: http.StatusOK,
			body:       `{"series":[{"dimensions":{"variableMeasured":"Count_Person"}}]}`,
			wantNil:    false,
		},
		{
			name:       "remote error 400",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"unsupported component filter"}`,
			wantNil:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()

			meta := &resource.Metadata{
				RemoteMixerDomain: ts.URL,
				RemoteMixerAPIKey: "test-api-key",
			}
			client, err := NewRemoteClient(meta)
			if err != nil {
				t.Fatalf("NewRemoteClient failed: %v", err)
			}

			got, err := client.SdmxData(&sdmxpb.SdmxDataQuery{})
			if err != nil {
				t.Fatalf("client.SdmxData() unexpected error = %v", err)
			}
			if tc.wantNil && got != nil {
				t.Errorf("client.SdmxData() = %v, want nil", got)
			}
			if !tc.wantNil && got == nil {
				t.Errorf("client.SdmxData() = nil, want non-nil result")
			}
		})
	}
}
