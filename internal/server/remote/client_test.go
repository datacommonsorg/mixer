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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

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
		wantSeries int
	}{
		{
			name:       "success 200",
			statusCode: http.StatusOK,
			body:       `{"series":[{"dimensions":{"variableMeasured":"Count_Person"}}]}`,
			wantSeries: 1,
		},
		{
			name:       "remote error 400",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"unsupported component filter"}`,
			wantSeries: 0,
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

			got, err := client.SdmxData(context.Background(), &sdmxpb.SdmxDataQuery{})
			if err != nil {
				t.Fatalf("client.SdmxData() unexpected error = %v", err)
			}
			if got == nil {
				t.Fatalf("client.SdmxData() returned nil result, want non-nil")
			}
			if len(got.GetSeries()) != tc.wantSeries {
				t.Errorf("len(client.SdmxData().GetSeries()) = %d, want %d", len(got.GetSeries()), tc.wantSeries)
			}
		})
	}
}

// Node peels the per-source continuation token out of the composite token it is
// given. The request belongs to the caller and is shared across the concurrent
// data source fan-out, so that peel must not be visible to the caller.
func TestRemoteClientNodeKeepsCallerPaginationToken(t *testing.T) {
	const remoteToken = "remote-page-two"
	var sentNextToken string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading remote request body: %v", err)
			return
		}
		sentReq := &pbv2.NodeRequest{}
		if err := protojson.Unmarshal(body, sentReq); err != nil {
			t.Errorf("unmarshaling remote request: %v", err)
			return
		}
		sentNextToken = sentReq.GetNextToken()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	client, err := NewRemoteClient(&resource.Metadata{
		RemoteMixerDomain: ts.URL,
		RemoteMixerAPIKey: "test-api-key",
	})
	if err != nil {
		t.Fatalf("NewRemoteClient() error = %v", err)
	}

	// The remote data source keys its pagination info by the remote mixer domain.
	callerToken, err := util.EncodeProto(&pbv2.Pagination{
		Info: []*pbv2.Pagination_DataSourceInfo{{
			Id: ts.URL,
			DataSourceInfo: &pbv2.Pagination_DataSourceInfo_StringInfo{
				StringInfo: remoteToken,
			},
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProto() error = %v", err)
	}

	req := &pbv2.NodeRequest{
		Nodes:     []string{"geoId/06"},
		Property:  "->name",
		NextToken: callerToken,
	}
	if _, err := client.Node(context.Background(), req); err != nil {
		t.Fatalf("Node() error = %v", err)
	}

	if got := req.GetNextToken(); got != callerToken {
		t.Errorf("caller request next token = %q, want %q", got, callerToken)
	}
	if sentNextToken != remoteToken {
		t.Errorf("remote received next token = %q, want %q", sentNextToken, remoteToken)
	}
}

// MergeMultiNode drops a source from the merged token once it stops returning a
// cursor, so a later page carries a cursor for other sources only. Node must
// skip the RPC rather than refetch the remote's first page.
func TestRemoteClientNodeSkipsExhaustedSource(t *testing.T) {
	var remoteCalls int

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	client, err := NewRemoteClient(&resource.Metadata{
		RemoteMixerDomain: ts.URL,
		RemoteMixerAPIKey: "test-api-key",
	})
	if err != nil {
		t.Fatalf("NewRemoteClient() error = %v", err)
	}

	callerToken, err := util.EncodeProto(&pbv2.Pagination{
		Info: []*pbv2.Pagination_DataSourceInfo{{
			Id: "spanner-other",
			DataSourceInfo: &pbv2.Pagination_DataSourceInfo_SpannerInfo{
				SpannerInfo: &pbv2.SpannerInfo{Offset: 100},
			},
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProto() error = %v", err)
	}

	req := &pbv2.NodeRequest{
		Nodes:     []string{"geoId/06"},
		Property:  "->name",
		NextToken: callerToken,
	}
	got, err := client.Node(context.Background(), req)
	if err != nil {
		t.Fatalf("Node() error = %v", err)
	}

	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}
	if diff := cmp.Diff(got, &pbv2.NodeResponse{}, cmpOpts); diff != "" {
		t.Errorf("Node() payload mismatch:\n%v", diff)
	}
	if remoteCalls != 0 {
		t.Errorf("Node() issued %d remote calls, want 0", remoteCalls)
	}
}
