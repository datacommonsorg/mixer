// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

type versionTimestampProvider struct {
	timestamp time.Time
	err       error
}

func (p *versionTimestampProvider) SpannerStalenessTimestamp() (time.Time, error) {
	return p.timestamp, p.err
}

func TestGetVersionSpannerStalenessTimestamp(t *testing.T) {
	want := time.Date(2026, time.July, 29, 10, 21, 34, 123456000, time.UTC)
	server := newVersionTestServer(&versionTimestampProvider{timestamp: want})

	got, err := server.GetVersion(context.Background(), &pb.GetVersionRequest{})
	if err != nil {
		t.Fatalf("GetVersion() error = %v", err)
	}
	if got.GetSpannerStalenessTimestamp() == nil || !got.GetSpannerStalenessTimestamp().AsTime().Equal(want) {
		t.Fatalf("GetVersion().spanner_staleness_timestamp = %v, want %v", got.GetSpannerStalenessTimestamp(), want)
	}

	jsonResponse, err := protojson.Marshal(got)
	if err != nil {
		t.Fatalf("protojson.Marshal() error = %v", err)
	}
	wantJSON := `"spannerStalenessTimestamp":"2026-07-29T10:21:34.123456Z"`
	if !strings.Contains(string(jsonResponse), wantJSON) {
		t.Fatalf("GetVersion() JSON = %s, want field %s", jsonResponse, wantJSON)
	}
}

func TestGetVersionOmitsUnavailableSpannerStalenessTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider SpannerStalenessTimestampProvider
	}{
		{name: "missing provider"},
		{name: "timestamp error", provider: &versionTimestampProvider{err: errors.New("timestamp unavailable")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newVersionTestServer(tc.provider)
			got, err := server.GetVersion(context.Background(), &pb.GetVersionRequest{})
			if err != nil {
				t.Fatalf("GetVersion() error = %v", err)
			}
			if got.GetSpannerStalenessTimestamp() != nil {
				t.Fatalf("GetVersion().spanner_staleness_timestamp = %v, want nil", got.GetSpannerStalenessTimestamp())
			}
		})
	}
}

func newVersionTestServer(provider SpannerStalenessTimestampProvider) *Server {
	sources := datasources.NewDataSources(nil, nil)
	return &Server{
		store:                             &store.Store{},
		metadata:                          &resource.Metadata{},
		dispatcher:                        dispatcher.NewDispatcher(nil, sources),
		flags:                             nil,
		spannerStalenessTimestampProvider: provider,
	}
}

func TestDeprecatedPlaceRankingEndpointsReturnEmpty(t *testing.T) {
	ctx := context.Background()
	// Configure a remote Mixer so that any attempt to federate is recorded.
	transport := &mockRemoteTransport{}
	s := &Server{
		store:      &store.Store{},
		metadata:   &resource.Metadata{RemoteMixerDomain: "http://mock-remote"},
		httpClient: &http.Client{Transport: transport},
	}
	rankingsReq := &pb.GetLocationsRankingsRequest{
		PlaceType:    "County",
		WithinPlace:  "geoId/06",
		StatVarDcids: []string{"Count_Person"},
	}

	for _, tc := range []struct {
		name string
		call func() (proto.Message, error)
		want proto.Message
	}{
		{
			name: "GetLocationsRankings",
			call: func() (proto.Message, error) { return s.GetLocationsRankings(ctx, rankingsReq) },
			want: &pb.GetLocationsRankingsResponse{},
		},
		{
			name: "V2GetLocationsRankings",
			call: func() (proto.Message, error) { return s.V2GetLocationsRankings(ctx, rankingsReq) },
			want: &pb.GetLocationsRankingsResponse{},
		},
		{
			name: "GetRelatedLocations",
			call: func() (proto.Message, error) {
				return s.GetRelatedLocations(ctx, &pb.GetRelatedLocationsRequest{
					Dcid:         "geoId/06085",
					StatVarDcids: []string{"Count_Person"},
				})
			},
			want: &pb.GetRelatedLocationsResponse{},
		},
		// Requests with missing arguments also get an empty response, not an error.
		{
			name: "GetLocationsRankings empty request",
			call: func() (proto.Message, error) {
				return s.GetLocationsRankings(ctx, &pb.GetLocationsRankingsRequest{})
			},
			want: &pb.GetLocationsRankingsResponse{},
		},
		{
			name: "V2GetLocationsRankings empty request",
			call: func() (proto.Message, error) {
				return s.V2GetLocationsRankings(ctx, &pb.GetLocationsRankingsRequest{})
			},
			want: &pb.GetLocationsRankingsResponse{},
		},
		{
			name: "GetRelatedLocations empty request",
			call: func() (proto.Message, error) {
				return s.GetRelatedLocations(ctx, &pb.GetRelatedLocationsRequest{})
			},
			want: &pb.GetRelatedLocationsResponse{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport.lastPath = ""
			got, err := tc.call()
			if err != nil {
				t.Fatalf("%s() error = %v", tc.name, err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("%s() unexpected diff (-want +got):\n%s", tc.name, diff)
			}
			if transport.lastPath != "" {
				t.Errorf("%s() called remote Mixer path %q, want no remote call", tc.name, transport.lastPath)
			}
		})
	}
}
