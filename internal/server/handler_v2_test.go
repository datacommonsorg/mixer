// Copyright 2024 Google LLC
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
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/featureflags"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	v2observation "github.com/datacommonsorg/mixer/internal/server/v2/observation"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestUseMetadata(t *testing.T) {
	for _, c := range []struct {
		desc         string
		ctx          context.Context
		wantSurface  string
		wantToRemote bool
	}{
		{
			"empty context",
			context.Background(),
			"",
			false,
		},
		{
			"with surface",
			metadata.NewIncomingContext(
				context.Background(),
				metadata.Pairs("x-surface", "test-surface"),
			),
			"test-surface",
			false,
		},
		{
			"with surface and remote",
			metadata.NewIncomingContext(
				context.Background(),
				metadata.Pairs(
					"x-surface", "test-surface",
					"x-remote", "true",
				),
			),
			"test-surface",
			true,
		},
	} {
		surface, toRemote := util.GetMetadata(c.ctx)
		if diff := cmp.Diff(surface, c.wantSurface); diff != "" {
			t.Errorf("%s: unexpected surface diff %v", c.desc, diff)
		}
		if diff := cmp.Diff(toRemote, c.wantToRemote); diff != "" {
			t.Errorf("%s: unexpected toRemote diff %v", c.desc, diff)
		}
	}
}

// Only tests the empty response case and the queryType, which is used in the usage logs.
func TestObservationInternal(t *testing.T) {
	ctx := context.Background()
	s := &store.Store{}
	c := &cache.Cache{}
	m := resource.Metadata{}
	h := &http.Client{}

	for _, tc := range []struct {
			desc          string
			req           *pbv2.ObservationRequest
			wantQueryType shared.QueryType
			wantResp      *pbv2.ObservationResponse
	}{
		{
			"series",
			&pbv2.ObservationRequest{
				Select: []string{"variable", "entity", "date", "value"},
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"country/USA"},
				},
			},
			shared.QueryTypeValue,
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"country/USA": {}, 
						},
					},
				},
			},
		},
		{
			"collection",
			&pbv2.ObservationRequest{
				Select: []string{"variable", "entity", "date", "value"},
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Expression: "geoId/06<-containedInPlace+{typeOf: City}",
				},
			},
			shared.QueryTypeValue,
			nil,
		},
		{
			"series facet",
			&pbv2.ObservationRequest{
				Select: []string{"variable", "entity", "facet"},
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"country/USA"},
				},
			},
			shared.QueryTypeFacet,
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {}, 
				},
			},
		},
		{
			"collection facet",
			&pbv2.ObservationRequest{
				Select: []string{"variable", "entity", "facet"},
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Expression: "geoId/06<-containedInPlace+{typeOf: City}",
				},
			},
			shared.QueryTypeFacet,
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"": {}, 
						},
					},
				},
			},
		},
		{
			"existence",
			&pbv2.ObservationRequest{
				Select: []string{"variable", "entity"},
				Variable: &pbv2.DcidOrExpression{
					Dcids: []string{"Count_Person"},
				},
				Entity: &pbv2.DcidOrExpression{
					Dcids: []string{"country/USA"},
				},
			},
			shared.QueryTypeExistence,
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {}, 
				},
			},
		},
	} {
		resp, queryType, err := v2observation.ObservationInternal(ctx, s, c, &m, h, tc.req, "")
		if err != nil {
			t.Errorf("ObservationInternal() got err: %v", err)
		}
		if diff := cmp.Diff(queryType, tc.wantQueryType); diff != "" {
			t.Errorf("%s: unexpected queryType diff %v", tc.desc, diff)
		}
		if diff := cmp.Diff(resp, tc.wantResp, protocmp.Transform()); diff != "" {
			t.Errorf("%s: unexpected resp diff %v", tc.desc, diff)
		}
	}
}

func TestV2Observation_UsageLog(t *testing.T) {
	ctx := context.Background()
	s := &Server{
		store:    &store.Store{},
		metadata: &resource.Metadata{},
		flags: &featureflags.Flags{
			WriteUsageLogs: 1.0,
		},
	}
	s.cachedata.Store(&cache.Cache{})
	req := &pbv2.ObservationRequest{
		Select: []string{"variable", "entity", "date", "value"},
		Variable: &pbv2.DcidOrExpression{
			Dcids: []string{"Count_Person"},
		},
		Entity: &pbv2.DcidOrExpression{
			Dcids: []string{"country/USA"},
		},
	}

	// Capture slog output
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)
	logger := slog.New(handler)
	originalLogger := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(originalLogger)

	_, _ = s.V2Observation(ctx, req)

	outStr := strings.TrimSpace(buf.String())

	// Use regex to match the log message, ignoring the timestamp and pointer address.
	wantLogRegex := `time=\S+ level=INFO msg=new_query usage_log.feature="{IsRemote:false Surface:}" usage_log.place_types=[] usage_log.query_type=value usage_log.stat_vars=\[0x[0-9a-f]+\]`
	matched, err := regexp.MatchString(wantLogRegex, outStr)
	if err != nil {
		t.Fatalf("Failed to compile regex: %v", err)
	}
	if !matched {
		t.Errorf("log output did not match expected pattern.\nGot: %s\nWant regex: %s", outStr, wantLogRegex)
	}
}
