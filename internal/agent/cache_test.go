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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// mockMixerClient implements agent.Mixer for unit testing the cache.
type mockMixerClient struct {
	observationCalls int32
	mu               sync.Mutex
	requestedPlaces  []string
	observationFunc  func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error)
}

func (m *mockMixerClient) V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	return nil, nil
}

func (m *mockMixerClient) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return nil, nil
}

func (m *mockMixerClient) V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	atomic.AddInt32(&m.observationCalls, 1)

	m.mu.Lock()
	if len(in.GetEntity().GetDcids()) > 0 {
		m.requestedPlaces = append(m.requestedPlaces, in.GetEntity().GetDcids()[0])
	}
	m.mu.Unlock()

	return m.observationFunc(ctx, in)
}

func TestCacheCheckAvailability(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, tc := range []struct {
		desc                string
		setupCache          func(*Cache)
		places              []string
		variables           []string
		mockResponse        *pbv2.ObservationResponse
		wantCalls           int32
		wantPlacesRequested []string
		wantResult          map[string]map[string]bool
		wantCache           map[string]map[string]struct{}
	}{
		{
			desc: "100% Cache Hit - returns instantly without calling V2Observation",
			setupCache: func(c *Cache) {
				c.placeVars["geoId/06"] = map[string]struct{}{
					"Count_Person":        {},
					"Count_Person_Female": {},
				}
			},
			places:              []string{"geoId/06"},
			variables:           []string{"Count_Person", "Count_Person_Female", "Count_Person_Male"},
			wantCalls:           0,
			wantPlacesRequested: nil,
			wantResult: map[string]map[string]bool{
				"geoId/06": {
					"Count_Person":        true,
					"Count_Person_Female": true,
					"Count_Person_Male":   false,
				},
			},
			wantCache: map[string]map[string]struct{}{
				"geoId/06": {
					"Count_Person":        {},
					"Count_Person_Female": {},
				},
			},
		},
		{
			desc: "Cache Miss - triggers V2Observation wildcard query and caches results",
			setupCache: func(c *Cache) {
				// Cache empty
			},
			places:              []string{"geoId/06"},
			variables:           []string{"Count_Person", "Count_Person_Male"},
			mockResponse: &pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {},
				},
			},
			wantCalls:           1,
			wantPlacesRequested: []string{"geoId/06"},
			wantResult: map[string]map[string]bool{
				"geoId/06": {
					"Count_Person":      true,
					"Count_Person_Male": false,
				},
			},
			wantCache: map[string]map[string]struct{}{
				"geoId/06": {
					"Count_Person": {},
				},
			},
		},
		{
			desc: "Partial Cache Hit - fetches missing place and keeps existing cache",
			setupCache: func(c *Cache) {
				c.placeVars["geoId/06"] = map[string]struct{}{
					"Count_Person": {},
				}
			},
			places:              []string{"geoId/06", "geoId/17"}, // Illinois is missing
			variables:           []string{"Count_Person"},
			mockResponse: &pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {},
				},
			},
			wantCalls:           1,
			wantPlacesRequested: []string{"geoId/17"}, // Only Illinois is requested
			wantResult: map[string]map[string]bool{
				"geoId/06": {
					"Count_Person": true,
				},
				"geoId/17": {
					"Count_Person": true,
				},
			},
			wantCache: map[string]map[string]struct{}{
				"geoId/06": {
					"Count_Person": {},
				},
				"geoId/17": {
					"Count_Person": {},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mock := &mockMixerClient{
				observationFunc: func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
					// Verify wildcard constraints
					if len(in.GetVariable().GetDcids()) > 0 {
						return nil, fmt.Errorf("expected wildcard request without variables, got: %v", in.GetVariable().GetDcids())
					}
					if len(in.GetEntity().GetDcids()) != 1 {
						return nil, fmt.Errorf("expected single place per request for better Redis L2 caching, got: %v", in.GetEntity().GetDcids())
					}
					return tc.mockResponse, nil
				},
			}

			cache := NewCache(mock)
			if tc.setupCache != nil {
				tc.setupCache(cache)
			}

			gotResult, err := cache.CheckAvailability(context.Background(), tc.places, tc.variables)
			if err != nil {
				t.Fatalf("CheckAvailability failed: %v", err)
			}

			if mock.observationCalls != tc.wantCalls {
				t.Errorf("CheckAvailability triggered %d V2Observation calls, want: %d", mock.observationCalls, tc.wantCalls)
			}

			// Assert that exactly the expected place DCIDs were requested
			if diff := cmp.Diff(mock.requestedPlaces, tc.wantPlacesRequested); diff != "" {
				t.Errorf("Mock received unexpected place requests (-got +want):\n%s", diff)
			}

			if diff := cmp.Diff(gotResult, tc.wantResult); diff != "" {
				t.Errorf("CheckAvailability returned unexpected results (-got +want):\n%s", diff)
			}

			if diff := cmp.Diff(cache.placeVars, tc.wantCache, cmpOpts); diff != "" {
				t.Errorf("Cache state after lookup did not match expected state (-got +want):\n%s", diff)
			}
		})
	}
}
