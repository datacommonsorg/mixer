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
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// mockMixerClient implements agent.Mixer for unit testing the cache.
type mockMixerClient struct {
	Mixer
	observationCalls  int32
	mu                sync.Mutex
	requestedEntities []string
	observationFunc   func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error)
}

func (m *mockMixerClient) V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	atomic.AddInt32(&m.observationCalls, 1)

	m.mu.Lock()
	if len(in.GetEntity().GetDcids()) > 0 {
		m.requestedEntities = append(m.requestedEntities, in.GetEntity().GetDcids()[0])
	}
	m.mu.Unlock()

	return m.observationFunc(ctx, in)
}

func TestCacheCheckAvailability(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, tc := range []struct {
		desc                  string
		setupCache            func(*Cache)
		entities              []string
		variables             []string
		mockResponse          *pbv2.ObservationResponse
		wantCalls             int32
		wantEntitiesRequested []string
		wantResult            map[string]map[string]bool
		wantCache             map[string]map[string]struct{}
	}{
		{
			desc: "100% Cache Hit - returns instantly without calling V2Observation",
			setupCache: func(c *Cache) {
				c.entityVars["geoId/06"] = map[string]struct{}{
					"Count_Person":        {},
					"Count_Person_Female": {},
				}
			},
			entities:              []string{"geoId/06"},
			variables:             []string{"Count_Person", "Count_Person_Female", "Count_Person_Male"},
			wantCalls:             0,
			wantEntitiesRequested: nil,
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
			entities:  []string{"geoId/06"},
			variables: []string{"Count_Person", "Count_Person_Male"},
			mockResponse: &pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {},
				},
			},
			wantCalls:             1,
			wantEntitiesRequested: []string{"geoId/06"},
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
			desc: "Partial Cache Hit - fetches missing entity and keeps existing cache",
			setupCache: func(c *Cache) {
				c.entityVars["geoId/06"] = map[string]struct{}{
					"Count_Person": {},
				}
			},
			entities:  []string{"geoId/06", "geoId/17"}, // Illinois is missing
			variables: []string{"Count_Person"},
			mockResponse: &pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {},
				},
			},
			wantCalls:             1,
			wantEntitiesRequested: []string{"geoId/17"}, // Only Illinois is requested
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
						return nil, fmt.Errorf("expected single entity per request for better Redis L2 caching, got: %v", in.GetEntity().GetDcids())
					}
					return tc.mockResponse, nil
				},
			}

			cache := NewCache(mock)
			if tc.setupCache != nil {
				tc.setupCache(cache)
			}

			gotResult, err := cache.CheckAvailability(context.Background(), tc.entities, tc.variables)
			if err != nil {
				t.Fatalf("CheckAvailability failed: %v", err)
			}

			if mock.observationCalls != tc.wantCalls {
				t.Errorf("CheckAvailability triggered %d V2Observation calls, want: %d", mock.observationCalls, tc.wantCalls)
			}

			// Assert that exactly the expected entity DCIDs were requested
			if diff := cmp.Diff(mock.requestedEntities, tc.wantEntitiesRequested); diff != "" {
				t.Errorf("Mock received unexpected entity requests (-got +want):\n%s", diff)
			}

			if diff := cmp.Diff(gotResult, tc.wantResult); diff != "" {
				t.Errorf("CheckAvailability returned unexpected results (-got +want):\n%s", diff)
			}

			if diff := cmp.Diff(cache.entityVars, tc.wantCache, cmpOpts); diff != "" {
				t.Errorf("Cache state after lookup did not match expected state (-got +want):\n%s", diff)
			}
		})
	}
}

func TestCacheEntitiesWithoutData(t *testing.T) {
	withVars := &pbv2.ObservationResponse{ByVariable: map[string]*pbv2.VariableObservation{"Count_Person": {}}}
	for _, tc := range []struct {
		desc                  string
		setupCache            func(*Cache)
		entities              []string
		varsByEntity          map[string]*pbv2.ObservationResponse
		fetchErr              error
		wantWithoutData       []string
		wantErr               bool
		wantEntitiesRequested []string
		wantCache             map[string]map[string]struct{}
	}{
		{
			desc:     "loads uncached entities, returns those without data in request order, and caches all",
			entities: []string{"bogus/1", "geoId/06", "dc/food_bank/1", "bogus/1"},
			varsByEntity: map[string]*pbv2.ObservationResponse{
				"geoId/06": withVars,
			},
			wantWithoutData:       []string{"bogus/1", "dc/food_bank/1"},
			wantEntitiesRequested: []string{"bogus/1", "dc/food_bank/1", "geoId/06"},
			wantCache: map[string]map[string]struct{}{
				"bogus/1":        {},
				"dc/food_bank/1": {},
				"geoId/06":       {"Count_Person": {}},
			},
		},
		{
			desc: "serves cached entities from L1 without fetching",
			setupCache: func(c *Cache) {
				c.entityVars["geoId/06"] = map[string]struct{}{"Count_Person": {}}
				c.entityVars["bogus/1"] = map[string]struct{}{}
			},
			entities:        []string{"geoId/06", "bogus/1"},
			wantWithoutData: []string{"bogus/1"},
			wantCache: map[string]map[string]struct{}{
				"bogus/1":  {},
				"geoId/06": {"Count_Person": {}},
			},
		},
		{
			desc:                  "fetch errors are returned and not cached",
			entities:              []string{"geoId/06"},
			fetchErr:              errors.New("observation backend down"),
			wantErr:               true,
			wantEntitiesRequested: []string{"geoId/06"},
			wantCache:             map[string]map[string]struct{}{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mock := &mockMixerClient{
				observationFunc: func(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
					if tc.fetchErr != nil {
						return nil, tc.fetchErr
					}
					if resp, ok := tc.varsByEntity[in.GetEntity().GetDcids()[0]]; ok {
						return resp, nil
					}
					return &pbv2.ObservationResponse{}, nil
				},
			}
			cache := NewCache(mock)
			if tc.setupCache != nil {
				tc.setupCache(cache)
			}

			got, err := cache.EntitiesWithoutData(context.Background(), tc.entities)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("EntitiesWithoutData() = %v, want error", got)
				}
			} else if err != nil {
				t.Fatalf("EntitiesWithoutData() error = %v", err)
			}
			if diff := cmp.Diff(tc.wantWithoutData, got); diff != "" {
				t.Errorf("EntitiesWithoutData() mismatch (-want +got):\n%s", diff)
			}
			gotRequested := slices.Sorted(slices.Values(mock.requestedEntities))
			if diff := cmp.Diff(tc.wantEntitiesRequested, gotRequested); diff != "" {
				t.Errorf("requested entities mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantCache, cache.entityVars); diff != "" {
				t.Errorf("cache state mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// childTypesMixer implements agent.Mixer ChildTypes for cache tests, recording requested ancestors.
type childTypesMixer struct {
	Mixer
	childTypes map[string][]string
	err        error
	calls      [][]string
}

func (m *childTypesMixer) ChildTypes(ctx context.Context, in *pbv2.ChildTypesRequest) (*pbv2.ChildTypesResponse, error) {
	m.calls = append(m.calls, in.GetAncestors())
	if m.err != nil {
		return nil, m.err
	}
	resp := &pbv2.ChildTypesResponse{ChildTypes: map[string]*pbv2.ChildTypesResponse_ChildTypes{}}
	for _, ancestor := range in.GetAncestors() {
		if types, ok := m.childTypes[ancestor]; ok {
			resp.ChildTypes[ancestor] = &pbv2.ChildTypesResponse_ChildTypes{Types: types}
		}
	}
	return resp, nil
}

func TestCacheChildTypes(t *testing.T) {
	ctx := context.Background()
	mixer := &childTypesMixer{childTypes: map[string][]string{"geoId/06": {"City", "County"}}}
	cache := NewCache(mixer)

	got, err := cache.ChildTypes(ctx, []string{"geoId/06", "nowhere/1"})
	if err != nil {
		t.Fatalf("ChildTypes() error = %v", err)
	}
	want := map[string][]string{"geoId/06": {"City", "County"}, "nowhere/1": {}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("ChildTypes() mismatch (-want +got):\n%s", diff)
	}

	// Both ancestors, including the one without child types, are now cached.
	if _, err := cache.ChildTypes(ctx, []string{"nowhere/1", "geoId/06", "country/KEN"}); err != nil {
		t.Fatalf("ChildTypes() error = %v", err)
	}
	cache.Reset()
	if _, err := cache.ChildTypes(ctx, []string{"geoId/06"}); err != nil {
		t.Fatalf("ChildTypes() error = %v", err)
	}
	wantCalls := [][]string{{"geoId/06", "nowhere/1"}, {"country/KEN"}, {"geoId/06"}}
	if diff := cmp.Diff(wantCalls, mixer.calls); diff != "" {
		t.Errorf("Mixer.ChildTypes calls mismatch (-want +got):\n%s", diff)
	}
}

func TestCacheChildTypesDoesNotCacheErrors(t *testing.T) {
	ctx := context.Background()
	mixer := &childTypesMixer{err: errors.New("LinkedEdge not found")}
	cache := NewCache(mixer)

	if _, err := cache.ChildTypes(ctx, []string{"geoId/06"}); err == nil {
		t.Fatalf("ChildTypes() error = nil, want error")
	}
	// After the failure clears, the ancestor is fetched again rather than served as cached empty.
	mixer.err = nil
	mixer.childTypes = map[string][]string{"geoId/06": {"County"}}
	got, err := cache.ChildTypes(ctx, []string{"geoId/06"})
	if err != nil {
		t.Fatalf("ChildTypes() error = %v", err)
	}
	if diff := cmp.Diff(map[string][]string{"geoId/06": {"County"}}, got); diff != "" {
		t.Errorf("ChildTypes() mismatch (-want +got):\n%s", diff)
	}
	if len(mixer.calls) != 2 {
		t.Errorf("Mixer.ChildTypes called %d times, want 2", len(mixer.calls))
	}
}
