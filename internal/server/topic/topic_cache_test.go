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

package topic

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

type mockNodeFetcher struct {
	mu        sync.Mutex
	callCount int
	resps     map[string]*pbv2.LinkedGraph
}

func (m *mockNodeFetcher) NodeFetchAll(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()
	if m.resps != nil {
		return &pbv2.NodeResponse{Data: m.resps}, nil
	}
	return &pbv2.NodeResponse{}, nil
}

func TestTopicCacheManagerInMemory(t *testing.T) {
	ctx := context.Background()

	fetcher := &mockNodeFetcher{}
	manager := NewTopicCacheManager(nil)
	manager.InitFetcher(fetcher)

	// Assert initial state is empty
	if got := manager.CachedHierarchy(); got != nil {
		t.Fatalf("CachedHierarchy() should be nil initially")
	}

	// Assert GetHierarchy synchronously loads and updates the cache
	hierarchy, err := manager.GetHierarchy(ctx)
	if err != nil {
		t.Fatalf("GetHierarchy() failed: %v", err)
	}
	if hierarchy == nil {
		t.Fatalf("GetHierarchy() returned nil")
	}

	// Verify in-memory cache is updated
	cached := manager.CachedHierarchy()
	if cached == nil {
		t.Fatalf("CachedHierarchy() returned nil after load")
	}

	// Assert that loading again hits the in-memory cache directly and matches
	secondLoad, err := manager.GetHierarchy(ctx)
	if err != nil {
		t.Fatalf("Second GetHierarchy() failed: %v", err)
	}
	if secondLoad != cached {
		t.Errorf("Second GetHierarchy() should return the exact same cached pointer")
	}
}

func TestTopicCacheManagerRefresher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetcher := &mockNodeFetcher{}
	manager := NewTopicCacheManager(nil)

	// Start refresher with a fast interval
	manager.Start(ctx, fetcher, 10*time.Millisecond)

	// Let ticker fire a couple of times
	time.Sleep(35 * time.Millisecond)

	// Verify Close shuts down goroutines cleanly
	manager.Close()

	fetcher.mu.Lock()
	count := fetcher.callCount
	fetcher.mu.Unlock()

	if count < 2 {
		t.Errorf("Expected refresher to trigger LoadHierarchy at least 2 times, got %d", count)
	}
}

func TestGetStatVarInfos(t *testing.T) {
	ctx := context.Background()
	resps := map[string]*pbv2.LinkedGraph{
		"Count_Person": {
			Arcs: map[string]*pbv2.Nodes{
				"name": {
					Nodes: []*pb.EntityInfo{
						{Name: "Person Count"},
					},
				},
				"observationProperty": {
					Nodes: []*pb.EntityInfo{
						{Value: "statVarObservation"},
					},
				},
				"entityMapping": {
					Nodes: []*pb.EntityInfo{
						{Value: "Count_Person_Column"},
					},
				},
			},
		},
	}
	fetcher := &mockNodeFetcher{resps: resps}
	manager := NewTopicCacheManager(nil)
	manager.InitFetcher(fetcher)

	// Update cache with empty hierarchy to initialize m.cache
	manager.Update(&pb.TopicHierarchy{})

	infos, err := manager.GetStatVarInfos(ctx, []string{"Count_Person"})
	if err != nil {
		t.Fatalf("GetStatVarInfos failed: %v", err)
	}
	info, ok := infos["Count_Person"]
	if !ok || info.Name != "Person Count" || len(info.ObservationProperties) == 0 || info.ObservationProperties[0] != "statVarObservation" {
		t.Errorf("GetStatVarInfos mismatch: got %+v", info)
	}

	// Fetch again to verify cache hit
	beforeCount := fetcher.callCount
	_, _ = manager.GetStatVarInfos(ctx, []string{"Count_Person"})
	if fetcher.callCount != beforeCount {
		t.Errorf("Expected cache hit, but callCount increased from %d to %d", beforeCount, fetcher.callCount)
	}
}
