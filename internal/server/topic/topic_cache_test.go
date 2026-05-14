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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

type mockDataSource struct {
	datasource.DataSource
	mu        sync.Mutex
	callCount int
}

func (m *mockDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()
	return &pbv2.NodeResponse{}, nil
}

func TestTopicCacheManagerInMemory(t *testing.T) {
	ctx := context.Background()

	// Setup empty mock datasource since this test only verifies in-memory cache state and mutex locking
	ds := &mockDataSource{}
	manager := NewTopicCacheManager(ds, nil)

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

	ds := &mockDataSource{}
	manager := NewTopicCacheManager(ds, nil)

	// Start refresher with a fast interval
	manager.Start(ctx, 10*time.Millisecond)

	// Let ticker fire a couple of times
	time.Sleep(35 * time.Millisecond)

	// Verify Close shuts down goroutines cleanly
	manager.Close()

	ds.mu.Lock()
	count := ds.callCount
	ds.mu.Unlock()

	if count < 2 {
		t.Errorf("Expected refresher to trigger LoadHierarchy at least 2 times, got %d", count)
	}
}
