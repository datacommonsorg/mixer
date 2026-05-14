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
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

type mockDataSource struct {
	datasource.DataSource
}

func (m *mockDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
	return &pbv2.NodeResponse{}, nil
}

func TestTopicCacheManagerInMemory(t *testing.T) {
	ctx := context.Background()

	// Setup empty mock datasource since this test only verifies in-memory cache state and mutex locking
	ds := &mockDataSource{}
	manager := NewTopicCacheManager(ds, nil)

	// Assert initial state is empty
	if got := manager.GetHierarchy(); got != nil {
		t.Fatalf("GetHierarchy() should be nil initially")
	}

	// Assert LoadHierarchy synchronously loads and updates the cache
	hierarchy, err := manager.LoadHierarchy(ctx)
	if err != nil {
		t.Fatalf("LoadHierarchy() failed: %v", err)
	}
	if hierarchy == nil {
		t.Fatalf("LoadHierarchy() returned nil")
	}

	// Verify in-memory cache is updated
	cached := manager.GetHierarchy()
	if cached == nil {
		t.Fatalf("GetHierarchy() returned nil after load")
	}

	// Assert that loading again hits the in-memory cache directly and matches
	secondLoad, err := manager.LoadHierarchy(ctx)
	if err != nil {
		t.Fatalf("Second LoadHierarchy() failed: %v", err)
	}
	if secondLoad != cached {
		t.Errorf("Second LoadHierarchy() should return the exact same cached pointer")
	}
}
