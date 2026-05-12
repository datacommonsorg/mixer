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
	"encoding/json"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
)

func TestFetchTopicsFromKGGolden(t *testing.T) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "golden")
	goldenFile := "topic_cache.json"

	// Instantiate actual test SpannerClient pointing to the real database
	client := test.NewSpannerClient()
	if client == nil {
		t.Skip("Skipping TestFetchTopicsFromKGGolden (ENABLE_SPANNER_GRAPH not set or spanner database unavailable)")
	}

	// Instantiate real SpannerDataSource wrapper with the real database client
	ds := spanner.NewSpannerDataSource(client, nil, nil, false)
	manager := NewTopicCacheManager(ds)

	got, err := manager.fetchTopicsFromKG(ctx)
	if err != nil {
		t.Fatalf("fetchTopicsFromKG() failed: %v", err)
	}

	// Trim relevant variables to at most 3 elements to keep the golden JSON size compact
	for _, node := range got {
		if len(node.RelevantVariables) > 3 {
			node.RelevantVariables = node.RelevantVariables[:3]
		}
	}

	// Generate golden output if GENERATE_GOLDEN is true
	if test.GenerateGolden {
		if err := os.MkdirAll(goldenDir, 0755); err != nil {
			t.Fatalf("Failed to create golden directory: %v", err)
		}
		data, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatalf("Failed to marshal response: %v", err)
		}
		if err := os.WriteFile(path.Join(goldenDir, goldenFile), data, 0644); err != nil {
			t.Fatalf("Failed to write golden file: %v", err)
		}
		t.Logf("Golden file updated: %s", goldenFile)
		return
	}

	// Read and assert against stored golden JSON file
	goldenPath := path.Join(goldenDir, goldenFile)
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("Failed to read golden file: %v", err)
	}

	var want map[string]*TopicNode
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatalf("Failed to unmarshal golden: %v", err)
	}

	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("fetchTopicsFromKG() golden mismatch (-got +want):\n%s", diff)
	}
}
