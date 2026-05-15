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

package golden

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/internal/server/topic"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFetchTopicsFromKGGolden(t *testing.T) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	// The golden file is in the same directory as this test file
	goldenDir := path.Dir(filename)
	goldenFile := "topic_cache.json"

	// Instantiate actual test SpannerClient pointing to the real database
	client := test.NewSpannerClient()
	if client == nil {
		t.Skip("Skipping TestFetchTopicsFromKGGolden (ENABLE_SPANNER_GRAPH not set or spanner database unavailable)")
	}

	// Instantiate real SpannerDataSource wrapper with the real database client
	ds := spanner.NewSpannerDataSource(client, nil, nil, false)
	manager := topic.NewTopicCacheManager(ds, nil)

	got, err := manager.FetchTopicsFromKG(ctx)
	if err != nil {
		t.Fatalf("FetchTopicsFromKG() failed: %v", err)
	}

	// Trim relevant variables to at most 3 elements to keep the golden JSON size compact
	for _, node := range got.Topics {
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

	var want *pb.TopicHierarchy
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatalf("Failed to unmarshal golden: %v", err)
	}

	if diff := cmp.Diff(got, want, protocmp.Transform()); diff != "" {
		t.Errorf("FetchTopicsFromKG() golden mismatch (-got +want):\n%s", diff)
	}
}
