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

package resolve

import (
	"encoding/json"
	"testing"
)

func TestSpannerSearchConfig_JSON(t *testing.T) {
	data := []byte(`{
		"search_algorithm": "vector_search",
		"embedding_config": {
			"embedding_model": "text-embedding-005",
			"embedding_type": "RETRIEVAL_QUERY",
			"embedding_dimension": 768
		},
		"graph_mode": "default"
	}`)

	var cfg SpannerSearchConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("Failed to unmarshal JSON into SpannerSearchConfig: %v", err)
	}

	if cfg.SearchAlgorithm != VectorSearch {
		t.Errorf("Expected SearchAlgorithm=%s, got %s", VectorSearch, cfg.SearchAlgorithm)
	}
	if cfg.EmbeddingConfig.EmbeddingModel != "text-embedding-005" {
		t.Errorf("Expected EmbeddingModel=text-embedding-005, got %s", cfg.EmbeddingConfig.EmbeddingModel)
	}
	if cfg.EmbeddingConfig.EmbeddingDimension != 768 {
		t.Errorf("Expected EmbeddingDimension=768, got %d", cfg.EmbeddingConfig.EmbeddingDimension)
	}
	if cfg.GraphMode != GraphConfigDefault {
		t.Errorf("Expected GraphMode=%s, got %s", GraphConfigDefault, cfg.GraphMode)
	}
}
