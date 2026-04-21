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
	"strings"
	"testing"
)

func TestSpannerSearchConfig_JSON(t *testing.T) {
	data := []byte(`{
		"search_algorithm": "vector_search",
		"vector_search_config": {
			"embedding_model": "text-embedding-005",
			"embedding_type": "RETRIEVAL_QUERY",
			"vector_search_algo": "ANN"
		},
		"postprocessing": ["none"]
	}`)

	var cfg SpannerSearchConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("Failed to unmarshal JSON into SpannerSearchConfig: %v", err)
	}

	if cfg.SearchAlgorithm != VectorSearch {
		t.Errorf("Expected SearchAlgorithm=%s, got %s", VectorSearch, cfg.SearchAlgorithm)
	}
	if cfg.VectorSearchConfig.VectorSearchAlgo != VectorSearchAlgoANN {
		t.Errorf("Expected VectorSearchAlgo=%s, got %s", VectorSearchAlgoANN, cfg.VectorSearchConfig.VectorSearchAlgo)
	}
	if cfg.VectorSearchConfig.EmbeddingModel != "text-embedding-005" {
		t.Errorf("Expected EmbeddingModel=text-embedding-005, got %s", cfg.VectorSearchConfig.EmbeddingModel)
	}
	if cfg.VectorSearchConfig.EmbeddingType != EmbeddingTypeRetrievalQuery {
		t.Errorf("Expected EmbeddingType=%s, got %s", EmbeddingTypeRetrievalQuery, cfg.VectorSearchConfig.EmbeddingType)
	}
	if len(cfg.Postprocessing) != 1 || cfg.Postprocessing[0] != PostprocessingNone {
		t.Errorf("Expected Postprocessing=[none], got %v", cfg.Postprocessing)
	}
}

func TestGetSpannerSearchConfigPath(t *testing.T) {
	path := GetSpannerSearchConfigPath("default")
	if !strings.HasSuffix(path, "internal/server/v2/resolve/spanner_config/default.yaml") {
		t.Errorf("Unexpected path suffix: %s", path)
	}
}

func TestReadSpannerSearchConfig(t *testing.T) {
	path := GetSpannerSearchConfigPath("default")
	cfg, err := ReadSpannerSearchConfig(path)
	if err != nil {
		t.Fatalf("Failed to read SpannerSearchConfig: %v", err)
	}
	if cfg.SearchAlgorithm != VectorSearch {
		t.Errorf("Expected SearchAlgorithm=%s, got %s", VectorSearch, cfg.SearchAlgorithm)
	}
	if cfg.VectorSearchConfig.VectorSearchAlgo != VectorSearchAlgoANN {
		t.Errorf("Expected VectorSearchAlgo=%s, got %s", VectorSearchAlgoANN, cfg.VectorSearchConfig.VectorSearchAlgo)
	}
	if cfg.VectorSearchConfig.EmbeddingModel != "TextEmbeddings" {
		t.Errorf("Expected EmbeddingModel=TextEmbeddings, got %s", cfg.VectorSearchConfig.EmbeddingModel)
	}
	if cfg.VectorSearchConfig.EmbeddingType != EmbeddingTypeRetrievalQuery {
		t.Errorf("Expected EmbeddingType=%s, got %s", EmbeddingTypeRetrievalQuery, cfg.VectorSearchConfig.EmbeddingType)
	}
	if len(cfg.Postprocessing) != 1 || cfg.Postprocessing[0] != PostprocessingNone {
		t.Errorf("Expected Postprocessing=[none], got %v", cfg.Postprocessing)
	}
}
