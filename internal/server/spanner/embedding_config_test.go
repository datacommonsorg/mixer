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

package spanner

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSpannerSearchConfig_JSON(t *testing.T) {
	data := []byte(`{
		"search_config": {
			"search_algorithm": "vector_search",
			"embedding_model": "text-embedding-005",
			"query_task_type": "RETRIEVAL_QUERY",
			"embedding_label": "base_text_embedding",
			"vector_search_algo": "ANN"
		},
		"postprocessing": ["none"]
	}`)

	var cfg SpannerSearchConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("Failed to unmarshal JSON into SpannerSearchConfig: %v", err)
	}

	if cfg.SearchConfig.SearchAlgorithm != VectorSearch {
		t.Errorf("Expected SearchAlgorithm=%s, got %s", VectorSearch, cfg.SearchConfig.SearchAlgorithm)
	}
	if cfg.SearchConfig.VectorSearchAlgo != VectorSearchAlgoANN {
		t.Errorf("Expected VectorSearchAlgo=%s, got %s", VectorSearchAlgoANN, cfg.SearchConfig.VectorSearchAlgo)
	}
	if cfg.SearchConfig.EmbeddingModel != "text-embedding-005" {
		t.Errorf("Expected EmbeddingModel=text-embedding-005, got %s", cfg.SearchConfig.EmbeddingModel)
	}
	if cfg.SearchConfig.QueryTaskType != QueryTaskTypeRetrievalQuery {
		t.Errorf("Expected QueryTaskType=%s, got %s", QueryTaskTypeRetrievalQuery, cfg.SearchConfig.QueryTaskType)
	}
	if cfg.SearchConfig.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %s", cfg.SearchConfig.EmbeddingLabel)
	}
	if len(cfg.Postprocessing) != 1 || cfg.Postprocessing[0] != PostprocessingNone {
		t.Errorf("Expected Postprocessing=[none], got %v", cfg.Postprocessing)
	}
}

func TestGetSpannerSearchConfigPath(t *testing.T) {
	path := GetSpannerSearchConfigPath("default")
	if !strings.HasSuffix(path, "internal/server/spanner/spanner_config/default.yaml") {
		t.Errorf("Unexpected path suffix: %s", path)
	}

	dcpPath := GetSpannerSearchConfigPath("dcp_default")
	if !strings.HasSuffix(dcpPath, "internal/server/spanner/spanner_config/dcp_default.yaml") {
		t.Errorf("Unexpected dcp_default path suffix: %s", dcpPath)
	}
}

func TestReadSpannerSearchConfig(t *testing.T) {
	path := GetSpannerSearchConfigPath("default")
	cfg, err := ReadSpannerSearchConfig(path)
	if err != nil {
		t.Fatalf("Failed to read SpannerSearchConfig: %v", err)
	}
	if cfg.SearchConfig.SearchAlgorithm != VectorSearch {
		t.Errorf("Expected SearchAlgorithm=%s, got %s", VectorSearch, cfg.SearchConfig.SearchAlgorithm)
	}
	if cfg.SearchConfig.VectorSearchAlgo != VectorSearchAlgoANN {
		t.Errorf("Expected VectorSearchAlgo=%s, got %s", VectorSearchAlgoANN, cfg.SearchConfig.VectorSearchAlgo)
	}
	if cfg.SearchConfig.EmbeddingModel != "NodeEmbeddingModel" {
		t.Errorf("Expected EmbeddingModel=NodeEmbeddingModel, got %s", cfg.SearchConfig.EmbeddingModel)
	}
	if cfg.SearchConfig.QueryTaskType != QueryTaskTypeRetrievalQuery {
		t.Errorf("Expected QueryTaskType=%s, got %s", QueryTaskTypeRetrievalQuery, cfg.SearchConfig.QueryTaskType)
	}
	if cfg.SearchConfig.EmbeddingLabel != "nl_stat_var_embedding" {
		t.Errorf("Expected EmbeddingLabel=nl_stat_var_embedding, got %s", cfg.SearchConfig.EmbeddingLabel)
	}
	if len(cfg.Postprocessing) != 1 || cfg.Postprocessing[0] != PostprocessingNone {
		t.Errorf("Expected Postprocessing=[none], got %v", cfg.Postprocessing)
	}
}

func TestReadSpannerSearchConfig_DCPDefault(t *testing.T) {
	path := GetSpannerSearchConfigPath("dcp_default")
	cfg, err := ReadSpannerSearchConfig(path)
	if err != nil {
		t.Fatalf("Failed to read DCP default SpannerSearchConfig: %v", err)
	}
	if cfg.SearchConfig.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %s", cfg.SearchConfig.EmbeddingLabel)
	}
}

func TestLoadSpannerSearchConfig(t *testing.T) {
	// Test loading via short profile name
	cfgProfile, err := loadSpannerSearchConfig("dcp_default")
	if err != nil {
		t.Fatalf("Failed to load search config via profile name: %v", err)
	}
	if cfgProfile.SearchConfig.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %s", cfgProfile.SearchConfig.EmbeddingLabel)
	}

	// Test fallback to default on invalid path
	cfgFallback, err := loadSpannerSearchConfig("/non/existent/path.yaml")
	if err != nil {
		t.Fatalf("Expected fallback to default config, got error: %v", err)
	}
	if cfgFallback.SearchConfig.EmbeddingLabel != "nl_stat_var_embedding" {
		t.Errorf("Expected fallback EmbeddingLabel=nl_stat_var_embedding, got %s", cfgFallback.SearchConfig.EmbeddingLabel)
	}
}

func TestNewSpannerDataSource_SearchConfigPath(t *testing.T) {
	opts := &SpannerDataSourceOptions{
		SearchConfigPath: "dcp_default",
	}
	ds := NewSpannerDataSource(nil, opts)
	if ds.searchConfig == nil {
		t.Fatalf("Expected searchConfig to be initialized, got nil")
	}
	if ds.searchConfig.SearchConfig.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %s", ds.searchConfig.SearchConfig.EmbeddingLabel)
	}
}


