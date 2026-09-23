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
		"search_configs": {
			"indicator": {
				"search_algorithm": "vector_search",
				"embedding_model": "text-embedding-005",
				"query_task_type": "RETRIEVAL_QUERY",
				"embedding_label": "base_text_embedding",
				"vector_search_algo": "ANN"
			}
		},
		"postprocessing": ["none"]
	}`)

	var cfg SpannerSearchConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("Failed to unmarshal JSON into SpannerSearchConfig: %v", err)
	}

	sc, ok := cfg.SearchConfigs["indicator"]
	if !ok {
		t.Fatalf("Expected SearchConfig for key indicator")
	}
	if sc.SearchAlgorithm != VectorSearch {
		t.Errorf("Expected SearchAlgorithm=%s, got %s", VectorSearch, sc.SearchAlgorithm)
	}
	if sc.VectorSearchAlgo != VectorSearchAlgoANN {
		t.Errorf("Expected VectorSearchAlgo=%s, got %s", VectorSearchAlgoANN, sc.VectorSearchAlgo)
	}
	if sc.EmbeddingModel != "text-embedding-005" {
		t.Errorf("Expected EmbeddingModel=text-embedding-005, got %s", sc.EmbeddingModel)
	}
	if sc.QueryTaskType != QueryTaskTypeRetrievalQuery {
		t.Errorf("Expected QueryTaskType=%s, got %s", QueryTaskTypeRetrievalQuery, sc.QueryTaskType)
	}
	if sc.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %s", sc.EmbeddingLabel)
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
	if len(cfg.SearchConfigs) != 2 {
		t.Fatalf("Expected 2 search configs, got %d", len(cfg.SearchConfigs))
	}

	indicatorCfg, ok := cfg.SearchConfigs["indicator"]
	if !ok {
		t.Fatalf("Expected SearchConfig for key indicator")
	}
	if indicatorCfg.SearchAlgorithm != VectorSearch {
		t.Errorf("Expected SearchAlgorithm=%s, got %s", VectorSearch, indicatorCfg.SearchAlgorithm)
	}
	if indicatorCfg.VectorSearchAlgo != VectorSearchAlgoANN {
		t.Errorf("Expected VectorSearchAlgo=%s, got %s", VectorSearchAlgoANN, indicatorCfg.VectorSearchAlgo)
	}
	if indicatorCfg.EmbeddingModel != "NodeEmbeddingModel" {
		t.Errorf("Expected EmbeddingModel=NodeEmbeddingModel, got %s", indicatorCfg.EmbeddingModel)
	}
	if indicatorCfg.EmbeddingModelEndpoint != "text-embedding-005" {
		t.Errorf("Expected EmbeddingModelEndpoint=text-embedding-005, got %s", indicatorCfg.EmbeddingModelEndpoint)
	}
	if indicatorCfg.QueryTaskType != QueryTaskTypeRetrievalQuery {
		t.Errorf("Expected QueryTaskType=%s, got %s", QueryTaskTypeRetrievalQuery, indicatorCfg.QueryTaskType)
	}
	if indicatorCfg.EmbeddingLabel != "nl_stat_var_embedding" {
		t.Errorf("Expected EmbeddingLabel=nl_stat_var_embedding, got %s", indicatorCfg.EmbeddingLabel)
	}

	nonPlaceCfg, ok := cfg.SearchConfigs["non_place_entity"]
	if !ok {
		t.Fatalf("Expected SearchConfig for key non_place_entity")
	}
	if nonPlaceCfg.EmbeddingLabel != "non_place_entity_experimental_embedding" {
		t.Errorf("Expected EmbeddingLabel=non_place_entity_experimental_embedding, got %s", nonPlaceCfg.EmbeddingLabel)
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
	if len(cfg.SearchConfigs) != 1 {
		t.Fatalf("Expected 1 search config, got %d", len(cfg.SearchConfigs))
	}
	indicatorSc, ok := cfg.SearchConfigs["indicator"]
	if !ok {
		t.Fatalf("Expected SearchConfig for indicator")
	}
	if indicatorSc.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %s", indicatorSc.EmbeddingLabel)
	}
}

func TestLoadSpannerSearchConfig(t *testing.T) {
	// Test loading via short profile name
	cfgProfile, err := loadSpannerSearchConfig("dcp_default")
	if err != nil {
		t.Fatalf("Failed to load search config via profile name: %v", err)
	}
	sc, ok := cfgProfile.SearchConfigs["indicator"]
	if !ok || sc.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %v", sc)
	}

	// Test error on invalid path
	_, err = loadSpannerSearchConfig("/non/existent/path.yaml")
	if err == nil {
		t.Fatalf("Expected error when loading non-existent search config path, got nil")
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
	sc, ok := ds.searchConfig.SearchConfigs["indicator"]
	if !ok || sc.EmbeddingLabel != "base_text_embedding" {
		t.Errorf("Expected EmbeddingLabel=base_text_embedding, got %v", sc)
	}
}


