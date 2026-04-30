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
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

// SearchMethod defines the type of search algorithm to use. Either vector search, full text search, or hybrid search.
// The SearchMethod will be mapped with related SQL query/DDL statements to be executed in Query.
type SearchMethod string

const (
	VectorSearch   SearchMethod = "vector_search" // search by embeddings
	FullTextSearch SearchMethod = "full_text_search" // search by term match
	HybridSearch   SearchMethod = "hybrid_search" // combine vector search and full text search
)

// VectorSearchAlgo defines whether to use exact KNN or approximate ANN search.
type VectorSearchAlgo string

const (
	VectorSearchAlgoKNN VectorSearchAlgo = "KNN" // Exact Nearest Neighbor, slow, scan through the full dataset.
	VectorSearchAlgoANN VectorSearchAlgo = "ANN" // Approximate Nearest Neighbor, fast and default method
)

// EmbeddingType defines the task type for the embedding model (e.g., RETRIEVAL_QUERY) to generate embeddings for the term/node to resolve.
// Supported types can be found in this document: https://ai.google.dev/gemini-api/docs/embeddings#supported-task-types
type EmbeddingType string

const (
	EmbeddingTypeRetrievalQuery      EmbeddingType = "RETRIEVAL_QUERY" // embedding task optimized for query to search from
	EmbeddingTypeRetrievalDocument   EmbeddingType = "RETRIEVAL_DOCUMENT" // embedding task optimized for document to search on
	EmbeddingTypeSemanticSimilarity EmbeddingType = "SEMANTIC_SIMILARITY" // embedding task optimized for finding semantic
)

// VectorSearchConfig holds the configuration for the parameters necessary to vector search.
type VectorSearchConfig struct {
	EmbeddingModel   string           `json:"embedding_model" yaml:"embedding_model"` // the model name registered in spanner to invoke
	EmbeddingTable   string           `json:"embedding_table" yaml:"embedding_table"` // the table name in spanner for embeddings
	EmbeddingType    EmbeddingType    `json:"embedding_type" yaml:"embedding_type"`
	VectorSearchAlgo VectorSearchAlgo `json:"vector_search_algo" yaml:"vector_search_algo"`
	Limit            int              `json:"limit" yaml:"limit"`
	NumLeaves        int              `json:"num_leaves" yaml:"num_leaves"`
	Threshold        float64          `json:"threshold" yaml:"threshold"`
}

// PostprocessingType defines post-processing steps applied to search results. Currently only have no prostprocessing setup.
type PostprocessingType string

const (
	PostprocessingNone PostprocessingType = "none" // no extra postprocessing steps
)

// SpannerSearchConfig holds the full configuration for Spanner search operations.
type SpannerSearchConfig struct {
	SearchAlgorithm    SearchMethod         `json:"search_algorithm" yaml:"search_algorithm"`
	VectorSearchConfig VectorSearchConfig   `json:"vector_search_config" yaml:"vector_search_config"`
	Postprocessing     []PostprocessingType `json:"postprocessing" yaml:"postprocessing"` // list of postprocessing steps to apply to search results
}

// GetSpannerSearchConfigPath returns the absolute path to the YAML configuration file for a given environment.
func GetSpannerSearchConfigPath(env string) string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	dir := filepath.Dir(filename)
	return filepath.Join(dir, "spanner_config", env+".yaml")
}

// ReadSpannerSearchConfig reads YAML into the SpannerSearchConfig from the path string.
func ReadSpannerSearchConfig(path string) (*SpannerSearchConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	var cfg SpannerSearchConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}
	return &cfg, nil
}
