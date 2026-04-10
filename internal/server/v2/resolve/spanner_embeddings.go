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

const (
	VectorSearch   = "vector_search"
	FullTextSearch = "full_text_search"
	HybridSearch   = "hybrid_search"
)

const (
	GraphConfigDefault        = "default"
	GraphConfigReranking      = "reranking"
	GraphConfigGraphTraversal = "graph_traversal"
)

type EmbeddingConfig struct {
	EmbeddingModel     string `json:"embedding_model"`
	EmbeddingType      string `json:"embedding_type"`
	EmbeddingDimension int    `json:"embedding_dimension"`
}

type SpannerSearchConfig struct {
	SearchAlgorithm string `json:"search_algorithm"`
	EmbeddingConfig EmbeddingConfig `json:"embedding_config"`
	GraphMode       string `json:"graph_mode"`
}
