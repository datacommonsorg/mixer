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
	"sync"

	"github.com/datacommonsorg/mixer/internal/server/datasource"
)

// TopicNode represents a topic and its immediate children.
type TopicNode struct {
	Dcid              string   `json:"dcid"`
	Name              string   `json:"name"`
	RelevantVariables []string `json:"relevantVariables"` // Can be SVs or Sub-Topics
}

// TopicHierarchy represents the processed graph of topics.
type TopicHierarchy struct {
	Topics         map[string]*TopicNode `json:"topics"`
	RootTopicDcids []string              `json:"rootTopicDcids"`
}

// TopicVariableCache is a composite struct to allow synchronized invalidation
// of both Topics and SVs in the future.
type TopicVariableCache struct {
	TopicHierarchy *TopicHierarchy `json:"topicHierarchy"`
	// SVs map[string]*SVInfo `json:"svs,omitempty"` // Placeholder for follow-up task
}

// TopicCacheManager manages the loading, building, and caching of topics.
type TopicCacheManager struct {
	ds datasource.DataSource

	mu    sync.RWMutex
	cache *TopicVariableCache
}

// NewTopicCacheManager creates a new TopicCacheManager.
func NewTopicCacheManager(ds datasource.DataSource) *TopicCacheManager {
	return &TopicCacheManager{
		ds: ds,
	}
}
