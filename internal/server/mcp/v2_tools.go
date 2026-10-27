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

package mcp

import (
	"context"
	"fmt"

	"github.com/datacommonsorg/mixer/internal/agent"
)

// V2ResolveEntities resolves entity names (places and non-place entities) to DCIDs.
func (t *McpTools) V2ResolveEntities(ctx context.Context, input V2ResolveEntitiesInput) (*agent.V2ResolveEntitiesResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	queries := make([]*agent.V2EntityQuery, 0, len(input.Entities))
	for _, entity := range input.Entities {
		queries = append(queries, &agent.V2EntityQuery{
			Name:                 entity.Name,
			EntityTypeHints:      entity.EntityTypeHints,
			ChildEntityTypeHints: entity.ChildEntityTypeHints,
		})
	}
	return t.agentService.V2ResolveEntities(ctx, &agent.V2ResolveEntitiesRequest{
		Entities: queries,
		Target:   t.searchScope,
	})
}

// V2SearchIndicators searches for topics and variables, or browses topics, keeping only those with data for
// the given entities.
func (t *McpTools) V2SearchIndicators(ctx context.Context, input V2SearchIndicatorsInput) (*agent.V2SearchIndicatorsResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	return t.agentService.V2SearchIndicators(ctx, &agent.V2SearchIndicatorsRequest{
		Query:       input.Query,
		TopicDcids:  input.TopicDcids,
		EntityDcids: input.EntityDcids,
		Target:      t.searchScope,
	})
}
