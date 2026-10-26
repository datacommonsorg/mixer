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

	"github.com/datacommonsorg/mixer/internal/agent"
)

// V2AgentBackend defines the agent.Service methods required by the v2 MCP tools.
type V2AgentBackend interface {
	V2ResolveEntities(ctx context.Context, req *agent.V2ResolveEntitiesRequest) (*agent.V2ResolveEntitiesResponse, error)
}

// V2EntityInput is a single entity to resolve in the resolve_entities MCP tool.
// jsonschema tags become per-field descriptions in the tool's input schema shown to the LLM.
type V2EntityInput struct {
	Name            string   `json:"name" jsonschema:"Fully qualified entity name including its type word and parent places, e.g. 'Santa Clara County, California, United States', 'Washington State, United States', 'Madrid, Spain', 'Lincoln Elementary School, Atlanta, Georgia'."`
	EntityTypeHints []string `json:"entity_type_hints,omitempty" jsonschema:"Optional entity types, most specific first, e.g. ['state'], ['county', 'administrative area 2'], ['food bank']."`
}

// V2ResolveEntitiesInput holds arguments for the v2 resolve_entities MCP tool.
type V2ResolveEntitiesInput struct {
	Entities []V2EntityInput `json:"entities" jsonschema:"Entities to resolve, up to 20 per call."`
}
