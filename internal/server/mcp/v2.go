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

const (
	v2ResolveEntitiesInstructionFile = "tools/resolve_entities.md"
)

// V2Spec returns the VersionSpec for MCP v2, the entity-first tool surface under development.
func V2Spec() VersionSpec {
	return VersionSpec{
		ID:    "v2",
		Stage: StagePreview,
		// Tools are added incrementally while v2 is in preview.
		Tools: []ToolSpec{
			newToolSpec("resolve_entities", v2ResolveEntitiesInstructionFile, (*McpTools).V2ResolveEntities),
		},
	}
}
