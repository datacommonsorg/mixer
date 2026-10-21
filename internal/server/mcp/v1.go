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

// V1Spec returns the VersionSpec for MCP v1.
func V1Spec() VersionSpec {
	return VersionSpec{
		ID:    "v1",
		Stage: StageStable,
		Tools: []ToolSpec{
			newToolSpec("search_indicators", searchIndicatorsInstructionFile, (*McpTools).SearchIndicators),
			newToolSpec("search_child_indicators", searchChildIndicatorsInstructionFile, (*McpTools).SearchChildIndicators),
			newToolSpec("get_variable_metadata", getVariableMetadataInstructionFile, (*McpTools).GetVariableMetadata),
			newToolSpec("get_observations", getObservationsInstructionFile, (*McpTools).GetObservations),
			newToolSpec("get_child_observations", getChildObservationsInstructionFile, (*McpTools).GetChildObservations),
			newToolSpec("get_multi_entity_observations", getMultiEntityObservationsInstructionFile, (*McpTools).GetMultiEntityObservations),
		},
		SkillNames: []string{
			"data-commons-child-places-researcher",
			"data-commons-multi-entity-researcher",
			"data-commons-researcher",
		},
	}
}
