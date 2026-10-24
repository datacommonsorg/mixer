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
	"reflect"
)

// McpTools holds dependencies for executing Data Commons MCP tools in-process.
type McpTools struct {
	agentService AgentBackend
	searchScope  string
}

// NewMcpTools creates a new McpTools instance backed by the provided AgentBackend.
func NewMcpTools(agentService AgentBackend, searchScope string) *McpTools {
	if agentService != nil {
		if v := reflect.ValueOf(agentService); v.Kind() == reflect.Ptr && v.IsNil() {
			agentService = nil
		}
	}
	return &McpTools{
		agentService: agentService,
		searchScope:  searchScope,
	}
}
