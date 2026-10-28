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
	"encoding/json"
	"testing"

	"github.com/datacommonsorg/mixer/internal/agent"
	"github.com/google/go-cmp/cmp"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestV2ResolveEntities(t *testing.T) {
	tests := []struct {
		name        string
		input       V2ResolveEntitiesInput
		searchScope string
		wantReq     *agent.V2ResolveEntitiesRequest
	}{
		{
			name: "maps entities and hints and uses the server search scope as target",
			input: V2ResolveEntitiesInput{Entities: []V2EntityInput{
				{Name: "California", ChildEntityTypeHints: []string{"county"}},
				{Name: "St. Mary's Food Bank", EntityTypeHints: []string{"food bank"}},
			}},
			searchScope: "base_only",
			wantReq: &agent.V2ResolveEntitiesRequest{
				Entities: []*agent.V2EntityQuery{
					{Name: "California", ChildEntityTypeHints: []string{"county"}},
					{Name: "St. Mary's Food Bank", EntityTypeHints: []string{"food bank"}},
				},
				Target: "base_only",
			},
		},
		{
			name:    "empty input is passed through for agent validation",
			input:   V2ResolveEntitiesInput{},
			wantReq: &agent.V2ResolveEntitiesRequest{Entities: []*agent.V2EntityQuery{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			if _, err := NewMcpTools(backend, tc.searchScope).V2ResolveEntities(context.Background(), tc.input); err != nil {
				t.Fatalf("V2ResolveEntities() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantReq, backend.lastV2ResolveReq); diff != "" {
				t.Errorf("agent request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestV2ResolveEntitiesInputSchema(t *testing.T) {
	tool := mcp.NewTool("resolve_entities", withToolInputSchema[V2ResolveEntitiesInput]())
	var got map[string]any
	if err := json.Unmarshal(tool.RawInputSchema, &got); err != nil {
		t.Fatalf("failed to decode input schema: %v", err)
	}
	entities := got["properties"].(map[string]any)["entities"].(map[string]any)
	items := entities["items"].(map[string]any)
	want := map[string]any{
		"entitiesType": "array",
		"itemsType":    "object",
		"nameType":     "string",
		"hintsType":    "array",
		"required":     []any{"name"},
	}
	itemProps := items["properties"].(map[string]any)
	for field, prop := range map[string]any{
		"entities":                entities,
		"name":                    itemProps["name"],
		"entity_type_hints":       itemProps["entity_type_hints"],
		"child_entity_type_hints": itemProps["child_entity_type_hints"],
	} {
		if desc, _ := prop.(map[string]any)["description"].(string); desc == "" {
			t.Errorf("resolve_entities input schema field %q has no description", field)
		}
	}
	gotSummary := map[string]any{
		"entitiesType": entities["type"],
		"itemsType":    items["type"],
		"nameType":     itemProps["name"].(map[string]any)["type"],
		"hintsType":    itemProps["entity_type_hints"].(map[string]any)["type"],
		"required":     items["required"],
	}
	if diff := cmp.Diff(want, gotSummary); diff != "" {
		t.Errorf("resolve_entities input schema diff (-want +got):\n%s", diff)
	}
}

func TestV2SearchIndicators(t *testing.T) {
	tests := []struct {
		name        string
		input       V2SearchIndicatorsInput
		searchScope string
		wantReq     *agent.V2SearchIndicatorsRequest
	}{
		{
			name:        "maps search fields and uses the server search scope as target",
			input:       V2SearchIndicatorsInput{Query: "unemployment rate", EntityDcids: []string{"country/KEN"}},
			searchScope: "base_only",
			wantReq:     &agent.V2SearchIndicatorsRequest{Query: "unemployment rate", EntityDcids: []string{"country/KEN"}, Target: "base_only"},
		},
		{
			name:    "maps child mode fields",
			input:   V2SearchIndicatorsInput{Query: "median income", EntityDcids: []string{"geoId/06"}, ChildEntityTypeDcid: "County"},
			wantReq: &agent.V2SearchIndicatorsRequest{Query: "median income", EntityDcids: []string{"geoId/06"}, ChildEntityTypeDcid: "County"},
		},
		{
			name:    "maps browse fields",
			input:   V2SearchIndicatorsInput{TopicDcids: []string{"dc/topic/Health"}},
			wantReq: &agent.V2SearchIndicatorsRequest{TopicDcids: []string{"dc/topic/Health"}},
		},
		{
			name:    "empty input is passed through to browse the top-level topics",
			wantReq: &agent.V2SearchIndicatorsRequest{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			if _, err := NewMcpTools(backend, tc.searchScope).V2SearchIndicators(context.Background(), tc.input); err != nil {
				t.Fatalf("V2SearchIndicators() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantReq, backend.lastV2SearchReq); diff != "" {
				t.Errorf("agent request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestV2SearchIndicatorsInputSchema(t *testing.T) {
	tool := mcp.NewTool("search_indicators", withToolInputSchema[V2SearchIndicatorsInput]())
	var got map[string]any
	if err := json.Unmarshal(tool.RawInputSchema, &got); err != nil {
		t.Fatalf("failed to decode input schema: %v", err)
	}
	props := got["properties"].(map[string]any)
	gotTypes := map[string]any{}
	for _, field := range []string{"query", "topic_dcids", "entity_dcids", "child_entity_type_dcid"} {
		prop := props[field].(map[string]any)
		if desc, _ := prop["description"].(string); desc == "" {
			t.Errorf("search_indicators input schema field %q has no description", field)
		}
		gotTypes[field] = prop["type"]
	}
	wantTypes := map[string]any{"query": "string", "topic_dcids": "array", "entity_dcids": "array", "child_entity_type_dcid": "string"}
	if diff := cmp.Diff(wantTypes, gotTypes); diff != "" {
		t.Errorf("search_indicators input schema types diff (-want +got):\n%s", diff)
	}
	if required, ok := got["required"]; ok && len(required.([]any)) > 0 {
		t.Errorf("search_indicators input schema required = %v, want no required fields", required)
	}
}
