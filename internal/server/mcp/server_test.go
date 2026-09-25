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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDocumentationHeaderHandling(t *testing.T) {
	t.Setenv("MIXER_HASH", "dev-test-hash")
	srv := NewDcMcpServer(context.Background(), &fakeAgentBackend{}, "", "")
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Verify GET health check returns server name and gitHash.
	healthResp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("GET health request failed: %v", err)
	}
	defer func() { _ = healthResp.Body.Close() }()
	var healthBody map[string]string
	if err := json.NewDecoder(healthResp.Body).Decode(&healthBody); err != nil {
		t.Fatalf("failed to decode health response: %v", err)
	}
	wantHealth := map[string]string{
		"status":  "ok",
		"service": "Data Commons MCP Server",
		"gitHash": "dev-test-hash",
	}
	if diff := cmp.Diff(wantHealth, healthBody); diff != "" {
		t.Errorf("health response diff (-want +got):\n%s", diff)
	}

	tests := []struct {
		name           string
		headerVal      string
		wantErrMsg     string
		wantInstrMatch string
		wantInstrOmit  string
	}{
		{
			name:           "no header returns base instructions only",
			headerVal:      "",
			wantInstrMatch: "Data Commons",
			wantInstrOmit:  "https://docs.datacommons.org/llms.txt",
		},
		{
			name:           "false header returns base instructions only",
			headerVal:      "false",
			wantInstrMatch: "Data Commons",
			wantInstrOmit:  "https://docs.datacommons.org/llms.txt",
		},
		{
			name:           "true header appends documentation extension",
			headerVal:      "true",
			wantInstrMatch: "https://docs.datacommons.org/llms.txt",
		},
		{
			name:       "invalid header value returns JSON-RPC error",
			headerVal:  "maybe",
			wantErrMsg: "X-DC-Enable-Documentation must be true or false",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			respMap := sendJSONRPC(t, ts.URL, tc.headerVal, map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"method":  "initialize",
				"params": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{},
					"clientInfo":      map[string]any{"name": "test-client", "version": "1.0.0"},
				},
			})

			if tc.wantErrMsg != "" {
				errObj, ok := respMap["error"].(map[string]any)
				if !ok {
					t.Fatalf("expected JSON-RPC error object, got: %+v", respMap)
				}
				if msg, _ := errObj["message"].(string); !strings.Contains(msg, tc.wantErrMsg) {
					t.Errorf("error message = %q, want substring %q", msg, tc.wantErrMsg)
				}
				return
			}

			resObj, ok := respMap["result"].(map[string]any)
			if !ok {
				t.Fatalf("expected JSON-RPC result object, got: %+v", respMap)
			}
			serverInfo, _ := resObj["serverInfo"].(map[string]any)
			if gotName, _ := serverInfo["name"].(string); gotName != "Data Commons MCP Server" {
				t.Errorf("serverInfo.name = %q, want %q", gotName, "Data Commons MCP Server")
			}
			if gotVersion, _ := serverInfo["version"].(string); gotVersion != "dev-test-hash" {
				t.Errorf("serverInfo.version = %q, want %q", gotVersion, "dev-test-hash")
			}
			instructions, _ := resObj["instructions"].(string)
			if tc.wantInstrMatch != "" && !strings.Contains(instructions, tc.wantInstrMatch) {
				t.Errorf("instructions missing %q; got: %q", tc.wantInstrMatch, instructions)
			}
			if tc.wantInstrOmit != "" && strings.Contains(instructions, tc.wantInstrOmit) {
				t.Errorf("instructions unexpectedly contained %q; got: %q", tc.wantInstrOmit, instructions)
			}
		})
	}
}

func TestToolsAndResourcesProtocol(t *testing.T) {
	srv := NewDcMcpServer(context.Background(), &fakeAgentBackend{}, "", "base_and_custom")
	ts := httptest.NewServer(srv)
	defer ts.Close()

	tests := []struct {
		name   string
		method string
		params map[string]any
		verify func(t *testing.T, respMap map[string]any)
	}{
		{
			name:   "tools/list returns all 6 expected tools with normalized schemas and defaults",
			method: "tools/list",
			params: map[string]any{},
			verify: func(t *testing.T, respMap map[string]any) {
				resultObj := respMap["result"].(map[string]any)
				rawTools := resultObj["tools"].([]any)
				var gotToolNames []string
				toolByName := make(map[string]map[string]any)
				for _, item := range rawTools {
					toolMap := item.(map[string]any)
					name := toolMap["name"].(string)
					gotToolNames = append(gotToolNames, name)
					toolByName[name] = toolMap

					// Ensure every property's "type" is a single string (e.g. "array") rather than a list (e.g. ["null", "array"]).
					schema := toolMap["inputSchema"].(map[string]any)
					props := schema["properties"].(map[string]any)
					for propName, rawProp := range props {
						propMap := rawProp.(map[string]any)
						if _, isString := propMap["type"].(string); !isString {
							t.Errorf("tool %s property %s has non-string type: %#v", name, propName, propMap["type"])
						}
					}
				}
				slices.Sort(gotToolNames)
				wantToolNames := []string{
					"get_child_observations",
					"get_multi_entity_observations",
					"get_observations",
					"get_variable_metadata",
					"search_child_indicators",
					"search_indicators",
				}
				if diff := cmp.Diff(wantToolNames, gotToolNames); diff != "" {
					t.Errorf("tools/list names diff (-want +got):\n%s", diff)
				}

				searchProps := toolByName["search_indicators"]["inputSchema"].(map[string]any)["properties"].(map[string]any)
				if gotLimitDefault := searchProps["per_search_limit"].(map[string]any)["default"]; gotLimitDefault != float64(10) {
					t.Errorf("search_indicators per_search_limit default = %v, want 10", gotLimitDefault)
				}
				if gotTopicsDefault := searchProps["include_topics"].(map[string]any)["default"]; gotTopicsDefault != true {
					t.Errorf("search_indicators include_topics default = %v, want true", gotTopicsDefault)
				}

				obsProps := toolByName["get_observations"]["inputSchema"].(map[string]any)["properties"].(map[string]any)
				if gotDateDefault := obsProps["date"].(map[string]any)["default"]; gotDateDefault != "latest" {
					t.Errorf("get_observations date default = %v, want \"latest\"", gotDateDefault)
				}
			},
		},
		{
			name:   "resources/list returns SKILL.md for all 3 skills",
			method: "resources/list",
			params: map[string]any{},
			verify: func(t *testing.T, respMap map[string]any) {
				resResult := respMap["result"].(map[string]any)
				rawResources := resResult["resources"].([]any)
				var gotResourceURIs []string
				for _, item := range rawResources {
					resMap := item.(map[string]any)
					gotResourceURIs = append(gotResourceURIs, resMap["uri"].(string))
				}
				slices.Sort(gotResourceURIs)
				wantResourceURIs := []string{
					"skill://data-commons-child-places-researcher/SKILL.md",
					"skill://data-commons-multi-entity-researcher/SKILL.md",
					"skill://data-commons-researcher/SKILL.md",
				}
				if diff := cmp.Diff(wantResourceURIs, gotResourceURIs); diff != "" {
					t.Errorf("resources/list URIs diff (-want +got):\n%s", diff)
				}
			},
		},
		{
			name:   "resources/read returns SKILL.md content",
			method: "resources/read",
			params: map[string]any{"uri": "skill://data-commons-researcher/SKILL.md"},
			verify: func(t *testing.T, respMap map[string]any) {
				readResult := respMap["result"].(map[string]any)
				contents := readResult["contents"].([]any)
				if len(contents) != 1 {
					t.Fatalf("resources/read contents len = %d, want 1", len(contents))
				}
				contentItem := contents[0].(map[string]any)
				if !strings.Contains(contentItem["text"].(string), "data-commons-researcher") {
					t.Errorf("unexpected SKILL.md content: %v", contentItem["text"])
				}
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			respMap := sendJSONRPC(t, ts.URL, "", map[string]any{
				"jsonrpc": "2.0",
				"id":      i + 10,
				"method":  tc.method,
				"params":  tc.params,
			})
			tc.verify(t, respMap)
		})
	}
}

// sendJSONRPC sends a JSON-RPC POST request to the test server and decodes the JSON response map.
func sendJSONRPC(t *testing.T, url, docHeader string, payload map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal JSON-RPC payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if docHeader != "" {
		req.Header.Set(headerEnableDocumentation, docHeader)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode JSON-RPC response: %v", err)
	}
	return out
}
