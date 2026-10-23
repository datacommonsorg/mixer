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

	"github.com/datacommonsorg/mixer/internal/agent"
	"github.com/google/go-cmp/cmp"
)

func TestDocumentationHeaderHandling(t *testing.T) {
	t.Setenv("MIXER_HASH", "dev-test-hash")
	srv, err := NewDcMcpServer(context.Background(), &fakeAgentBackend{}, nil)
	if err != nil {
		t.Fatalf("NewDcMcpServer failed: %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	getTests := []struct {
		name       string
		accept     string
		wantStatus int
		wantHealth map[string]string
	}{
		{
			name:       "plain GET returns 200 health JSON with version",
			accept:     "",
			wantStatus: http.StatusOK,
			wantHealth: map[string]string{
				"status":  "ok",
				"service": "Data Commons MCP Server",
				"version": "v1",
				"gitHash": "dev-test-hash",
			},
		},
		{
			name:       "SSE GET with Accept text/event-stream returns 405 Method Not Allowed",
			accept:     "text/event-stream",
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tc := range getTests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
			if err != nil {
				t.Fatalf("failed to create GET request: %v", err)
			}
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("GET request failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("GET status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if tc.wantHealth != nil {
				var healthBody map[string]string
				if err := json.NewDecoder(resp.Body).Decode(&healthBody); err != nil {
					t.Fatalf("failed to decode health response: %v", err)
				}
				if diff := cmp.Diff(tc.wantHealth, healthBody); diff != "" {
					t.Errorf("health response diff (-want +got):\n%s", diff)
				}
			}
		})
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
	srv, err := NewDcMcpServer(context.Background(), &fakeAgentBackend{}, &ServerOptions{
		SearchScope:    "base_and_custom",
		DefaultVersion: DefaultVersionID,
		EnablePreview:  true,
	})
	if err != nil {
		t.Fatalf("NewDcMcpServer failed: %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	for _, routePath := range []string{"", "/v1"} {
		t.Run("route_"+routePath, func(t *testing.T) {
			endpoint := ts.URL + routePath
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

							if desc, _ := toolMap["description"].(string); strings.TrimSpace(desc) == "" {
								t.Errorf("tool %s has empty description", name)
							}
							wantAnnotations := map[string]any{"readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": true}
							if diff := cmp.Diff(wantAnnotations, toolMap["annotations"]); diff != "" {
								t.Errorf("tool %s annotations diff (-want +got):\n%s", name, diff)
							}

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
					respMap := sendJSONRPC(t, endpoint, "", map[string]any{
						"jsonrpc": "2.0",
						"id":      i + 10,
						"method":  tc.method,
						"params":  tc.params,
					})
					tc.verify(t, respMap)
				})
			}
		})
	}
}

func TestVersionRegistryValidation(t *testing.T) {
	tests := []struct {
		name           string
		versions       []VersionSpec
		defaultVersion string
		enablePreview  bool
		wantErrSubstr  string
	}{
		{
			name:           "valid AllVersions registry with preview disabled",
			versions:       AllVersions(),
			defaultVersion: "v1",
			enablePreview:  false,
		},
		{
			name:           "valid AllVersions registry with preview enabled",
			versions:       AllVersions(),
			defaultVersion: "v1",
			enablePreview:  true,
		},
		{
			name:           "empty versions slice fails",
			versions:       []VersionSpec{},
			defaultVersion: "v1",
			wantErrSubstr:  "at least one MCP version must be registered",
		},
		{
			name: "empty version ID fails",
			versions: []VersionSpec{
				{ID: "  ", Stage: StageStable},
			},
			defaultVersion: "v1",
			wantErrSubstr:  "MCP version ID cannot be empty",
		},
		{
			name: "reserved preview ID fails",
			versions: []VersionSpec{
				{ID: "preview", Stage: StageStable},
			},
			defaultVersion: "v1",
			wantErrSubstr:  `MCP version ID "preview" is reserved`,
		},
		{
			name: "version ID containing slash fails",
			versions: []VersionSpec{
				{ID: "v1/sub", Stage: StageStable},
			},
			defaultVersion: "v1/sub",
			wantErrSubstr:  "cannot contain slashes",
		},
		{
			name: "duplicate version ID fails",
			versions: []VersionSpec{
				{ID: "v1", Stage: StageStable},
				{ID: "v1", Stage: StagePreview},
			},
			defaultVersion: "v1",
			wantErrSubstr:  `duplicate MCP version ID "v1"`,
		},
		{
			name: "invalid version stage fails",
			versions: []VersionSpec{
				{ID: "v1", Stage: "beta"},
			},
			defaultVersion: "v1",
			wantErrSubstr:  `invalid stage "beta"`,
		},
		{
			name: "no stable versions fails",
			versions: []VersionSpec{
				{ID: "v2", Stage: StagePreview},
			},
			defaultVersion: "v2",
			enablePreview:  true,
			wantErrSubstr:  "at least one stable MCP version must be registered",
		},
		{
			name: "more than one StagePreview version fails",
			versions: []VersionSpec{
				{ID: "v1", Stage: StageStable},
				{ID: "v2", Stage: StagePreview},
				{ID: "v3", Stage: StagePreview},
			},
			defaultVersion: "v1",
			enablePreview:  true,
			wantErrSubstr:  `at most one MCP version may have stage "preview"`,
		},
		{
			name:           "empty default version fails",
			versions:       AllVersions(),
			defaultVersion: "",
			wantErrSubstr:  "default MCP version cannot be empty",
		},
		{
			name:           "default version set to preview alias fails",
			versions:       AllVersions(),
			defaultVersion: "preview",
			enablePreview:  true,
			wantErrSubstr:  `default MCP version cannot be "preview"`,
		},
		{
			name: "default version pointing to unmounted preview version fails when preview disabled",
			versions: []VersionSpec{
				{ID: "v1", Stage: StageStable},
				{ID: "v2", Stage: StagePreview},
			},
			defaultVersion: "v2",
			enablePreview:  false,
			wantErrSubstr:  `default MCP version "v2" is not an active mounted version`,
		},
		{
			name:           "default version pointing to preview version fails when preview enabled",
			versions:       AllVersions(),
			defaultVersion: "v2",
			enablePreview:  true,
			wantErrSubstr:  `default MCP version "v2" is not an active mounted version`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewDcMcpServer(
				context.Background(),
				&fakeAgentBackend{},
				&ServerOptions{
					DefaultVersion: tc.defaultVersion,
					EnablePreview:  tc.enablePreview,
					Versions:       tc.versions,
				},
			)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("NewDcMcpServer error = %v, want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewDcMcpServer unexpected error: %v", err)
			}
		})
	}
}

func TestVersionRoutingAndSurfacePropagation(t *testing.T) {
	v2PreviewSpec := VersionSpec{
		ID:    "v2",
		Stage: StagePreview,
		Tools: []ToolSpec{
			newToolSpec("search_indicators", searchIndicatorsInstructionFile, (*McpTools).SearchIndicators),
		},
	}
	versionsWithV2Preview := []VersionSpec{
		V1Spec(),
		v2PreviewSpec,
	}

	tests := []struct {
		name            string
		versions        []VersionSpec
		enablePreview   bool
		path            string
		wantGetStatus   int
		wantVersion     string
		wantToolCount   int
		wantCallSurface string
	}{
		{
			name:            "default /mcp alias routes to v1 with mcp-default surface",
			versions:        versionsWithV2Preview,
			enablePreview:   false,
			path:            "/",
			wantGetStatus:   http.StatusOK,
			wantVersion:     "v1",
			wantToolCount:   6,
			wantCallSurface: "mcp-default",
		},
		{
			name:            "explicit /mcp/v1 routes to v1 with mcp-v1 surface",
			versions:        versionsWithV2Preview,
			enablePreview:   false,
			path:            "/v1",
			wantGetStatus:   http.StatusOK,
			wantVersion:     "v1",
			wantToolCount:   6,
			wantCallSurface: "mcp-v1",
		},
		{
			name:          "/mcp/preview returns 404 when preview is disabled",
			versions:      versionsWithV2Preview,
			enablePreview: false,
			path:          "/preview",
			wantGetStatus: http.StatusNotFound,
		},
		{
			name:          "preview-stage /mcp/v2 returns 404 when preview is disabled",
			versions:      versionsWithV2Preview,
			enablePreview: false,
			path:          "/v2",
			wantGetStatus: http.StatusNotFound,
		},
		{
			name:            "/mcp/preview aliases StagePreview v2 when preview is enabled",
			versions:        versionsWithV2Preview,
			enablePreview:   true,
			path:            "/preview",
			wantGetStatus:   http.StatusOK,
			wantVersion:     "v2",
			wantToolCount:   1,
			wantCallSurface: "mcp-preview",
		},
		{
			name:            "/mcp/preview falls back to latest StageStable v1 when no StagePreview version exists",
			versions:        []VersionSpec{V1Spec()},
			enablePreview:   true,
			path:            "/preview",
			wantGetStatus:   http.StatusOK,
			wantVersion:     "v1",
			wantToolCount:   6,
			wantCallSurface: "mcp-preview",
		},
		{
			name:          "/mcp/preview serves registered V2Spec when preview is enabled",
			versions:      AllVersions(),
			enablePreview: true,
			path:          "/preview",
			wantGetStatus: http.StatusOK,
			wantVersion:   "v2",
			wantToolCount: len(V2Spec().Tools),
			// V2Spec has no search_indicators tool, so the tools/call probe errors before reaching the backend.
			wantCallSurface: "",
		},
		{
			name:          "registered V2Spec is not mounted at /mcp/v2 while in preview",
			versions:      AllVersions(),
			enablePreview: true,
			path:          "/v2",
			wantGetStatus: http.StatusNotFound,
		},
		{
			name:          "unknown version /mcp/v99 returns 404",
			versions:      versionsWithV2Preview,
			enablePreview: true,
			path:          "/v99",
			wantGetStatus: http.StatusNotFound,
		},
		{
			name:          "nested path /mcp/v1/extra returns 404",
			versions:      versionsWithV2Preview,
			enablePreview: true,
			path:          "/v1/extra",
			wantGetStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			srv, err := NewDcMcpServer(
				context.Background(),
				backend,
				&ServerOptions{
					DefaultVersion: "v1",
					EnablePreview:  tc.enablePreview,
					Versions:       tc.versions,
				},
			)
			if err != nil {
				t.Fatalf("NewDcMcpServer failed: %v", err)
			}
			ts := httptest.NewServer(srv)
			defer ts.Close()

			endpoint := ts.URL + tc.path

			resp, err := ts.Client().Get(endpoint)
			if err != nil {
				t.Fatalf("GET %s failed: %v", endpoint, err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tc.wantGetStatus {
				t.Fatalf("GET %s status = %d, want %d", tc.path, resp.StatusCode, tc.wantGetStatus)
			}
			if tc.wantGetStatus != http.StatusOK {
				return
			}

			var healthBody map[string]string
			if err := json.NewDecoder(resp.Body).Decode(&healthBody); err != nil {
				t.Fatalf("failed to decode health JSON: %v", err)
			}
			if gotVer := healthBody["version"]; gotVer != tc.wantVersion {
				t.Errorf("health version = %q, want %q", gotVer, tc.wantVersion)
			}

			listResp := sendJSONRPC(t, endpoint, "", map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"method":  "tools/list",
				"params":  map[string]any{},
			})
			rawTools := listResp["result"].(map[string]any)["tools"].([]any)
			if len(rawTools) != tc.wantToolCount {
				t.Errorf("tools/list count = %d, want %d", len(rawTools), tc.wantToolCount)
			}

			_ = sendJSONRPC(t, endpoint, "", map[string]any{
				"jsonrpc": "2.0",
				"id":      2,
				"method":  "tools/call",
				"params": map[string]any{
					"name": "search_indicators",
					"arguments": map[string]any{
						"query": "population",
					},
				},
			})
			if backend.lastSurface != tc.wantCallSurface {
				t.Errorf("backend surface = %q, want %q", backend.lastSurface, tc.wantCallSurface)
			}
		})
	}
}

func TestV2PreviewSurface(t *testing.T) {
	srv, err := NewDcMcpServer(context.Background(), &fakeAgentBackend{}, &ServerOptions{
		DefaultVersion: DefaultVersionID,
		EnablePreview:  true,
	})
	if err != nil {
		t.Fatalf("NewDcMcpServer failed: %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	endpoint := ts.URL + "/preview"

	v2ServerMD, err := instructionsFS.ReadFile("instructions/v2/server.md")
	if err != nil {
		t.Fatalf("failed to read embedded v2 server.md: %v", err)
	}
	v2DocExtension, err := instructionsFS.ReadFile("instructions/v2/doc_instructions_extension.md")
	if err != nil {
		t.Fatalf("failed to read embedded v2 doc_instructions_extension.md: %v", err)
	}
	initializeParams := map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test-client", "version": "1.0.0"},
	}

	tests := []struct {
		name      string
		method    string
		params    map[string]any
		docHeader string
		resultKey string
		transform func(any) any
		want      any
	}{
		{
			name:      "initialize returns v2 server instructions",
			method:    "initialize",
			params:    initializeParams,
			resultKey: "instructions",
			want:      strings.TrimSpace(string(v2ServerMD)),
		},
		{
			name:      "initialize with documentation header appends v2 documentation extension",
			method:    "initialize",
			params:    initializeParams,
			docHeader: "true",
			resultKey: "instructions",
			want:      strings.TrimSpace(string(v2ServerMD)) + "\n\n" + strings.TrimSpace(string(v2DocExtension)),
		},
		{
			name:      "tools/list returns V2Spec tools",
			method:    "tools/list",
			params:    map[string]any{},
			resultKey: "tools",
			transform: listedToolNames,
			want:      []string{"resolve_entities"},
		},
		{
			name:      "resources/list returns no skills",
			method:    "resources/list",
			params:    map[string]any{},
			resultKey: "resources",
			want:      []any{},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			respMap := sendJSONRPC(t, endpoint, tc.docHeader, map[string]any{
				"jsonrpc": "2.0",
				"id":      i + 1,
				"method":  tc.method,
				"params":  tc.params,
			})
			resObj, ok := respMap["result"].(map[string]any)
			if !ok {
				t.Fatalf("expected JSON-RPC result object, got: %+v", respMap)
			}
			got := resObj[tc.resultKey]
			if tc.transform != nil {
				got = tc.transform(got)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("%s result.%s diff (-want +got):\n%s", tc.method, tc.resultKey, diff)
			}
		})
	}
}

func TestV2PreviewResolveEntitiesCall(t *testing.T) {
	backend := &fakeAgentBackend{v2ResolveResp: &agent.V2ResolveEntitiesResponse{ResolvedEntities: &agent.Table{
		Columns: []string{"query", "dcid", "name", "types"},
		Rows:    [][]any{{"California", "geoId/06", "California", []string{"State"}}},
	}}}
	srv, err := NewDcMcpServer(context.Background(), backend, &ServerOptions{
		DefaultVersion: DefaultVersionID,
		EnablePreview:  true,
	})
	if err != nil {
		t.Fatalf("NewDcMcpServer failed: %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	respMap := sendJSONRPC(t, ts.URL+"/preview", "", map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "resolve_entities",
			"arguments": map[string]any{"entities": []any{map[string]any{"name": "California"}}},
		},
	})
	result, ok := respMap["result"].(map[string]any)
	if !ok || result["isError"] == true {
		t.Fatalf("expected successful tools/call result, got: %+v", respMap)
	}
	wantStructured := map[string]any{"resolved_entities": map[string]any{
		"columns": []any{"query", "dcid", "name", "types"},
		"rows":    []any{[]any{"California", "geoId/06", "California", []any{"State"}}},
	}}
	if diff := cmp.Diff(wantStructured, result["structuredContent"]); diff != "" {
		t.Errorf("structuredContent diff (-want +got):\n%s", diff)
	}
	text := result["content"].([]any)[0].(map[string]any)["text"].(string)
	var textJSON any
	if err := json.Unmarshal([]byte(text), &textJSON); err != nil {
		t.Fatalf("text content is not JSON: %v", err)
	}
	if diff := cmp.Diff(wantStructured, textJSON); diff != "" {
		t.Errorf("text content diff (-want +got):\n%s", diff)
	}
	if backend.lastSurface != "mcp-preview" {
		t.Errorf("backend surface = %q, want %q", backend.lastSurface, "mcp-preview")
	}
	wantReq := &agent.V2ResolveEntitiesRequest{Entities: []*agent.V2EntityQuery{{Name: "California"}}}
	if diff := cmp.Diff(wantReq, backend.lastV2ResolveReq); diff != "" {
		t.Errorf("agent request diff (-want +got):\n%s", diff)
	}
}

// listedToolNames extracts the sorted tool names from a decoded tools/list "tools" result.
func listedToolNames(rawTools any) any {
	names := []string{}
	for _, item := range rawTools.([]any) {
		names = append(names, item.(map[string]any)["name"].(string))
	}
	slices.Sort(names)
	return names
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
