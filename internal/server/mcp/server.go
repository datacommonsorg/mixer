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

// Package mcp implements the Data Commons Model Context Protocol (MCP) server inside Mixer.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"gopkg.in/yaml.v3"
)

const (
	serverName                = "Data Commons MCP Server"
	headerEnableDocumentation = "X-DC-Enable-Documentation"
	defaultHeartbeatInterval  = 30 * time.Second
)

// docHeaderContextKey is the context key used to propagate the X-DC-Enable-Documentation
// HTTP request header into MCP server lifecycle hooks.
type docHeaderContextKey struct{}

// DcMcpServer wraps the stateless Streamable HTTP MCP server.
type DcMcpServer struct {
	httpServer *server.StreamableHTTPServer
}

// NewDcMcpServer initializes the Data Commons MCP server with all tools, instructions, and skill resources.
func NewDcMcpServer(
	ctx context.Context,
	agentService AgentBackend,
	instructionsDir string,
	searchScope string,
) *DcMcpServer {
	loader := NewInstructionLoader(ctx, instructionsDir)
	defer func() {
		if err := loader.Close(); err != nil {
			slog.Warn("Failed to close MCP instruction loader", "error", err)
		}
	}()

	tools := NewMcpTools(agentService, searchScope)
	sdkServer := newConfiguredMCPServer(ctx, loader, tools)

	httpServer := server.NewStreamableHTTPServer(
		sdkServer,
		server.WithStateLess(true),
		server.WithHeartbeatInterval(defaultHeartbeatInterval),
		server.WithHTTPContextFunc(func(reqCtx context.Context, r *http.Request) context.Context {
			return context.WithValue(reqCtx, docHeaderContextKey{}, r.Header.Get(headerEnableDocumentation))
		}),
	)

	return &DcMcpServer{
		httpServer: httpServer,
	}
}

// ServeHTTP dispatches non-SSE GET probes to the health handler and all MCP traffic to the Streamable HTTP server.
func (s *DcMcpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Per the MCP Streamable HTTP spec, GET requests with "Accept: text/event-stream" open
	// a Server-Sent Events (SSE) stream handled by s.httpServer; plain GET requests return health status.
	if r.Method == http.MethodGet && !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		writeHealthResponse(w)
		return
	}
	s.httpServer.ServeHTTP(w, r)
}

// newConfiguredMCPServer builds an MCPServer instance configured with instructions, hooks, tools, and skills.
func newConfiguredMCPServer(
	ctx context.Context,
	loader *InstructionLoader,
	tools *McpTools,
) *server.MCPServer {
	baseInstructions := strings.TrimSpace(loader.Load(ctx, serverInstructionsFile))
	docInstructions := baseInstructions
	if ext := strings.TrimSpace(loader.Load(ctx, docInstructionsExtensionFile)); ext != "" {
		docInstructions = baseInstructions + "\n\n" + ext
	}

	srv := server.NewMCPServer(
		serverName,
		resolveServerVersion(),
		server.WithInstructions(baseInstructions),
		server.WithHooks(newDocumentationHooks(docInstructions)),
		server.WithToolCapabilities(false),
		server.WithResourceCapabilities(false, false),
	)

	registerTool(ctx, srv, loader, "search_indicators", searchIndicatorsInstructionFile, tools.SearchIndicators)
	registerTool(ctx, srv, loader, "search_child_indicators", searchChildIndicatorsInstructionFile, tools.SearchChildIndicators)
	registerTool(ctx, srv, loader, "get_variable_metadata", getVariableMetadataInstructionFile, tools.GetVariableMetadata)
	registerTool(ctx, srv, loader, "get_observations", getObservationsInstructionFile, tools.GetObservations)
	registerTool(ctx, srv, loader, "get_child_observations", getChildObservationsInstructionFile, tools.GetChildObservations)
	registerTool(ctx, srv, loader, "get_multi_entity_observations", getMultiEntityObservationsInstructionFile, tools.GetMultiEntityObservations)

	registerSkills(ctx, srv, loader)
	return srv
}

// newDocumentationHooks creates MCP server hooks that validate the X-DC-Enable-Documentation
// header on every request and append documentation instructions during initialize when enabled.
func newDocumentationHooks(docInstructions string) *server.Hooks {
	hooks := &server.Hooks{}
	hooks.AddOnRequestInitialization(func(reqCtx context.Context, _ any, _ any) error {
		rawHeader, _ := reqCtx.Value(docHeaderContextKey{}).(string)
		_, err := parseEnableDocumentationHeader(rawHeader)
		return err
	})
	hooks.AddAfterInitialize(func(reqCtx context.Context, _ any, _ *mcp.InitializeRequest, result *mcp.InitializeResult) {
		rawHeader, _ := reqCtx.Value(docHeaderContextKey{}).(string)
		if docEnabled, err := parseEnableDocumentationHeader(rawHeader); err == nil && docEnabled {
			result.Instructions = docInstructions
		}
	})
	return hooks
}

// registerTool registers a typed tool with its markdown description and generated JSON schema.
func registerTool[T_IN any, T_OUT any](
	ctx context.Context,
	srv *server.MCPServer,
	loader *InstructionLoader,
	name string,
	instructionFile string,
	bizLogic func(context.Context, T_IN) (T_OUT, error),
) {
	tool := mcp.NewTool(
		name,
		mcp.WithDescription(loader.Load(ctx, instructionFile)),
		withToolInputSchema[T_IN](),
	)
	srv.AddTool(tool, mcp.NewStructuredToolHandler(func(tCtx context.Context, _ mcp.CallToolRequest, args T_IN) (T_OUT, error) {
		slog.Debug("Invoking MCP tool", "name", name)
		res, err := bizLogic(tCtx, args)
		if err != nil {
			slog.Error("MCP tool execution failed", "name", name, "error", err)
		}
		return res, err
	}))
}

// withToolInputSchema generates a JSON Schema from T, simplifies nullable type lists
// to single-string types, and populates "default" values from struct tags.
func withToolInputSchema[T any]() mcp.ToolOption {
	schema, err := jsonschema.For[T](&jsonschema.ForOptions{IgnoreInvalidTypes: true})
	if err != nil {
		return mcp.WithInputSchema[T]()
	}
	normalizeSchema(schema)
	applyStructDefaults[T](schema)

	rawBytes, err := json.Marshal(schema)
	if err != nil {
		return mcp.WithInputSchema[T]()
	}
	return func(t *mcp.Tool) {
		t.InputSchema.Type = ""
		t.RawInputSchema = json.RawMessage(rawBytes)
	}
}

// normalizeSchema simplifies nullable type lists like `"type": ["null", "array"]`
// (generated by jsonschema.For for Go slices, maps, and pointers) to `"type": "array"`
// so LLM clients that require `"type"` to be a single string can validate the schema.
func normalizeSchema(s *jsonschema.Schema) {
	if s == nil {
		return
	}
	if len(s.Types) == 2 {
		if s.Types[0] == "null" {
			s.Type = s.Types[1]
			s.Types = nil
		} else if s.Types[1] == "null" {
			s.Type = s.Types[0]
			s.Types = nil
		}
	}
	for _, prop := range s.Properties {
		normalizeSchema(prop)
	}
	normalizeSchema(s.Items)
	normalizeSchema(s.AdditionalProperties)
}

// applyStructDefaults inspects struct field tags for `default:"..."` and populates the
// corresponding JSON Schema property's "default" annotation.
func applyStructDefaults[T any](s *jsonschema.Schema) {
	if s == nil || len(s.Properties) == 0 {
		return
	}
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		defaultVal, hasDefault := field.Tag.Lookup("default")
		if !hasDefault {
			continue
		}
		jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
		prop, ok := s.Properties[jsonName]
		if !ok || prop == nil {
			continue
		}
		if prop.Type == "string" {
			if encoded, err := json.Marshal(defaultVal); err == nil {
				prop.Default = encoded
			}
		} else {
			prop.Default = json.RawMessage(defaultVal)
		}
	}
}

// registerSkills registers each skill://<name>/SKILL.md resource on the MCPServer.
func registerSkills(ctx context.Context, srv *server.MCPServer, loader *InstructionLoader) {
	for _, skillName := range defaultSkillNames {
		skillText := loader.Load(ctx, path.Join("skills", skillName, "SKILL.md"))
		if skillText == "" {
			continue
		}
		uri := fmt.Sprintf("skill://%s/SKILL.md", skillName)
		res := mcp.NewResource(
			uri,
			fmt.Sprintf("%s/SKILL.md", skillName),
			mcp.WithMIMEType("text/markdown"),
			mcp.WithResourceDescription(extractSkillDescription(skillText, skillName)),
		)
		srv.AddResource(res, func(_ context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
			return []mcp.ResourceContents{
				mcp.TextResourceContents{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     skillText,
				},
			}, nil
		})
	}
}

// extractSkillDescription parses the YAML frontmatter of a SKILL.md document to extract its description.
func extractSkillDescription(markdown, fallbackName string) string {
	trimmed := strings.TrimSpace(markdown)
	if !strings.HasPrefix(trimmed, "---") {
		return fallbackName
	}
	rest := strings.TrimPrefix(trimmed, "---")
	endIdx := strings.Index(rest, "\n---")
	if endIdx == -1 {
		return fallbackName
	}
	var fm struct {
		Description string `yaml:"description"`
	}
	if err := yaml.Unmarshal([]byte(rest[:endIdx]), &fm); err != nil || strings.TrimSpace(fm.Description) == "" {
		return fallbackName
	}
	return strings.TrimSpace(fm.Description)
}

// resolveServerVersion returns MIXER_HASH (matching Mixer's /version endpoint).
func resolveServerVersion() string {
	return strings.TrimSpace(os.Getenv("MIXER_HASH"))
}

// writeHealthResponse writes the JSON health status including the server name and git hash.
func writeHealthResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": serverName,
		"gitHash": resolveServerVersion(),
	})
}

// parseEnableDocumentationHeader validates the X-DC-Enable-Documentation header value.
func parseEnableDocumentationHeader(rawValue string) (docEnabled bool, err error) {
	trimmed := strings.TrimSpace(rawValue)
	if trimmed == "" || strings.EqualFold(trimmed, "false") {
		return false, nil
	}
	if strings.EqualFold(trimmed, "true") {
		return true, nil
	}
	return false, fmt.Errorf("%s must be true or false", headerEnableDocumentation)
}
