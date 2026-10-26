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

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v3"
)

const (
	serverName                = "Data Commons MCP Server"
	headerEnableDocumentation = "X-DC-Enable-Documentation"
	surfaceHeaderKey          = "x-surface"
	defaultRouteSurface       = "mcp-default"
	versionSurfacePrefix      = "mcp-"
)

// docHeaderContextKey is the context key used to propagate the X-DC-Enable-Documentation
// HTTP request header into MCP server lifecycle hooks.
type docHeaderContextKey struct{}

// versionInstance holds the initialized Streamable HTTP server for a single MCP version.
type versionInstance struct {
	versionID  string
	httpServer *server.StreamableHTTPServer
}

// ServerOptions holds configuration options for DcMcpServer.
type ServerOptions struct {
	InstructionsDir string
	SearchScope     string
	DefaultVersion  string
	EnablePreview   bool
	Versions        []VersionSpec
}

// DcMcpServer routes requests across the unversioned /mcp alias and versioned /mcp/<id> endpoints.
type DcMcpServer struct {
	defaultVersion string
	routes         map[string]*versionInstance
}

// NewDcMcpServer initializes the Data Commons MCP server across all active versions.
func NewDcMcpServer(
	ctx context.Context,
	agentService AgentBackend,
	opts *ServerOptions,
) (*DcMcpServer, error) {
	cfg := ServerOptions{
		DefaultVersion: DefaultVersionID,
		Versions:       AllVersions(),
	}
	if opts != nil {
		cfg = *opts
		if cfg.Versions == nil {
			cfg.Versions = AllVersions()
		}
	}

	mounted, err := resolveMountedVersions(cfg.Versions, cfg.DefaultVersion, cfg.EnablePreview)
	if err != nil {
		return nil, err
	}

	tools := NewMcpTools(agentService, cfg.SearchScope)
	instancesByID := make(map[string]*versionInstance, len(mounted))
	routes := make(map[string]*versionInstance, len(mounted))

	for routeKey, spec := range mounted {
		inst, exists := instancesByID[spec.ID]
		if !exists {
			inst = newVersionInstance(ctx, cfg.InstructionsDir, tools, spec)
			instancesByID[spec.ID] = inst
		}
		routes[routeKey] = inst
	}

	return &DcMcpServer{
		defaultVersion: strings.TrimSpace(cfg.DefaultVersion),
		routes:         routes,
	}, nil
}

// ServeHTTP dispatches requests to the matching MCP version instance, injecting the
// route-specific x-surface metadata and handling plain GET health checks.
func (s *DcMcpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	inst, surface, ok := s.resolveRoute(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	// Per the MCP Streamable HTTP spec, GET requests with "Accept: text/event-stream" are
	// routed to inst.httpServer (which rejects SSE streaming with 405 Method Not Allowed in stateless mode);
	// plain GET requests return health status.
	if r.Method == http.MethodGet && !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		writeHealthResponse(w, inst.versionID)
		return
	}

	ctx := withSurfaceMetadata(r.Context(), surface)
	inst.httpServer.ServeHTTP(w, r.WithContext(ctx))
}

// newVersionInstance constructs an isolated MCPServer and StreamableHTTPServer for a single VersionSpec.
func newVersionInstance(
	ctx context.Context,
	instructionsDir string,
	tools *McpTools,
	spec VersionSpec,
) *versionInstance {
	loader := NewInstructionLoader(ctx, instructionsDir, spec.ID)
	defer func() {
		if err := loader.Close(); err != nil {
			slog.Warn("Failed to close MCP instruction loader", "version", spec.ID, "error", err)
		}
	}()

	sdkServer := newConfiguredMCPServer(ctx, loader, tools, spec)
	httpServer := server.NewStreamableHTTPServer(
		sdkServer,
		server.WithStateLess(true),
		server.WithDisableStreaming(true),
		server.WithHTTPContextFunc(func(reqCtx context.Context, r *http.Request) context.Context {
			return context.WithValue(reqCtx, docHeaderContextKey{}, r.Header.Get(headerEnableDocumentation))
		}),
	)

	return &versionInstance{
		versionID:  spec.ID,
		httpServer: httpServer,
	}
}

// newConfiguredMCPServer builds an MCPServer instance configured with instructions, hooks, tools, and skills for spec.
func newConfiguredMCPServer(
	ctx context.Context,
	loader *InstructionLoader,
	tools *McpTools,
	spec VersionSpec,
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

	for _, toolSpec := range spec.Tools {
		tool := mcp.NewTool(
			toolSpec.Name,
			mcp.WithDescription(loader.Load(ctx, toolSpec.InstructionFile)),
			toolSpec.InputSchema,
			readOnlyToolAnnotations(),
		)
		srv.AddTool(tool, toolSpec.Handler(tools))
	}

	registerSkills(ctx, srv, loader, spec.SkillNames)
	return srv
}

// resolveRoute maps an incoming HTTP request path (after /mcp prefix stripping) to its
// backing versionInstance and usage-log surface string.
func (s *DcMcpServer) resolveRoute(urlPath string) (inst *versionInstance, surface string, ok bool) {
	trimmed := strings.Trim(urlPath, "/")
	if trimmed == "" {
		inst, ok = s.routes[s.defaultVersion]
		return inst, defaultRouteSurface, ok
	}
	if strings.Contains(trimmed, "/") {
		return nil, "", false
	}
	inst, ok = s.routes[trimmed]
	if !ok {
		return nil, "", false
	}
	return inst, versionSurfacePrefix + trimmed, true
}

// withSurfaceMetadata attaches the x-surface key to incoming gRPC metadata on ctx,
// preserving any existing incoming metadata keys.
func withSurfaceMetadata(ctx context.Context, surface string) context.Context {
	surfaceMD := metadata.Pairs(surfaceHeaderKey, surface)
	if existingMD, ok := metadata.FromIncomingContext(ctx); ok {
		copied := existingMD.Copy()
		copied.Set(surfaceHeaderKey, surface)
		return metadata.NewIncomingContext(ctx, copied)
	}
	return metadata.NewIncomingContext(ctx, surfaceMD)
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

// readOnlyToolAnnotations marks a tool as read-only, non-destructive and idempotent. All Data Commons
// tools only query data, and mcp-go's defaults (destructive, not read-only) can make clients ask users
// to confirm every call. OpenWorldHint keeps its default (true), since tools query an external
// knowledge graph and resolvers such as Google Maps.
func readOnlyToolAnnotations() mcp.ToolOption {
	return mcp.WithToolAnnotation(mcp.ToolAnnotation{
		ReadOnlyHint:    mcp.ToBoolPtr(true),
		DestructiveHint: mcp.ToBoolPtr(false),
		IdempotentHint:  mcp.ToBoolPtr(true),
		OpenWorldHint:   mcp.ToBoolPtr(true),
	})
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
func registerSkills(ctx context.Context, srv *server.MCPServer, loader *InstructionLoader, skillNames []string) {
	for _, skillName := range skillNames {
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

// writeHealthResponse writes the JSON health status including the server name, MCP version ID, and git hash.
func writeHealthResponse(w http.ResponseWriter, versionID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": serverName,
		"version": versionID,
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
