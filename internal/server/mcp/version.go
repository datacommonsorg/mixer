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
	"log/slog"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// VersionStage represents the lifecycle stage of an MCP version.
type VersionStage string

const (
	// StageStable indicates a released MCP version mounted in all environments.
	StageStable VersionStage = "stable"
	// StagePreview indicates an in-development MCP version mounted only when --enable_mcp_preview is set.
	StagePreview VersionStage = "preview"

	// DefaultVersionID is the default MCP version backing the unversioned /mcp alias.
	DefaultVersionID = "v1"
	// previewRouteAlias is the URL route segment reserved for the preview alias (/mcp/preview).
	previewRouteAlias = "preview"
)

// ToolSpec holds the declarative metadata, input schema option, and bound handler factory for an MCP tool.
type ToolSpec struct {
	Name            string
	InstructionFile string
	InputSchema     mcp.ToolOption
	Handler         func(tools *McpTools) server.ToolHandlerFunc
}

// VersionSpec defines the complete surface area and lifecycle stage for a single MCP version.
type VersionSpec struct {
	ID         string
	Stage      VersionStage
	Tools      []ToolSpec
	SkillNames []string
}

// AllVersions returns the ordered registry of all defined MCP versions from oldest to newest.
func AllVersions() []VersionSpec {
	return []VersionSpec{
		V1Spec(),
	}
}

// newToolSpec constructs a pure-data ToolSpec from a typed McpTools method expression.
func newToolSpec[T_IN any, T_OUT any](
	name string,
	instructionFile string,
	method func(*McpTools, context.Context, T_IN) (T_OUT, error),
) ToolSpec {
	return ToolSpec{
		Name:            name,
		InstructionFile: instructionFile,
		InputSchema:     withToolInputSchema[T_IN](),
		Handler: func(tools *McpTools) server.ToolHandlerFunc {
			return mcp.NewStructuredToolHandler(func(tCtx context.Context, _ mcp.CallToolRequest, args T_IN) (T_OUT, error) {
				slog.Debug("Invoking MCP tool", "name", name)
				res, err := method(tools, tCtx, args)
				if err != nil {
					slog.Error("MCP tool execution failed", "name", name, "error", err)
				}
				return res, err
			})
		},
	}
}

// resolveMountedVersions validates the version registry and returns a map of mounted
// route keys (e.g. "v1", "preview") to their backing VersionSpec.
func resolveMountedVersions(
	versions []VersionSpec,
	defaultVersion string,
	enablePreview bool,
) (map[string]VersionSpec, error) {
	if len(versions) == 0 {
		return nil, fmt.Errorf("at least one MCP version must be registered")
	}

	seenIDs := make(map[string]struct{}, len(versions))
	var stableVersions []VersionSpec
	var previewSpec *VersionSpec

	for i := range versions {
		v := versions[i]
		id := strings.TrimSpace(v.ID)
		if id == "" {
			return nil, fmt.Errorf("MCP version ID cannot be empty")
		}
		if id == previewRouteAlias {
			return nil, fmt.Errorf("MCP version ID %q is reserved for the preview alias route", previewRouteAlias)
		}
		if strings.Contains(id, "/") {
			return nil, fmt.Errorf("MCP version ID %q cannot contain slashes", id)
		}
		if _, exists := seenIDs[id]; exists {
			return nil, fmt.Errorf("duplicate MCP version ID %q", id)
		}
		seenIDs[id] = struct{}{}
		v.ID = id

		switch v.Stage {
		case StageStable:
			stableVersions = append(stableVersions, v)
		case StagePreview:
			if previewSpec != nil {
				return nil, fmt.Errorf("at most one MCP version may have stage %q (found %q and %q)", StagePreview, previewSpec.ID, v.ID)
			}
			specCopy := v
			previewSpec = &specCopy
		default:
			return nil, fmt.Errorf("MCP version %q has invalid stage %q", v.ID, v.Stage)
		}
	}

	if len(stableVersions) == 0 {
		return nil, fmt.Errorf("at least one stable MCP version must be registered")
	}

	mounted := make(map[string]VersionSpec, len(stableVersions)+2)
	for _, v := range stableVersions {
		mounted[v.ID] = v
	}

	if enablePreview {
		if previewSpec != nil {
			mounted[previewSpec.ID] = *previewSpec
			mounted[previewRouteAlias] = *previewSpec
		} else {
			latestStable := stableVersions[len(stableVersions)-1]
			mounted[previewRouteAlias] = latestStable
		}
	}

	trimmedDefault := strings.TrimSpace(defaultVersion)
	if trimmedDefault == "" {
		return nil, fmt.Errorf("default MCP version cannot be empty")
	}
	if trimmedDefault == previewRouteAlias {
		return nil, fmt.Errorf("default MCP version cannot be %q; specify a concrete version ID", previewRouteAlias)
	}
	if _, ok := mounted[trimmedDefault]; !ok {
		return nil, fmt.Errorf("default MCP version %q is not an active mounted version", trimmedDefault)
	}

	return mounted, nil
}
