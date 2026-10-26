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
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"google.golang.org/grpc/status"
)

// VersionStage represents the lifecycle stage of an MCP version.
type VersionStage string

const (
	// StageStable indicates a released MCP version mounted in all environments.
	StageStable VersionStage = "stable"
	// StagePreview indicates an in-development MCP version mounted only at /mcp/preview when --enable_mcp_preview is set.
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
		V2Spec(),
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
					return res, toolErrorForLLM(err)
				}
				return res, nil
			})
		},
	}
}

// toolErrorForLLM strips the gRPC status wrapper ("rpc error: code = ... desc = ...") from err so the
// tool result shown to the LLM contains only the actionable message. Non-status errors are unchanged.
func toolErrorForLLM(err error) error {
	if st, ok := status.FromError(err); ok {
		return errors.New(st.Message())
	}
	return err
}

// resolveMountedVersions validates the version registry and returns a map of mounted
// route keys (e.g. "v1", "preview") to their backing VersionSpec.
func resolveMountedVersions(
	versions []VersionSpec,
	defaultVersion string,
	enablePreview bool,
) (map[string]VersionSpec, error) {
	stableVersions, previewSpec, err := partitionVersions(versions)
	if err != nil {
		return nil, err
	}
	mounted := buildMountedRoutes(stableVersions, previewSpec, enablePreview)
	if err := validateDefaultVersion(defaultVersion, mounted); err != nil {
		return nil, err
	}
	return mounted, nil
}

// partitionVersions validates all registered VersionSpecs and separates them into
// stable versions and at most one preview version.
func partitionVersions(versions []VersionSpec) ([]VersionSpec, *VersionSpec, error) {
	if len(versions) == 0 {
		return nil, nil, fmt.Errorf("at least one MCP version must be registered")
	}

	seenIDs := make(map[string]struct{}, len(versions))
	var stableVersions []VersionSpec
	var previewSpec *VersionSpec

	for i := range versions {
		v := versions[i]
		id := strings.TrimSpace(v.ID)
		if err := validateVersionID(id, seenIDs); err != nil {
			return nil, nil, err
		}
		seenIDs[id] = struct{}{}
		v.ID = id

		switch v.Stage {
		case StageStable:
			stableVersions = append(stableVersions, v)
		case StagePreview:
			if previewSpec != nil {
				return nil, nil, fmt.Errorf("at most one MCP version may have stage %q (found %q and %q)", StagePreview, previewSpec.ID, v.ID)
			}
			specCopy := v
			previewSpec = &specCopy
		default:
			return nil, nil, fmt.Errorf("MCP version %q has invalid stage %q", v.ID, v.Stage)
		}
	}

	if len(stableVersions) == 0 {
		return nil, nil, fmt.Errorf("at least one stable MCP version must be registered")
	}
	return stableVersions, previewSpec, nil
}

// validateVersionID verifies that a version ID is non-empty, not reserved, slash-free, and unique.
func validateVersionID(id string, seenIDs map[string]struct{}) error {
	if id == "" {
		return fmt.Errorf("MCP version ID cannot be empty")
	}
	if id == previewRouteAlias {
		return fmt.Errorf("MCP version ID %q is reserved for the preview alias route", previewRouteAlias)
	}
	if strings.Contains(id, "/") {
		return fmt.Errorf("MCP version ID %q cannot contain slashes", id)
	}
	if _, exists := seenIDs[id]; exists {
		return fmt.Errorf("duplicate MCP version ID %q", id)
	}
	return nil
}

// buildMountedRoutes constructs the map of active route keys to VersionSpecs based on
// stable versions, any preview version, and whether preview routing is enabled.
func buildMountedRoutes(
	stableVersions []VersionSpec,
	previewSpec *VersionSpec,
	enablePreview bool,
) map[string]VersionSpec {
	mounted := make(map[string]VersionSpec, len(stableVersions)+2)
	for _, v := range stableVersions {
		mounted[v.ID] = v
	}

	if enablePreview {
		if previewSpec != nil {
			// A preview-stage version is served only via the /mcp/preview alias; its concrete
			// /mcp/<id> route is mounted once the version is promoted to StageStable.
			mounted[previewRouteAlias] = *previewSpec
		} else {
			latestStable := stableVersions[len(stableVersions)-1]
			mounted[previewRouteAlias] = latestStable
		}
	}
	return mounted
}

// validateDefaultVersion checks that defaultVersion is a concrete, mounted version ID.
func validateDefaultVersion(defaultVersion string, mounted map[string]VersionSpec) error {
	trimmedDefault := strings.TrimSpace(defaultVersion)
	if trimmedDefault == "" {
		return fmt.Errorf("default MCP version cannot be empty")
	}
	if trimmedDefault == previewRouteAlias {
		return fmt.Errorf("default MCP version cannot be %q; specify a concrete version ID", previewRouteAlias)
	}
	if _, ok := mounted[trimmedDefault]; !ok {
		return fmt.Errorf("default MCP version %q is not an active mounted version", trimmedDefault)
	}
	return nil
}
