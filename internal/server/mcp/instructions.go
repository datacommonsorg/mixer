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
	"embed"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
)

const (
	serverInstructionsFile                    = "server.md"
	docInstructionsExtensionFile              = "doc_instructions_extension.md"
	searchIndicatorsInstructionFile           = "tools/search_indicators.md"
	searchChildIndicatorsInstructionFile      = "tools/search_child_indicators.md"
	getVariableMetadataInstructionFile        = "tools/get_variable_metadata.md"
	getObservationsInstructionFile            = "tools/get_observations.md"
	getChildObservationsInstructionFile       = "tools/get_child_observations.md"
	getMultiEntityObservationsInstructionFile = "tools/get_multi_entity_observations.md"
)

var defaultSkillNames = []string{
	"data-commons-child-places-researcher",
	"data-commons-multi-entity-researcher",
	"data-commons-researcher",
}

//go:embed instructions
var instructionsFS embed.FS

// gcsObjectReader abstracts reading an object from a GCS bucket for testability.
type gcsObjectReader func(ctx context.Context, bucket, object string) ([]byte, error)

// InstructionLoader loads markdown instruction files from an optional custom directory
// (local path or gs:// URI), falling back to embedded instructions.
type InstructionLoader struct {
	customDir string
	gcsRead   gcsObjectReader
	closeFn   func() error
}

// NewInstructionLoader creates an InstructionLoader configured with an optional custom directory.
func NewInstructionLoader(ctx context.Context, customDir string) *InstructionLoader {
	trimmed := strings.TrimSpace(customDir)
	var reader gcsObjectReader
	var closeFn func() error
	if strings.HasPrefix(trimmed, "gs://") {
		client, err := storage.NewClient(ctx)
		if err != nil {
			slog.Warn("Failed to create GCS client for custom MCP instructions; falling back to embedded instructions", "customDir", trimmed, "error", err)
		} else {
			closeFn = client.Close
			reader = func(rCtx context.Context, bucket, object string) ([]byte, error) {
				rc, err := client.Bucket(bucket).Object(object).NewReader(rCtx)
				if err != nil {
					return nil, err
				}
				defer func() { _ = rc.Close() }()
				return io.ReadAll(rc)
			}
		}
	}
	return &InstructionLoader{
		customDir: trimmed,
		gcsRead:   reader,
		closeFn:   closeFn,
	}
}

// Close releases any underlying client resources held by the InstructionLoader.
func (l *InstructionLoader) Close() error {
	if l.closeFn != nil {
		return l.closeFn()
	}
	return nil
}

// Load reads a markdown instruction file relative to the instructions root (e.g. "server.md"
// or "tools/search_indicators.md"), checking customDir first before falling back to embedded files.
func (l *InstructionLoader) Load(ctx context.Context, filename string) string {
	cleanRel := path.Clean(strings.ReplaceAll(strings.TrimPrefix(filename, "/"), "\\", "/"))
	if cleanRel == "." || strings.HasPrefix(cleanRel, "..") {
		slog.Warn("Rejected invalid relative instruction path", "filename", filename)
		return ""
	}

	if l.customDir != "" {
		content, err := l.readExternalContent(ctx, cleanRel)
		if err == nil {
			return string(content)
		}
		slog.Debug("Custom instruction file not found in custom directory; falling back to embedded", "customDir", l.customDir, "filename", cleanRel, "error", err)
	}

	content, err := instructionsFS.ReadFile(path.Join("instructions", cleanRel))
	if err != nil {
		slog.Warn("Failed to read embedded instruction file", "filename", cleanRel, "error", err)
		return ""
	}
	return string(content)
}

// readExternalContent reads a relative file path from either a gs:// URI or a local directory.
func (l *InstructionLoader) readExternalContent(ctx context.Context, cleanRel string) ([]byte, error) {
	if strings.HasPrefix(l.customDir, "gs://") {
		if l.gcsRead == nil {
			return nil, fmt.Errorf("GCS reader is not initialized")
		}
		bucket, prefix, err := parseGCSURI(l.customDir)
		if err != nil {
			return nil, err
		}
		objectPath := cleanRel
		if prefix != "" {
			objectPath = path.Join(prefix, cleanRel)
		}
		return l.gcsRead(ctx, bucket, objectPath)
	}
	return os.ReadFile(filepath.Join(l.customDir, filepath.FromSlash(cleanRel)))
}

// parseGCSURI splits a gs://bucket/optional/prefix URI into bucket and object prefix.
func parseGCSURI(uri string) (bucket string, prefix string, err error) {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(uri), "gs://"), "/")
	if trimmed == "" {
		return "", "", fmt.Errorf("empty GCS bucket in URI %q", uri)
	}
	parts := strings.SplitN(trimmed, "/", 2)
	if parts[0] == "" {
		return "", "", fmt.Errorf("invalid GCS bucket in URI %q", uri)
	}
	if len(parts) > 1 {
		prefix = parts[1]
	}
	return parts[0], prefix, nil
}
