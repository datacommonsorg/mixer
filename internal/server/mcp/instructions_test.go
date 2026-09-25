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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseGCSURI(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{
			name:       "bucket with nested prefix",
			uri:        "gs://my-mcp-bucket/custom/instructions/",
			wantBucket: "my-mcp-bucket",
			wantPrefix: "custom/instructions",
		},
		{
			name:       "bucket root without prefix",
			uri:        "gs://my-mcp-bucket",
			wantBucket: "my-mcp-bucket",
			wantPrefix: "",
		},
		{
			name:    "empty bucket error",
			uri:     "gs://",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotBucket, gotPrefix, err := parseGCSURI(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseGCSURI(%q) expected error, got nil", tc.uri)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseGCSURI(%q) unexpected error: %v", tc.uri, err)
			}
			if gotBucket != tc.wantBucket || gotPrefix != tc.wantPrefix {
				t.Errorf("parseGCSURI(%q) = (%q, %q), want (%q, %q)", tc.uri, gotBucket, gotPrefix, tc.wantBucket, tc.wantPrefix)
			}
		})
	}
}

func TestInstructionLoaderLocalAndGCSFallback(t *testing.T) {
	tmpDir := t.TempDir()
	customServerMd := "# Custom Local Server Instructions"
	if err := os.WriteFile(filepath.Join(tmpDir, serverInstructionsFile), []byte(customServerMd), 0644); err != nil {
		t.Fatalf("failed to write custom server.md: %v", err)
	}

	tests := []struct {
		name       string
		loader     *InstructionLoader
		filename   string
		wantSubstr string
		wantEmpty  bool
	}{
		{
			name:       "embedded default server instructions",
			loader:     &InstructionLoader{},
			filename:   serverInstructionsFile,
			wantSubstr: "Data Commons",
		},
		{
			name:       "embedded documentation extension",
			loader:     &InstructionLoader{},
			filename:   docInstructionsExtensionFile,
			wantSubstr: "https://docs.datacommons.org/llms.txt",
		},
		{
			name:       "local custom directory override",
			loader:     &InstructionLoader{customDir: tmpDir},
			filename:   serverInstructionsFile,
			wantSubstr: customServerMd,
		},
		{
			name: "GCS custom directory override",
			loader: &InstructionLoader{
				customDir: "gs://test-bucket/mcp-prompts",
				gcsRead: func(_ context.Context, bucket, object string) ([]byte, error) {
					if bucket == "test-bucket" && object == "mcp-prompts/server.md" {
						return []byte("# Custom GCS Server Instructions"), nil
					}
					return nil, fmt.Errorf("not found")
				},
			},
			filename:   serverInstructionsFile,
			wantSubstr: "# Custom GCS Server Instructions",
		},
		{
			name: "GCS error falls back to embedded instructions",
			loader: &InstructionLoader{
				customDir: "gs://test-bucket/missing",
				gcsRead: func(_ context.Context, _, _ string) ([]byte, error) {
					return nil, fmt.Errorf("object not found")
				},
			},
			filename:   serverInstructionsFile,
			wantSubstr: "Data Commons",
		},
		{
			name:      "rejects relative path traversal",
			loader:    &InstructionLoader{customDir: tmpDir},
			filename:  "../../etc/passwd",
			wantEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.loader.Load(context.Background(), tc.filename)
			if tc.wantEmpty {
				if got != "" {
					t.Errorf("Load(%q) = %q, want empty string", tc.filename, got)
				}
				return
			}
			if !strings.Contains(got, tc.wantSubstr) {
				t.Errorf("Load(%q) = %q, want substring %q", tc.filename, got, tc.wantSubstr)
			}
		})
	}
}

func TestExtractSkillDescription(t *testing.T) {
	tests := []struct {
		name     string
		markdown string
		fallback string
		want     string
	}{
		{
			name: "valid frontmatter description",
			markdown: `---
name: data-commons-researcher
description: Guide for querying single-entity Data Commons indicators.
---
# Skill Content`,
			fallback: "fallback-name",
			want:     "Guide for querying single-entity Data Commons indicators.",
		},
		{
			name:     "no frontmatter uses fallback",
			markdown: "# Heading Only",
			fallback: "fallback-name",
			want:     "fallback-name",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractSkillDescription(tc.markdown, tc.fallback)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("extractSkillDescription diff (-want +got):\n%s", diff)
			}
		})
	}
}
