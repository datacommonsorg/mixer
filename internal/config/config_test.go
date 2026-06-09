// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadConfig(t *testing.T) {
	ctx := context.Background()

	// Test case 1: Empty path
	cfg, err := LoadConfig(ctx, "")
	if err != nil {
		t.Errorf("LoadConfig(\"\") returned unexpected error: %v", err)
	}
	if cfg != nil {
		t.Errorf("LoadConfig(\"\") expected nil config, got %+v", cfg)
	}

	// Test case 2: Non-existent file
	_, err = LoadConfig(ctx, "non_existent_file.yaml")
	if err == nil {
		t.Error("LoadConfig(non_existent_file.yaml) expected error, got nil")
	}

	// Setup temp directory for file tests
	tmpDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Test case 3: Valid YAML
	validYAML := `
embeddings_service:
  url: "http://test-server"
  default_index: "test_index"
  resolve_index_mapping:
    label1: index1
`
	validPath := filepath.Join(tmpDir, "valid_config.yaml")
	if err := os.WriteFile(validPath, []byte(validYAML), 0644); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}

	cfg, err = LoadConfig(ctx, validPath)
	if err != nil {
		t.Errorf("LoadConfig(valid) returned unexpected error: %v", err)
	}
	wantURL := "http://test-server"
	wantDefault := "test_index"
	want := &MixerConfig{
		EmbeddingsService: &EmbeddingsServiceConfig{
			URL:          &wantURL,
			DefaultIndex: &wantDefault,
			ResolveIndexMapping: map[string]string{
				"label1": "index1",
			},
		},
	}
	if diff := cmp.Diff(want, cfg); diff != "" {
		t.Errorf("LoadConfig(valid) mismatch (-want +got):\n%s", diff)
	}

	// Test case 4: Invalid YAML
	invalidYAML := `
embeddings_service: {
`
	invalidPath := filepath.Join(tmpDir, "invalid_config.yaml")
	if err := os.WriteFile(invalidPath, []byte(invalidYAML), 0644); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}

	_, err = LoadConfig(ctx, invalidPath)
	if err == nil {
		t.Error("LoadConfig(invalid) expected error, got nil")
	}
}

func TestParseConfig(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	tests := []struct {
		name               string
		userCfg            *EmbeddingsServiceConfig
		flagDefaultIndexes string
		flagServerURL      string
		want               *ParsedEmbeddingsConfig
	}{
		{
			name:               "Defaults only (nil config, empty flags)",
			userCfg:            nil,
			flagDefaultIndexes: "",
			flagServerURL:      "",
			want: &ParsedEmbeddingsConfig{
				URL:                 "",
				DefaultIndex:        "",
				ResolveIndexMapping: map[string]string{},
			},
		},
		{
			name: "YAML config only",
			userCfg: &EmbeddingsServiceConfig{
				URL:          strPtr("http://yaml-server"),
				DefaultIndex: strPtr("yaml_default"),
				ResolveIndexMapping: map[string]string{
					"custom-label": "custom_index",
					"base-nl":      "yaml_override_nl",
				},
			},
			flagDefaultIndexes: "",
			flagServerURL:      "",
			want: &ParsedEmbeddingsConfig{
				URL:          "http://yaml-server",
				DefaultIndex: "yaml_default",
				ResolveIndexMapping: map[string]string{
					"base-nl":      "yaml_override_nl",
					"custom-label": "custom_index",
				},
			},
		},
		{
			name: "Flags override YAML",
			userCfg: &EmbeddingsServiceConfig{
				URL:          strPtr("http://yaml-server"),
				DefaultIndex: strPtr("yaml_default"),
				ResolveIndexMapping: map[string]string{
					"multi-entity": "yaml_multi",
				},
			},
			flagDefaultIndexes: "flag_default",
			flagServerURL:      "http://flag-server",
			want: &ParsedEmbeddingsConfig{
				URL:          "http://flag-server",
				DefaultIndex: "flag_default",
				ResolveIndexMapping: map[string]string{
					"multi-entity": "yaml_multi",
				},
			},
		},
		{
			name:               "Flags only",
			userCfg:            nil,
			flagDefaultIndexes: "flag_default",
			flagServerURL:      "http://flag-server",
			want: &ParsedEmbeddingsConfig{
				URL:          "http://flag-server",
				DefaultIndex: "flag_default",
				ResolveIndexMapping: map[string]string{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseConfig(tc.userCfg, tc.flagDefaultIndexes, tc.flagServerURL)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ParseConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
