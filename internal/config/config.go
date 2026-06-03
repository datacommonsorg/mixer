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

	"gopkg.in/yaml.v3"
)

type MixerConfig struct {
	EmbeddingsService *EmbeddingsServiceConfig `yaml:"embeddings_service"`
}

type EmbeddingsServiceConfig struct {
	URL                 *string           `yaml:"url"`
	DefaultIndex        *string           `yaml:"default_index"`
	ResolveIndexMapping map[string]string `yaml:"resolve_index_mapping"`
}

// ParsedEmbeddingsConfig holds the parsed and merged configuration for the embeddings service.
type ParsedEmbeddingsConfig struct {
	URL                 string
	DefaultIndex        string
	ResolveIndexMapping map[string]string
}

// ParseConfig merges the configuration loaded from YAML with overriding CLI flags.
// flagDefaultIndexes can be a comma-separated list of indexes, which takes precedence
// over the default_index set in YAML.
func ParseConfig(
	userCfg *EmbeddingsServiceConfig,
	flagDefaultIndexes, flagServerURL string,
) *ParsedEmbeddingsConfig {
	cfg := &ParsedEmbeddingsConfig{
		ResolveIndexMapping: make(map[string]string),
	}

	// Apply User Config YAML
	if userCfg != nil {
		if userCfg.URL != nil {
			cfg.URL = *userCfg.URL
		}
		if userCfg.DefaultIndex != nil {
			cfg.DefaultIndex = *userCfg.DefaultIndex
		}
		if userCfg.ResolveIndexMapping != nil {
			for k, v := range userCfg.ResolveIndexMapping {
				cfg.ResolveIndexMapping[k] = v
			}
		}
	}

	// CLI Flags Override (takes precedence for backward compatibility)
	if flagDefaultIndexes != "" {
		cfg.DefaultIndex = flagDefaultIndexes
	}
	if flagServerURL != "" {
		cfg.URL = flagServerURL
	}

	return cfg
}

// LoadConfig loads and parses the MixerConfig from the given path (local or GCS).
// Returns nil config if path is empty.
func LoadConfig(ctx context.Context, path string) (*MixerConfig, error) {
	if path == "" {
		return nil, nil
	}
	bytes, err := ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	var cfg MixerConfig
	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
