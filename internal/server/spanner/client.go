// Copyright 2024 Google LLC
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

// A spanner client wrapper.
package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"gopkg.in/yaml.v3"
)

// SpannerClient encapsulates the Spanner client.
type SpannerClient struct {
	client *spanner.Client
}

// newSpannerClient creates a new SpannerClient.
func newSpannerClient(client *spanner.Client) *SpannerClient {
	return &SpannerClient{client: client}
}

// NewSpannerClient creates a new SpannerClient from the config yaml string.
func NewSpannerClient(ctx context.Context, spannerConfigYaml string) (*SpannerClient, error) {
	cfg, err := createSpannerConfig(spannerConfigYaml)
	if err != nil {
		return nil, fmt.Errorf("failed to create SpannerClient: %w", err)
	}
	client, err := createSpannerClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create SpannerClient: %w", err)
	}
	return newSpannerClient(client), nil
}

// createSpannerClient creates the database name string and initializes the Spanner client.
func createSpannerClient(ctx context.Context, cfg *SpannerConfig) (*spanner.Client, error) {
	// Construct the database name string
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)

	// Create the Spanner client
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %w", err)
	}

	return client, nil
}

// createSpannerConfig creates the config from specific yaml string.
func createSpannerConfig(spannerConfigYaml string) (*SpannerConfig, error) {
	var cfg SpannerConfig
	if err := yaml.Unmarshal([]byte(spannerConfigYaml), &cfg); err != nil {
		return nil, fmt.Errorf("failed to create spanner config: %w", err)
	}

	return &cfg, nil
}
