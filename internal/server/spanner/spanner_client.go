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
	"log/slog"

	"cloud.google.com/go/spanner"
	"gopkg.in/yaml.v3"
)

// SpannerClientImpl encapsulates the Spanner client.
type SpannerClientImpl struct {
	client *spanner.Client
}

// newSpannerClientImpl creates a new SpannerClientImpl.
func newSpannerClientImpl(client *spanner.Client) *SpannerClientImpl {
	return &SpannerClientImpl{client: client}
}

// NewSpannerClientImpl creates a new SpannerClientImpl from the config yaml string and an optional database override.
func NewSpannerClientImpl(ctx context.Context, spannerConfigYaml, databaseOverride string) (*SpannerClientImpl, error) {
	cfg, err := createSpannerConfig(spannerConfigYaml, databaseOverride)
	if err != nil {
		return nil, fmt.Errorf("failed to create SpannerClient: %w", err)
	}
	client, err := createSpannerClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create SpannerClient: %w", err)
	}
	return newSpannerClientImpl(client), nil
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

// createSpannerConfig creates the config from the specific yaml string and an optional database override.
func createSpannerConfig(spannerConfigYaml, databaseOverride string) (*SpannerConfig, error) {
	var cfg SpannerConfig
	if err := yaml.Unmarshal([]byte(spannerConfigYaml), &cfg); err != nil {
		return nil, fmt.Errorf("failed to create spanner config: %w", err)
	}

	// Override database with flag value if set.
	// This is temporary during development to allow fast rollout of version changes.
	// TODO: Once the Spanner instance is stable, revert to using the config.
	if databaseOverride != "" {
		slog.Debug("Setting Spanner database value from flag", "value", databaseOverride)
		cfg.Database = databaseOverride
	}

	return &cfg, nil
}

func (sc *SpannerClientImpl) Id() string {
	return sc.client.DatabaseName()
}

func (sc *SpannerClientImpl) Close() {
	sc.client.Close()
}
