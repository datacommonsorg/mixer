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

// A spanner client wrapped.
package spanner

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"gopkg.in/yaml.v3"
)

// SpannerClient encapsulates the Spanner client.
type SpannerClient struct {
	client *spanner.Client
	// Store SQL statements for better organization
	statements struct {
		getNodesByID string
	}
}

// newSpannerClient creates a new SpannerClient and initializes the statements.
func newSpannerClient(client *spanner.Client) *SpannerClient {
	sc := &SpannerClient{client: client}

	// Initialize the SQL statements
	sc.statements.getNodesByID = `
	SELECT id, typeOf, name, properties, provenances
	FROM Node
	WHERE id IN UNNEST(@ids)
	`

	return sc
}

// GetNodesByID retrieves nodes from Spanner given a list of IDs and returns a map.
func (sc *SpannerClient) GetNodesByID(ctx context.Context, ids []string) (map[string]*Node, error) {
	nodes := make(map[string]*Node)
	if len(ids) == 0 {
		return nodes, nil
	}

	stmt := spanner.Statement{
		SQL:    sc.statements.getNodesByID,
		Params: map[string]interface{}{"ids": ids},
	}

	iter := sc.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch row: %w", err)
		}

		var node Node
		if err := row.ToStruct(&node); err != nil {
			return nil, fmt.Errorf("failed to parse row: %w", err)
		}
		nodes[node.ID] = &node
	}

	return nodes, nil
}

// NewSpannerClient creates a new SpannerClient from the yaml config file.
func NewSpannerClient(ctx context.Context, yamlPath string) (*SpannerClient, error) {
	cfg, err := readSpannerConfig(yamlPath)
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

// readSpannerConfig reads the config from the yaml path and returns a SpannerConfig object.
func readSpannerConfig(yamlPath string) (*SpannerConfig, error) {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read spanner yaml file: %w", err)
	}

	var cfg SpannerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}
