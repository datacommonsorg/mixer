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
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"gopkg.in/yaml.v3"
)

// SpannerClient encapsulates the Spanner client.
type SpannerClient interface {
	GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error)
	GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*Edge, error)
	GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error)
	GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error)
	SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error)
	ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error)
	Sparql(ctx context.Context, nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) ([][]string, error)
	Id() string
	Start()
	Close()
}

// spannerDatabaseClient encapsulates the Spanner client that directly interacts with the Spanner database.
type spannerDatabaseClient struct {
	client        *spanner.Client
	useStaleReads bool
	timestamp     atomic.Int64
	ticker        Ticker
	stopCh        chan struct{}
	startOnce     sync.Once
	stopOnce      sync.Once
	wg            sync.WaitGroup

	// For mocking in tests.
	updateTimestamp func(context.Context) error
}

// newSpannerDatabaseClient creates a new spannerDatabaseClient.
func newSpannerDatabaseClient(client *spanner.Client, useStaleReads bool) (*spannerDatabaseClient, error) {
	sc := &spannerDatabaseClient{
		client:        client,
		useStaleReads: useStaleReads,
	}

	if !useStaleReads {
		return sc, nil
	}

	// Set an initial timestamp synchronously before starting the background loop.
	sc.ticker = NewTimestampTicker()
	sc.stopCh = make(chan struct{})
	sc.updateTimestamp = sc.fetchAndUpdateTimestamp
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sc.updateTimestamp(ctx); err != nil {
		slog.Error("Error initializing Spanner staleness timestamp")
		return nil, err
	}
	return sc, nil
}

// NewSpannerClient creates a new SpannerClient from the config yaml string and an optional database override.
func NewSpannerClient(ctx context.Context, spannerConfigYaml, databaseOverride string, useStaleReads bool) (SpannerClient, error) {
	cfg, err := createSpannerConfig(spannerConfigYaml, databaseOverride)
	if err != nil {
		return nil, fmt.Errorf("failed to create spannerDatabaseClient: %w", err)
	}
	client, err := createSpannerClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create spannerDatabaseClient: %w", err)
	}
	return newSpannerDatabaseClient(client, useStaleReads)
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

func (sc *spannerDatabaseClient) Id() string {
	return sc.client.DatabaseName()
}

// Start starts the background goroutine to periodically fetch the timestamp.
func (sc *spannerDatabaseClient) Start() {
	if !sc.useStaleReads {
		return
	}

	sc.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())

		sc.wg.Add(1)
		go func() {
			// Defer statements are processed in LIFO order.
			// Mark the wait group as done.
			defer sc.wg.Done()
			// Cancel the context to clean up any in-flight operations.
			defer cancel()
			// Stop the ticker.
			defer sc.ticker.Stop()

			for {
				select {
				case <-sc.stopCh:
					return
				case <-sc.ticker.C():
					// Ignore the error here to allow the process to continue running
					// even if one fetch fails. The previous timestamp remains in cache.
					err := sc.updateTimestamp(ctx)
					if err != nil {
						slog.Error("Error updating Spanner staleness timestamp", "error", err)
					}
				}
			}
		}()
	})
}

// Close closes the Spanner client and stops the background goroutine.
func (sc *spannerDatabaseClient) Close() {
	sc.stopOnce.Do(func() {
		if sc.useStaleReads {
			close(sc.stopCh)
		}

		sc.wg.Wait()

		if sc.client != nil {
			sc.client.Close()
		}
	})
}
