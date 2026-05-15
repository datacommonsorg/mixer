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
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

// SpannerClient encapsulates the Spanner client.
type SpannerClient interface {
	GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error)
	GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*Edge, error)
	GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error)
	CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error)
	GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error)
	SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error)
	ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error)
	GetEventCollectionDate(ctx context.Context, placeID, eventType string) ([]string, error)
	GetEventCollectionDcids(ctx context.Context, placeID, eventType, date string) ([]EventIdWithMagnitudeDcid, error)
	GetEventCollection(ctx context.Context, req *pbv1.EventCollectionRequest) (*pbv1.EventCollection, error)
	Sparql(ctx context.Context, nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) ([][]string, error)
	GetProvenanceSummary(ctx context.Context, ids []string) (map[string]map[string]*pb.StatVarSummary_ProvenanceSummary, error)
	GetStatVarGroupNode(ctx context.Context, nodes []string, includeDefinitions bool) ([]*StatVarGroupNode, error)
	GetFilteredStatVarGroupNode(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (map[string]*FilteredStatVarGroupNode, error)
	GetFilteredTopic(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (map[string]int, error)
	VectorSearchQuery(ctx context.Context, tableName string, limit int, embeddings []float64, numLeaves int, threshold float64, nodeTypes []string) ([]*VectorSearchResult, error)
	GetTermEmbeddingQuery(ctx context.Context, modelName, searchLabel, taskType string) ([]float64, error)
	FilterNodesByTypes(ctx context.Context, nodes []string, typeFilters []string) (map[string][]string, error)
	GetSdmxObservations(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error)
	Id() string
	Start()
	Close()
}

// standardSpannerClient encapsulates the Spanner client that directly interacts with the Spanner database.
type standardSpannerClient struct {
	exec *SpannerExecutor
}

// newStandardSpannerClient creates a new standardSpannerClient.
func newStandardSpannerClient(exec *SpannerExecutor) *standardSpannerClient {
	return &standardSpannerClient{exec: exec}
}

// normalizedSchemaClient encapsulates the Spanner client for the normalized schema.
// It embeds SpannerClient to inherit default behavior and only overrides specific methods.
type normalizedSchemaClient struct {
	SpannerClient // Embeds standardSpannerClient
	exec          *SpannerExecutor
}

// NewNormalizedClient creates a new normalizedSchemaClient.
func NewNormalizedClient(client SpannerClient) (*normalizedSchemaClient, error) {
	sc, ok := client.(*standardSpannerClient)
	if !ok {
		err := fmt.Errorf("NewNormalizedClient: expected *standardSpannerClient, got %T", client)
		slog.Error("Failed to create normalized client", "error", err)
		return nil, err
	}
	return &normalizedSchemaClient{
		SpannerClient: client,
		exec:          sc.exec,
	}, nil
}

// Force compiler that all methods required by the interface are implemented by clients
var _ SpannerClient = (*standardSpannerClient)(nil)
var _ SpannerClient = (*normalizedSchemaClient)(nil)

// NewRawSpannerClient creates a new SpannerClient without the schema selector.
// This is intended for testing and internal use where a direct client is needed.
func NewRawSpannerClient(ctx context.Context, spannerConfigYaml, databaseOverride string) (SpannerClient, error) {
	cfg, err := createSpannerConfig(spannerConfigYaml, databaseOverride)
	if err != nil {
		return nil, fmt.Errorf("failed to create standardSpannerClient: %w", err)
	}
	client, err := createSpannerClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create standardSpannerClient: %w", err)
	}
	exec, err := NewSpannerExecutor(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create standardSpannerClient: %w", err)
	}
	return newStandardSpannerClient(exec), nil
}

// NewSpannerClient creates a new SpannerClient from the config yaml string and an optional database override.
// It returns a wrapper client that handles request-time schema dispatching.
func NewSpannerClient(ctx context.Context, spannerConfigYaml, databaseOverride string) (SpannerClient, error) {
	cfg, err := createSpannerConfig(spannerConfigYaml, databaseOverride)
	if err != nil {
		return nil, err
	}
	client, err := createSpannerClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	exec, err := NewSpannerExecutor(client)
	if err != nil {
		return nil, err
	}

	defaultClient := newStandardSpannerClient(exec)
	normalizedSchemaClient, err := NewNormalizedClient(defaultClient)
	if err != nil {
		return nil, err
	}

	return normalizedSchemaClient, nil
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

func (sc *standardSpannerClient) Id() string {
	return sc.exec.Id()
}

// Start starts the background goroutine to periodically fetch the timestamp.
func (sc *standardSpannerClient) Start() {
	sc.exec.Start()
}

// Close closes the Spanner client and stops the background goroutine.
func (sc *standardSpannerClient) Close() {
	sc.exec.Close()
}

// GetSdmxObservations is not supported on the default client.
func (sc *standardSpannerClient) GetSdmxObservations(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	return nil, status.Error(codes.Unimplemented, "SDMX queries are only supported on the normalized schema")
}
