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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
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

// selectorClient dispatches calls to either default or normalized client based on request headers.
// It serves as the main entry point for the Spanner client, centralizing routing concerns.
//
// DESIGN NOTE: This client embeds the standard client (SpannerClient) to handle automatic
// fallback for methods that do not have a specialized normalized implementation.
// For methods that DO have a specialized implementation (like GetObservations), it explicitly
// checks the header and routes accordingly. It does NOT rely on the normalized client's
// internal fallback for general request routing, ensuring that the standard path remains
// the explicit default.
type selectorClient struct {
	SpannerClient // Embeds the standard client as the default client
	normalized    SpannerClient
}

// NewSpannerClient creates a new SpannerClient from the config yaml string and an optional database override.
// It returns a wrapper client that handles request-time schema dispatching.
func NewSpannerClient(ctx context.Context, spannerConfigYaml, databaseOverride string) (SpannerClient, error) {
	cfg, err := createSpannerConfig(spannerConfigYaml, databaseOverride)
	if err != nil {
		return nil, err
	}
	exec, err := NewSpannerConnector(ctx, cfg)
	if err != nil {
		return nil, err
	}

	defaultClient := newStandardSpannerClient(exec)
	normalizedClient := NewNormalizedClient(defaultClient)

	return &selectorClient{
		SpannerClient: defaultClient,
		normalized:    normalizedClient,
	}, nil
}



// GetObservations overrides the embedded client's GetObservations to dispatch based on schema selection.
func (s *selectorClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	if useNormalizedSchema(ctx) {
		logNormalizedInvocation("GetObservations",
			"num_variables", len(variables),
			"num_entities", len(entities),
		)
		return s.normalized.GetObservations(ctx, variables, entities)
	}
	return s.SpannerClient.GetObservations(ctx, variables, entities)
}

// CheckVariableExistence overrides the embedded client's CheckVariableExistence to dispatch based on schema selection.
func (s *selectorClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	if useNormalizedSchema(ctx) {
		logNormalizedInvocation("CheckVariableExistence",
			"num_variables", len(variables),
			"num_entities", len(entities),
		)
		return s.normalized.CheckVariableExistence(ctx, variables, entities)
	}
	return s.SpannerClient.CheckVariableExistence(ctx, variables, entities)
}

// GetObservationsContainedInPlace overrides the embedded client's GetObservationsContainedInPlace to dispatch based on schema selection.
func (s *selectorClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	if useNormalizedSchema(ctx) {
		logNormalizedInvocation("GetObservationsContainedInPlace",
			"num_variables", len(variables),
			"ancestor", containedInPlace.Ancestor,
			"child_place_type", containedInPlace.ChildPlaceType,
		)
		return s.normalized.GetObservationsContainedInPlace(ctx, variables, containedInPlace)
	}
	return s.SpannerClient.GetObservationsContainedInPlace(ctx, variables, containedInPlace)
}

// GetSdmxObservations overrides the embedded client's GetSdmxObservations.
// SDMX is only supported on the normalized schema, so it always delegates to the normalized client.
func (s *selectorClient) GetSdmxObservations(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	logNormalizedInvocation("GetSdmxObservations",
		"query", req,
	)
	return s.normalized.GetSdmxObservations(ctx, req)
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

// useNormalizedSchema checks whether to use the normalized Spanner schema based on request header.
func useNormalizedSchema(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		headers := md.Get(util.XUseNormalizedSchema)
		return len(headers) > 0 && headers[0] == "true"
	}
	return false
}

// logNormalizedInvocation logs that the normalized schema was invoked for a method with custom arguments.
func logNormalizedInvocation(methodName string, args ...any) {
	fullArgs := append([]any{"method", methodName}, args...)
	slog.Info("Invoking normalized Spanner schema", fullArgs...)
}


