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

package spanner

import (
	"context"
	"fmt"
	"log/slog"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

// schemaSelectorClient dispatches calls to either default or normalized client.
type schemaSelectorClient struct {
	SpannerClient // Embeds the default client
	normalized    *normalizedClient
}

// GetObservations overrides the embedded client's GetObservations to dispatch based on schema selection.
func (s *schemaSelectorClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
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
func (s *schemaSelectorClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
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
func (s *schemaSelectorClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
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

func (s *schemaSelectorClient) GetMultiEntityObservations(
	ctx context.Context,
	variables []string,
	dimensions []*pbv2.ObservationDimensionConstraint,
) ([]*multiEntityObservation, error) {
	if !useNormalizedSchema(ctx) {
		return nil, fmt.Errorf("multi-entity observations require normalized Spanner schema")
	}
	logNormalizedInvocation("GetMultiEntityObservations",
		"num_variables", len(variables),
		"num_dimensions", len(dimensions),
	)
	return s.normalized.GetMultiEntityObservations(ctx, variables, dimensions)
}

// NewSchemaSelectorClient creates a new SpannerClient that dispatches calls to either default or normalized client.
func NewSchemaSelectorClient(baseClient SpannerClient) (SpannerClient, error) {
	normalizedClient, err := NewNormalizedClient(baseClient)
	if err != nil {
		return nil, err
	}

	return &schemaSelectorClient{
		SpannerClient: baseClient,
		normalized:    normalizedClient,
	}, nil
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
