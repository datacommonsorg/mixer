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
	"log/slog"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// schemaSelectorClient dispatches calls to either default or multi-entity client.
type schemaSelectorClient struct {
	SpannerClient // Embeds the default client
	multiEntity   *multiEntityClient
}

// GetObservations overrides the embedded client's GetObservations to dispatch based on schema selection.
func (s *schemaSelectorClient) GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*Observation, error) {
	if useMultiEntitySchema(ctx) {
		logMultiEntityInvocation("GetObservations",
			"num_variables", len(variables),
			"num_entities", len(entities),
			"date", date,
		)
		return s.multiEntity.GetObservations(ctx, variables, entities, date)
	}
	return s.SpannerClient.GetObservations(ctx, variables, entities, date)
}

// CheckVariableExistence overrides the embedded client's CheckVariableExistence to dispatch based on schema selection.
func (s *schemaSelectorClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	if useMultiEntitySchema(ctx) {
		logMultiEntityInvocation("CheckVariableExistence",
			"num_variables", len(variables),
			"num_entities", len(entities),
		)
		return s.multiEntity.CheckVariableExistence(ctx, variables, entities)
	}
	return s.SpannerClient.CheckVariableExistence(ctx, variables, entities)
}

// CheckVariableSourceExistence delegates to the embedded client.
func (s *schemaSelectorClient) CheckVariableSourceExistence(ctx context.Context, variables []string, sources []string, predicate string) ([][]string, error) {
	// Source existence is backed by Cache/Edge provenance data, not the
	// observation storage schema. Delegate to the base client so source and
	// dataset existence requests keep working when multi-entity observation
	// reads are enabled.
	return s.SpannerClient.CheckVariableSourceExistence(ctx, variables, sources, predicate)
}

// CheckVariableGroupPlaceExistence overrides the embedded client's CheckVariableGroupPlaceExistence to dispatch based on schema selection.
func (s *schemaSelectorClient) CheckVariableGroupPlaceExistence(ctx context.Context, variableGroups []string, entities []string, predicate string) ([][]string, error) {
	if useMultiEntitySchema(ctx) {
		logMultiEntityInvocation("CheckVariableGroupPlaceExistence",
			"num_variable_groups", len(variableGroups),
			"num_entities", len(entities),
			"predicate", predicate,
		)
		return s.multiEntity.CheckVariableGroupPlaceExistence(ctx, variableGroups, entities, predicate)
	}
	return s.SpannerClient.CheckVariableGroupPlaceExistence(ctx, variableGroups, entities, predicate)
}

// GetObservationsContainedInPlace overrides the embedded client's GetObservationsContainedInPlace to dispatch based on schema selection.
func (s *schemaSelectorClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace, date string) ([]*Observation, error) {
	if containedInPlace == nil {
		return s.SpannerClient.GetObservationsContainedInPlace(ctx, variables, containedInPlace, date)
	}
	if useMultiEntitySchema(ctx) {
		logMultiEntityInvocation("GetObservationsContainedInPlace",
			"num_variables", len(variables),
			"ancestor", containedInPlace.Ancestor,
			"child_place_type", containedInPlace.ChildPlaceType,
			"date", date,
		)
		return s.multiEntity.GetObservationsContainedInPlace(ctx, variables, containedInPlace, date)
	}
	return s.SpannerClient.GetObservationsContainedInPlace(ctx, variables, containedInPlace, date)
}

// GetStatVarGroupNode overrides the embedded client's GetStatVarGroupNode to dispatch based on schema selection.
func (s *schemaSelectorClient) GetStatVarGroupNode(ctx context.Context, nodes []string, includeDefinitions bool) ([]*StatVarGroupNode, error) {
	if useMultiEntitySchema(ctx) {
		logMultiEntityInvocation("GetStatVarGroupNode",
			"num_nodes", len(nodes),
			"include_definitions", includeDefinitions,
		)
		return s.multiEntity.GetStatVarGroupNode(ctx, nodes, includeDefinitions)
	}
	return s.SpannerClient.GetStatVarGroupNode(ctx, nodes, includeDefinitions)
}

// GetSdmxObservations overrides the embedded client's GetSdmxObservations.
// SDMX is not supported in multi-entity schema.
func (s *schemaSelectorClient) GetSdmxObservations(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	return nil, status.Error(codes.Unimplemented, "SDMX is not supported on multi-entity schema")
}

// NewSchemaSelectorClient creates a new SpannerClient that dispatches calls to either default or multi-entity client.
func NewSchemaSelectorClient(baseClient SpannerClient) (SpannerClient, error) {
	multiEntityClient, err := NewMultiEntityClient(baseClient)
	if err != nil {
		return nil, err
	}

	return &schemaSelectorClient{
		SpannerClient: baseClient,
		multiEntity:   multiEntityClient,
	}, nil
}

// useMultiEntitySchema checks whether to use the multi-entity Spanner schema based on request header.
func useMultiEntitySchema(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		headers := md.Get(util.XUseMultiEntitySchema)
		return len(headers) > 0 && headers[0] == "true"
	}
	return false
}

// shouldLogSQL checks whether to log the full interpolated SQL query based on request header.
func shouldLogSQL(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		headers := md.Get(util.XLogSQL)
		return len(headers) > 0 && headers[0] == "true"
	}
	return false
}

// getSchemaName returns the name of the schema being used based on context.
func getSchemaName(ctx context.Context) string {
	if useMultiEntitySchema(ctx) {
		return "MultiEntity"
	}
	return "Legacy"
}

// logMultiEntityInvocation logs that the multi-entity schema was invoked for a method with custom arguments.
func logMultiEntityInvocation(methodName string, args ...any) {
	fullArgs := append([]any{"method", methodName}, args...)
	slog.Info("Invoking multi-entity Spanner schema", fullArgs...)
}
