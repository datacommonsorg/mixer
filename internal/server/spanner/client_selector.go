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
