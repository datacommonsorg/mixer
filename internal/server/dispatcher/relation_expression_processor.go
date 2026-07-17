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

package dispatcher

import (
	"context"
	"fmt"
	"log/slog"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

// RelationExpressionProcessor implements dispatcher.Processor to expand relation expressions
// by fetching child entities from the provided datasource.
type RelationExpressionProcessor struct {
	source datasource.DataSource
}

// NewRelationExpressionProcessor creates a new RelationExpressionProcessor.
func NewRelationExpressionProcessor(source datasource.DataSource) *RelationExpressionProcessor {
	return &RelationExpressionProcessor{
		source: source,
	}
}

// PreProcess handles expression expansion using the configured source.
func (p *RelationExpressionProcessor) PreProcess(rc *RequestContext) (Outcome, error) {
	// Only process observation requests.
	if rc.Type != TypeObservation {
		return Continue, nil
	}

	req, ok := rc.CurrentRequest.(*pbv2.ObservationRequest)
	if !ok {
		slog.Error("RelationExpressionProcessor: failed to cast request to ObservationRequest", "type", rc.Type)
		return Continue, fmt.Errorf("failed to cast request to ObservationRequest")
	}

	// Only process if there is an expression and a source is available.
	if req.Entity == nil || req.Entity.Expression == "" || p.source == nil {
		return Continue, nil
	}

	expr := req.Entity.Expression
	slog.Debug("RelationExpressionProcessor: expanding expression", "expression", expr)

	// Parse the expression to extract ancestor and child type.
	containedInPlace, err := v2.ParseContainedInPlace(expr)
	if err != nil {
		slog.Error("RelationExpressionProcessor: failed to parse expression", "expression", expr, "error", err)
		return Continue, fmt.Errorf("failed to parse expression: %w", err)
	}

	// Fetch entities from source.
	dcids, err := p.fetchEntities(rc.Context, p.source, containedInPlace)
	if err != nil {
		slog.Warn("RelationExpressionProcessor: falling back to local-only expression expansion", "error", err)
		return Continue, nil
	}

	// Add the list of DCIDs to context
	rc.Context = context.WithValue(rc.Context, RelationExpressionExpandedEntities, dcids)
	slog.Debug("RelationExpressionProcessor: resolved expression", "expression", expr, "count", len(dcids))

	return Continue, nil
}

// fetchEntities calls the source to expand the expression.
// Example:
//
//	Inputs: containedInPlace={Ancestor: "geoId/06", ChildPlaceType: "County"}
//	Outputs: []string{"geoId/06001", "geoId/06003"}, nil
func (p *RelationExpressionProcessor) fetchEntities(
	ctx context.Context,
	source datasource.DataSource,
	containedInPlace *v2.ContainedInPlace,
) ([]string, error) {
	property := fmt.Sprintf("<-containedInPlace+{typeOf:%s}", containedInPlace.ChildPlaceType)
	nodeReq := &pbv2.NodeRequest{
		Nodes:    []string{containedInPlace.Ancestor},
		Property: property,
	}

	// Call Node API on the source, fetching all pages.
	resp, err := datasource.NodeFetchAll(ctx, source, nodeReq, datasources.DefaultPageSize)
	if err != nil {
		return nil, err
	}

	var dcids []string
	for _, graph := range resp.Data {
		for _, nodes := range graph.Arcs {
			for _, node := range nodes.Nodes {
				if node.Dcid != "" {
					dcids = append(dcids, node.Dcid)
				}
			}
		}
	}
	return dcids, nil
}

// PostProcess is a no-op for this processor.
func (p *RelationExpressionProcessor) PostProcess(rc *RequestContext) (Outcome, error) {
	return Continue, nil
}
