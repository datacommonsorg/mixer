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
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxConcurrentSdmxContainedInPlaceExpansions = 3

// RelationExpressionProcessor implements dispatcher.Processor to expand relation expressions
// by fetching child entities from the provided datasource.
type RelationExpressionProcessor struct {
	source                        datasource.DataSource
	sdmxRemotePlaceExpansionLimit int
}

// NewRelationExpressionProcessor creates a new RelationExpressionProcessor.
func NewRelationExpressionProcessor(
	source datasource.DataSource,
	sdmxRemotePlaceExpansionLimit int,
) *RelationExpressionProcessor {
	return &RelationExpressionProcessor{
		source:                        source,
		sdmxRemotePlaceExpansionLimit: sdmxRemotePlaceExpansionLimit,
	}
}

// PreProcess handles expression expansion using the configured source.
// TODO: Mark local-only responses caused by remote expansion failures as
// non-cacheable so transient failures are not cached by Redis.
func (p *RelationExpressionProcessor) PreProcess(rc *RequestContext) (Outcome, error) {
	if rc.Type == TypeSdmxData {
		return p.preProcessSdmxData(rc)
	}

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

func (p *RelationExpressionProcessor) preProcessSdmxData(rc *RequestContext) (Outcome, error) {
	if p.source == nil {
		return Continue, nil
	}
	req, ok := rc.CurrentRequest.(*sdmx.SdmxDataQuery)
	if !ok {
		slog.Error("RelationExpressionProcessor: failed to cast request to SdmxDataQuery", "type", rc.Type)
		return Continue, fmt.Errorf("failed to cast request to SdmxDataQuery")
	}

	componentToContainedInPlace, err := datacommons.ContainedInPlaceConstraints(req.GetConstraints())
	if err != nil {
		return Continue, err
	}
	if len(componentToContainedInPlace) == 0 {
		return Continue, nil
	}

	relationSet := map[datacommons.ContainedInPlaceConstraint]struct{}{}
	for _, relation := range componentToContainedInPlace {
		relationSet[relation] = struct{}{}
	}
	relations := slices.SortedFunc(maps.Keys(relationSet), func(a, b datacommons.ContainedInPlaceConstraint) int {
		if result := cmp.Compare(a.Ancestor, b.Ancestor); result != 0 {
			return result
		}
		return cmp.Compare(a.ChildPlaceType, b.ChildPlaceType)
	})
	slog.Debug("RelationExpressionProcessor: expanding SDMX containment relations", "count", len(relations))

	type expansionResult struct {
		dcids []string
	}
	results := make([]expansionResult, len(relations))
	group, groupCtx := errgroup.WithContext(rc.Context)
	group.SetLimit(maxConcurrentSdmxContainedInPlaceExpansions)
	for i, relation := range relations {
		i, relation := i, relation
		group.Go(func() error {
			if err := groupCtx.Err(); err != nil {
				return err
			}
			start := time.Now()
			dcids, err := p.fetchSdmxEntities(groupCtx, relation)
			if err != nil {
				var limitErr *sdmxRemoteExpansionLimitError
				if errors.As(err, &limitErr) {
					slog.Info("RelationExpressionProcessor: SDMX containment expansion exceeded limit",
						"ancestor", relation.Ancestor,
						"childPlaceType", relation.ChildPlaceType,
						"limit", limitErr.limit,
					)
					return limitErr
				}
				// TODO: Propagate context cancellation and deadline errors instead of
				// falling back to local-only results when the shared Observation
				// relation-expansion behavior is updated.
				slog.Warn("RelationExpressionProcessor: falling back to local-only SDMX containment expansion",
					"ancestor", relation.Ancestor,
					"childPlaceType", relation.ChildPlaceType,
					"duration", time.Since(start),
					"error", err,
				)
				return nil
			}
			results[i].dcids = dcids
			slog.Debug("RelationExpressionProcessor: resolved SDMX containment relation",
				"ancestor", relation.Ancestor,
				"childPlaceType", relation.ChildPlaceType,
				"count", len(dcids),
				"duration", time.Since(start),
			)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		var limitErr *sdmxRemoteExpansionLimitError
		if errors.As(err, &limitErr) {
			return Continue, status.Error(codes.InvalidArgument, limitErr.Error())
		}
		return Continue, err
	}

	containedInPlaceToRemoteDCIDs := SdmxContainedInPlaceToRemoteDCIDs{}
	for i, relation := range relations {
		if len(results[i].dcids) > 0 {
			containedInPlaceToRemoteDCIDs[relation] = results[i].dcids
		}
	}
	if len(containedInPlaceToRemoteDCIDs) > 0 {
		rc.Context = WithSdmxContainedInPlaceToRemoteDCIDs(rc.Context, containedInPlaceToRemoteDCIDs)
	}
	return Continue, nil
}

type sdmxRemoteExpansionLimitError struct {
	relation datacommons.ContainedInPlaceConstraint
	limit    int
}

func (e *sdmxRemoteExpansionLimitError) Error() string {
	return fmt.Sprintf(
		"SDMX containedInPlace+ expansion for ancestor %q and typeOf %q exceeds the limit of %d places; choose a narrower ancestor or child place type",
		e.relation.Ancestor,
		e.relation.ChildPlaceType,
		e.limit,
	)
}

type sdmxRemoteExpansionSource struct {
	datasource.DataSource
	relation datacommons.ContainedInPlaceConstraint
	limit    int
	dcids    map[string]struct{}
}

func (s *sdmxRemoteExpansionSource) Node(
	ctx context.Context,
	req *pbv2.NodeRequest,
	pageSize int,
) (*pbv2.NodeResponse, error) {
	resp, err := s.DataSource.Node(ctx, req, pageSize)
	if err != nil {
		return nil, err
	}
	for _, graph := range resp.GetData() {
		for _, nodes := range graph.GetArcs() {
			for _, node := range nodes.GetNodes() {
				if node.GetDcid() == "" {
					continue
				}
				s.dcids[node.GetDcid()] = struct{}{}
				if len(s.dcids) > s.limit {
					return nil, &sdmxRemoteExpansionLimitError{relation: s.relation, limit: s.limit}
				}
			}
		}
	}
	return resp, nil
}

func (p *RelationExpressionProcessor) fetchSdmxEntities(
	ctx context.Context,
	relation datacommons.ContainedInPlaceConstraint,
) ([]string, error) {
	property := fmt.Sprintf("<-containedInPlace+{typeOf:%s}", relation.ChildPlaceType)
	nodeReq := &pbv2.NodeRequest{
		Nodes:    []string{relation.Ancestor},
		Property: property,
	}
	limitedSource := &sdmxRemoteExpansionSource{
		DataSource: p.source,
		relation:   relation,
		limit:      p.sdmxRemotePlaceExpansionLimit,
		dcids:      map[string]struct{}{},
	}
	if _, err := datasource.NodeFetchAll(ctx, limitedSource, nodeReq, datasources.DefaultPageSize); err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(limitedSource.dcids)), nil
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
