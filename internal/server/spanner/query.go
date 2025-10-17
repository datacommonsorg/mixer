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

// Queries executed by the SpannerClient.
package spanner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

const (
	// Maximum number of edge hops to traverse for chained properties.
	MAX_HOPS = 10
	// Page size for paginated responses.
	PAGE_SIZE = 5000
)

// GetNodeProps retrieves node properties from Spanner given a list of IDs and a direction and returns a map.
func (sc *SpannerClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error) {
	props := map[string][]*Property{}
	if len(ids) == 0 {
		return props, nil
	}
	for _, id := range ids {
		props[id] = []*Property{}
	}

	err := sc.queryAndCollect(
		ctx,
		*GetNodePropsQuery(ids, out),
		func() interface{} {
			return &Property{}
		},
		func(rowStruct interface{}) {
			prop := rowStruct.(*Property)
			subjectID := prop.SubjectID
			props[subjectID] = append(props[subjectID], prop)
		},
	)
	if err != nil {
		return props, err
	}

	return props, nil
}

// GetNodeEdgesByID retrieves node edges from Spanner and returns a map of subjectID to Edges.
func (sc *SpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, offset int32) (map[string][]*Edge, error) {
	edges := make(map[string][]*Edge)
	if len(ids) == 0 {
		return edges, nil
	}
	for _, id := range ids {
		edges[id] = []*Edge{}
	}

	// Validate input.
	if arc.Decorator != "" && (arc.SingleProp == "" || arc.SingleProp == WILDCARD || len(arc.BracketProps) > 0) {
		return nil, fmt.Errorf("chain expressions are only supported for a single property")
	}

	err := sc.queryAndCollect(
		ctx,
		*GetNodeEdgesByIDQuery(ids, arc, offset),
		func() interface{} {
			return &Edge{}
		},
		func(rowStruct interface{}) {
			edge := rowStruct.(*Edge)
			subjectID := edge.SubjectID
			edges[subjectID] = append(edges[subjectID], edge)
		},
	)
	if err != nil {
		return nil, err
	}

	return edges, nil
}

// GetObservations retrieves observations from Spanner given a list of variables and entities.
func (sc *SpannerClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	var observations []*Observation
	if len(entities) == 0 {
		return nil, fmt.Errorf("entity must be specified")
	}

	err := sc.queryAndCollect(
		ctx,
		*GetObservationsQuery(variables, entities),
		func() interface{} {
			return &Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*Observation)
			observations = append(observations, observation)
		},
	)
	if err != nil {
		return observations, err
	}

	return observations, nil
}

// GetObservationsContainedInPlace retrieves observations from Spanner given a list of variables and an entity expression.
func (sc *SpannerClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || containedInPlace == nil {
		return observations, nil
	}

	err := sc.queryAndCollect(
		ctx,
		*GetObservationsContainedInPlaceQuery(variables, containedInPlace),
		func() interface{} {
			return &Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*Observation)
			observations = append(observations, observation)
		},
	)
	if err != nil {
		return observations, err
	}

	return observations, nil
}

// SearchNodes searches nodes in the graph based on the query and optionally the types.
// If the types array is empty, it searches across nodes of all types.
// A maximum of 100 results are returned.
func (sc *SpannerClient) SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error) {
	var nodes []*SearchNode
	if query == "" {
		return nodes, nil
	}

	err := sc.queryAndCollect(
		ctx,
		*SearchNodesQuery(query, types),
		func() interface{} {
			return &SearchNode{}
		},
		func(rowStruct interface{}) {
			node := rowStruct.(*SearchNode)
			nodes = append(nodes, node)
		},
	)
	if err != nil {
		return nodes, err
	}

	return nodes, nil
}

// ResolveByID fetches ID resolution candidates for a list of input nodes and in and out properties and returns a map of node to candidates.
func (sc *SpannerClient) ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error) {
	candidates := make(map[string][]string)
	if len(nodes) == 0 {
		return candidates, nil
	}

	// Create a map of Spanner node value to dcid to decode encoded values.
	valueMap := map[string]string{}
	for _, node := range nodes {
		candidates[node] = []string{}
		value := generateValueHash(node)
		valueMap[node] = node
		valueMap[value] = node
	}

	err := sc.queryAndCollect(
		ctx,
		*ResolveByIDQuery(nodes, in, out),
		func() interface{} {
			return &ResolutionCandidate{}
		},
		func(rowStruct interface{}) {
			resolutionCandidate := rowStruct.(*ResolutionCandidate)
			node := valueMap[resolutionCandidate.Node]
			candidates[node] = append(candidates[node], resolutionCandidate.Candidate)
		},
	)
	if err != nil {
		return candidates, err
	}

	return candidates, nil
}

func (sc *SpannerClient) queryAndCollect(
	ctx context.Context,
	stmt spanner.Statement,
	newStruct func() interface{},
	withStruct func(interface{}),
) error {
	timestampBound, err := sc.GetStalenessTimestampBound(ctx)
	if err != nil {
		return err
	}

	// Attempt stale read
	iter := sc.client.Single().WithTimestampBound(*timestampBound).Query(ctx, stmt)
	defer iter.Stop()

	err = sc.processRows(iter, newStruct, withStruct)

	// Check if the error is due to an expired timestamp (FAILED_PRECONDITION).
	// Currently the timestamp is set manually so can naturally get stale.
	// So for now, just log an error and fallback to a strong read.
	// TODO: Once the Spanner instance is set to periodically update the timestamp, increase severity of check, as this indicates that ingestion failed.
	if spanner.ErrCode(err) == codes.FailedPrecondition {
		slog.Error("Stale read timestamp expired (before earliest_version_time). Falling back to StrongRead.",
			"expiredTimestamp", timestampBound.String())

		// Fallback to strong read
		strongBound := spanner.StrongRead()
		iter = sc.client.Single().WithTimestampBound(strongBound).Query(ctx, stmt)
		defer iter.Stop()

		err = sc.processRows(iter, newStruct, withStruct)
	}
	if err != nil {
		return fmt.Errorf("failed to execute Spanner query after fallback attempt: %w", err)
	}

	return nil
}

func (sc *SpannerClient) processRows(iter *spanner.RowIterator, newStruct func() interface{}, withStruct func(interface{})) error {
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		rowStruct := newStruct()
		if err := row.ToStructLenient(rowStruct); err != nil {
			return fmt.Errorf("failed to parse row: %w", err)
		}
		withStruct(rowStruct)
	}
	return nil
}

// fetchCompletionTimestampFromSpanner returns the latest reported CompletionTimestamp in IngestionHistory.
func (sc *SpannerClient) fetchCompletionTimestampFromSpanner(ctx context.Context) (*time.Time, error) {
	iter := sc.client.Single().Query(ctx, *GetCompletionTimestampQuery())
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, fmt.Errorf("no rows found in IngestionHistory")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch row: %w", err)
	}

	var timestamp time.Time
	if err := row.Column(0, &timestamp); err != nil {
		return nil, fmt.Errorf("failed to read CompletionTimestamp column: %w", err)
	}

	return &timestamp, nil
}

// getCompletionTimestamp returns the latest reported CompletionTimestamp.
// It prioritizes returning a value from an in-memory cache to reduce Spanner traffic.
func (sc *SpannerClient) getCompletionTimestamp(ctx context.Context) (*time.Time, error) {
	// Check cache
	sc.cacheMutex.RLock()
	if sc.cachedTimestamp != nil && sc.clock().Before(sc.cacheExpiry) {
		sc.cacheMutex.RUnlock()
		return sc.cachedTimestamp, nil
	}
	sc.cacheMutex.RUnlock()

	// Fetch from Spanner
	sc.cacheMutex.Lock()
	defer sc.cacheMutex.Unlock()

	// Re-check the cache under the write lock (to prevent a race condition
	// where another goroutine updated it between the RUnlock and this Lock)
	if sc.cachedTimestamp != nil && sc.clock().Before(sc.cacheExpiry) {
		return sc.cachedTimestamp, nil
	}
	timestamp, err := sc.timestampFetcher(ctx)
	if err != nil {
		return nil, err
	}

	// Update cache
	sc.cachedTimestamp = timestamp
	sc.cacheExpiry = sc.clock().Add(CACHE_DURATION)

	return timestamp, nil
}

// GetStalenessTimestampBound returns the TimestampBound that should be used for stale reads in Spanner.
func (sc *SpannerClient) GetStalenessTimestampBound(ctx context.Context) (*spanner.TimestampBound, error) {
	completionTimestamp, err := sc.getCompletionTimestamp(ctx)
	if err != nil {
		return nil, err
	}

	timestampBound := spanner.ReadTimestamp(*completionTimestamp)
	return &timestampBound, nil
}
