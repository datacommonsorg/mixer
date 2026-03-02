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
	"github.com/datacommonsorg/mixer/internal/metrics"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

const (
	// Maximum number of edge hops to traverse for chained properties.
	maxHops = 10
	where   = "\n\t\tWHERE\n\t\t\t"
	and     = "\n\t\t\tAND "
)

// GetNodeProps retrieves node properties from Spanner given a list of IDs and a direction and returns a map.
func (sc *spannerDatabaseClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error) {
	props := map[string][]*Property{}
	if len(ids) == 0 {
		return props, nil
	}
	for _, id := range ids {
		props[id] = []*Property{}
	}

	err := sc.queryStructs(
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
func (sc *spannerDatabaseClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*Edge, error) {
	edges := make(map[string][]*Edge)
	if len(ids) == 0 {
		return edges, nil
	}
	for _, id := range ids {
		edges[id] = []*Edge{}
	}

	err := sc.queryStructs(
		ctx,
		*GetNodeEdgesByIDQuery(ids, arc, pageSize, offset),
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
func (sc *spannerDatabaseClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	var observations []*Observation
	if len(entities) == 0 {
		return nil, fmt.Errorf("entity must be specified")
	}

	err := sc.queryStructs(
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
func (sc *spannerDatabaseClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || containedInPlace == nil {
		return observations, nil
	}

	err := sc.queryStructs(
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
func (sc *spannerDatabaseClient) SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error) {
	var nodes []*SearchNode
	if query == "" {
		return nodes, nil
	}

	err := sc.queryStructs(
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
func (sc *spannerDatabaseClient) ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error) {
	nodeToCandidates := make(map[string][]string)
	if len(nodes) == 0 {
		return nodeToCandidates, nil
	}

	// Create a map of Spanner node value to dcid to decode encoded values.
	valueMap := map[string]string{}
	for _, node := range nodes {
		value := generateObjectValue(node)
		valueMap[node] = node
		valueMap[value] = node
	}

	err := sc.queryStructs(
		ctx,
		*ResolveByIDQuery(nodes, in, out),
		func() interface{} {
			return &ResolutionCandidate{}
		},
		func(rowStruct interface{}) {
			resolutionCandidate := rowStruct.(*ResolutionCandidate)
			node := valueMap[resolutionCandidate.Node]
			nodeToCandidates[node] = append(nodeToCandidates[node], resolutionCandidate.Candidate)
		},
	)
	if err != nil {
		return nil, err
	}

	return nodeToCandidates, nil
}

func (sc *spannerDatabaseClient) Sparql(ctx context.Context, nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) ([][]string, error) {
	query, err := SparqlQuery(nodes, queries, opts)
	if err != nil {
		return nil, fmt.Errorf("error building sparql query: %v", err)
	}

	return sc.queryDynamic(ctx, *query)
}

// fetchAndUpdateTimestamp queries Spanner and updates the timestamp.
func (sc *spannerDatabaseClient) fetchAndUpdateTimestamp(ctx context.Context) error {
	iter := sc.client.Single().Query(ctx, *GetCompletionTimestampQuery())
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return fmt.Errorf("no valid rows found in IngestionHistory")
	}
	if err != nil {
		return fmt.Errorf("failed to fetch row: %w", err)
	}

	var timestamp time.Time
	if err := row.Column(0, &timestamp); err != nil {
		return fmt.Errorf("failed to read CompletionTimestamp column: %w", err)
	}

	sc.timestamp.Store(timestamp.UnixNano())
	return nil
}

func (sc *spannerDatabaseClient) getStalenessTimestamp() (time.Time, error) {
	val := sc.timestamp.Load()
	if val != 0 {
		return time.Unix(0, val).UTC(), nil
	}
	slog.Error("Spanner staleness timestamp not available")
	return time.Time{}, fmt.Errorf("error getting staleness timestamp")
}

func (sc *spannerDatabaseClient) executeQuery(
	ctx context.Context,
	stmt spanner.Statement,
	handleRows func(*spanner.RowIterator) error,
) error {
	runQuery := func(tb spanner.TimestampBound) error {
		metrics.RecordSpannerQuery(ctx)
		iter := sc.client.Single().WithTimestampBound(tb).Query(ctx, stmt)
		defer iter.Stop()
		return handleRows(iter)
	}

	if sc.useStaleReads {
		ts, err := sc.getStalenessTimestamp()
		if err != nil {
			return err
		}
		err = runQuery(spanner.ReadTimestamp(ts))

		// Log error if timestamp is older than retention and fall back to strong read.
		if spanner.ErrCode(err) == codes.FailedPrecondition {
			slog.Error("Stale read timestamp expired. Falling back to StrongRead.",
				"expiredTimestamp", ts.String())
			return runQuery(spanner.StrongRead())
		}
		return err
	}
	return runQuery(spanner.StrongRead())
}

// queryStructs executes a query and maps the results to an input struct.
func (sc *spannerDatabaseClient) queryStructs(
	ctx context.Context,
	stmt spanner.Statement,
	newStruct func() interface{},
	withStruct func(interface{}),
) error {
	return sc.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		return sc.processRows(iter, newStruct, withStruct)
	})
}

// queryDynamic executes a dynamically constructed query and returns the results as a slice of string slices.
func (sc *spannerDatabaseClient) queryDynamic(
	ctx context.Context,
	stmt spanner.Statement,
) ([][]string, error) {
	var rowData [][]string
	err := sc.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		result, err := sc.processDynamicRows(iter)
		rowData = result
		return err
	})
	return rowData, err
}

func (sc *spannerDatabaseClient) processRows(iter *spanner.RowIterator, newStruct func() interface{}, withStruct func(interface{})) error {
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to fetch row: %w", err)
		}

		rowStruct := newStruct()
		if err := row.ToStructLenient(rowStruct); err != nil {
			return fmt.Errorf("failed to parse row: %w", err)
		}
		withStruct(rowStruct)
	}

	return nil
}

// processDynamicRows processes rows from dynamically constructed queries.
func (sc *spannerDatabaseClient) processDynamicRows(iter *spanner.RowIterator) ([][]string, error) {
	rowData := [][]string{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return rowData, err
		}

		data := []string{}
		for i := 0; i < row.Size(); i++ {
			var val spanner.GenericColumnValue
			if err := row.Column(i, &val); err != nil {
				return rowData, err
			}
			data = append(data, val.Value.GetStringValue())
		}
		rowData = append(rowData, data)
	}
	return rowData, nil
}
