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

	"cloud.google.com/go/spanner"
	"github.com/datacommonsorg/mixer/internal/server/datasource/spannerds"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"google.golang.org/api/iterator"
)

const (
	// Maximum number of edge hops to traverse for chained properties.
	maxHops = 10
	// Used for Arc.SingleProp in Node requests and indicates that all properties should be returned.
	wildcard = "*"
	// Used for Arc.Decorator in Node requests and indicates that recursive property paths should be returned.
	chain = "+"
	where = "\n\t\tWHERE\n\t\t\t"
	and   = "\n\t\t\tAND "
)

// GetNodeProps retrieves node properties from Spanner given a list of IDs and a direction and returns a map.
func (sc *SpannerClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*spannerds.Property, error) {
	props := map[string][]*spannerds.Property{}
	if len(ids) == 0 {
		return props, nil
	}
	for _, id := range ids {
		props[id] = []*spannerds.Property{}
	}

	err := sc.queryAndCollect(
		ctx,
		*GetNodePropsQuery(ids, out),
		func() interface{} {
			return &spannerds.Property{}
		},
		func(rowStruct interface{}) {
			prop := rowStruct.(*spannerds.Property)
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
func (sc *SpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*spannerds.Edge, error) {
	edges := make(map[string][]*spannerds.Edge)
	if len(ids) == 0 {
		return edges, nil
	}
	for _, id := range ids {
		edges[id] = []*spannerds.Edge{}
	}

	// Validate input.
	if arc.Decorator != "" && (arc.SingleProp == "" || arc.SingleProp == wildcard || len(arc.BracketProps) > 0) {
		return nil, fmt.Errorf("chain expressions are only supported for a single property")
	}

	err := sc.queryAndCollect(
		ctx,
		*GetNodeEdgesByIDQuery(ids, arc, pageSize, offset),
		func() interface{} {
			return &spannerds.Edge{}
		},
		func(rowStruct interface{}) {
			edge := rowStruct.(*spannerds.Edge)
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
func (sc *SpannerClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*spannerds.Observation, error) {
	var observations []*spannerds.Observation
	if len(entities) == 0 {
		return nil, fmt.Errorf("entity must be specified")
	}

	err := sc.queryAndCollect(
		ctx,
		*GetObservationsQuery(variables, entities),
		func() interface{} {
			return &spannerds.Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*spannerds.Observation)
			observations = append(observations, observation)
		},
	)
	if err != nil {
		return observations, err
	}

	return observations, nil
}

// GetObservationsContainedInPlace retrieves observations from Spanner given a list of variables and an entity expression.
func (sc *SpannerClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*spannerds.Observation, error) {
	var observations []*spannerds.Observation
	if len(variables) == 0 || containedInPlace == nil {
		return observations, nil
	}

	err := sc.queryAndCollect(
		ctx,
		*GetObservationsContainedInPlaceQuery(variables, containedInPlace),
		func() interface{} {
			return &spannerds.Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*spannerds.Observation)
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
func (sc *SpannerClient) SearchNodes(ctx context.Context, query string, types []string) ([]*spannerds.SearchNode, error) {
	var nodes []*spannerds.SearchNode
	if query == "" {
		return nodes, nil
	}

	err := sc.queryAndCollect(
		ctx,
		*SearchNodesQuery(query, types),
		func() interface{} {
			return &spannerds.SearchNode{}
		},
		func(rowStruct interface{}) {
			node := rowStruct.(*spannerds.SearchNode)
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
	nodeToCandidates := make(map[string][]string)
	if len(nodes) == 0 {
		return nodeToCandidates, nil
	}

	// Create a map of Spanner node value to dcid to decode encoded values.
	valueMap := map[string]string{}
	for _, node := range nodes {
		value := generateValueHash(node)
		valueMap[node] = node
		valueMap[value] = node
	}

	err := sc.queryAndCollect(
		ctx,
		*ResolveByIDQuery(nodes, in, out),
		func() interface{} {
			return &spannerds.ResolutionCandidate{}
		},
		func(rowStruct interface{}) {
			resolutionCandidate := rowStruct.(*spannerds.ResolutionCandidate)
			node := valueMap[resolutionCandidate.Node]
			nodeToCandidates[node] = append(nodeToCandidates[node], resolutionCandidate.Candidate)
		},
	)
	if err != nil {
		return nil, err
	}

	return nodeToCandidates, nil
}

func (sc *SpannerClient) queryAndCollect(
	ctx context.Context,
	stmt spanner.Statement,
	newStruct func() interface{},
	withStruct func(interface{}),
) error {
	iter := sc.client.Single().Query(ctx, stmt)
	defer iter.Stop()

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
