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
	"google.golang.org/api/iterator"
)

// SQL / GQL statements executed by the SpannerClient
var statements = struct {
	getEdgesBySubjectID string
}{
	getEdgesBySubjectID: `
	SELECT 
		subject_id, 
		predicate, 
		COALESCE(object_id, '') AS object_id, 
		COALESCE(object_value, '') AS object_value, 
		COALESCE(provenance, '') AS provenance
	FROM Edge
	WHERE subject_id IN UNNEST(@ids)
	`,
}

// GetNodeEdgesByID retrieves node edges from Spanner given a list of IDs and returns a map.
func (sc *SpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string) (map[string][]*Edge, error) {
	edges := make(map[string][]*Edge)
	if len(ids) == 0 {
		return edges, nil
	}

	stmt := spanner.Statement{
		SQL:    statements.getEdgesBySubjectID,
		Params: map[string]interface{}{"ids": ids},
	}

	err := sc.queryAndCollect(
		ctx,
		stmt,
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
		return edges, err
	}

	return edges, nil
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
