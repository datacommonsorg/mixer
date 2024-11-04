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
	getNodesByID string
}{
	getNodesByID: `
	SELECT id, typeOf, name, properties, provenances
	FROM Node
	WHERE id IN UNNEST(@ids)
	`,
}

// GetNodesByID retrieves nodes from Spanner given a list of IDs and returns a map.
func (sc *SpannerClient) GetNodesByID(ctx context.Context, ids []string) (map[string]*Node, error) {
	nodes := make(map[string]*Node)
	if len(ids) == 0 {
		return nodes, nil
	}

	stmt := spanner.Statement{
		SQL:    statements.getNodesByID,
		Params: map[string]interface{}{"ids": ids},
	}

	iter := sc.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch row: %w", err)
		}

		var node Node
		if err := row.ToStructLenient(&node); err != nil {
			return nil, fmt.Errorf("failed to parse row: %w", err)
		}
		nodes[node.ID] = &node
	}

	return nodes, nil
}
