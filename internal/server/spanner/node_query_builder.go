// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"fmt"

	"cloud.google.com/go/spanner"
)

const nodeContainedInPlaceTypeFirstPattern = `(n:Node)-[@{FORCE_INDEX=InEdge}filter0:Edge
		WHERE
			filter0.predicate = @prop0
			AND filter0.object_id IN UNNEST(@val0)]->,
		@{FORCE_JOIN_ORDER=TRUE}
		(m:Node
		WHERE
			m.subject_id %[1]s)<-[e:Edge
		WHERE
			e.predicate = @predicate]-(n:Node)`

const nodeContainedInPlaceAncestorFirstPattern = `(m:Node
		WHERE
			m.subject_id %[1]s)<-[e:Edge
		WHERE
			e.predicate = @predicate]-(n:Node),
		@{FORCE_JOIN_ORDER=TRUE}
		(n)-[@{FORCE_INDEX=InEdge}filter0:Edge
		WHERE
			filter0.predicate = @prop0
			AND filter0.object_id IN UNNEST(@val0)]->`

// GetNodeContainedInPlaceEdgesByIDQuery builds the optimized Node containment query.
func GetNodeContainedInPlaceEdgesByIDQuery(
	ids []string,
	childPlaceType string,
	pageSize, offset int,
	queryConfig QueryConfig,
) *spanner.Statement {
	idFilter, idVal := getParamStatement("id", ids)
	params := map[string]interface{}{
		"id":        idVal,
		"predicate": linkedContainedInPlaceProperty,
		"prop0":     typeOfProperty,
		"val0":      addObjectValues([]string{childPlaceType}),
	}

	query := nodeContainedInPlaceTypeFirstPattern
	if queryConfig.containedInPlaceAccessPath(childPlaceType) == containedInPlaceAncestorFirst {
		query = nodeContainedInPlaceAncestorFirstPattern
	}
	template := statements.graphPrefix + fmt.Sprintf(query, idFilter) + statements.returnFilterEdges
	template = applyNodeQueryPagination(template, pageSize, offset)

	return &spanner.Statement{
		SQL:    template,
		Params: params,
	}
}

func applyNodeQueryPagination(query string, pageSize, offset int) string {
	if offset > 0 {
		query += fmt.Sprintf(statements.applyOffset, offset)
	}
	// Request pageSize+1 rows to determine whether to generate nextToken.
	return query + fmt.Sprintf(statements.applyLimit, pageSize+1)
}
