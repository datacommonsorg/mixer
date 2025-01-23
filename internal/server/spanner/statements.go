// Copyright 2025 Google LLC
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

// Query statements used by the SpannerClient.
package spanner

import "fmt"

// SQL / GQL statements executed by the SpannerClient
var statements = struct {
	// Fetch Properties for out arcs
	getPropsBySubjectID string
	// Fetch Properties for in arcs
	getPropsByObjectID string
	// Fetch Edges for out arcs with a single hop
	getEdgesBySubjectID string
	// Fetch Edges for out arcs with chaining
	getChainedEdgesBySubjectID string
	// Fetch Edges for in arcs with a single hop
	getEdgesByObjectID string
	// Fetch Edges for in arcs with chaining
	getChainedEdgesByObjectID string
	// Subquery to filter edges by predicate
	filterProps string
	// Subquery to filter edges by object property-values
	filterObjects string
	// Fetch Observations for variable+entity.
	getObsByVariableAndEntity string
	// Fetch observations for variable + contained in place.
	getObsByVariableAndContainedInPlace string
	// Search nodes by name only.
	searchNodesByQuery string
	// Search nodes by query and type(s).
	searchNodesByQueryAndTypes string
}{
	getPropsBySubjectID: `
		GRAPH DCGraph MATCH -[e:Edge
		WHERE 
			e.subject_id IN UNNEST(@ids)]->
		RETURN DISTINCT
			e.subject_id,
			e.predicate
		ORDER BY
			e.subject_id,
			e.predicate
	`,
	getPropsByObjectID: `
		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.object_id IN UNNEST(@ids)
			AND e.object_value IS NULL
		]->
		RETURN DISTINCT
			e.object_id AS subject_id,
			e.predicate
		ORDER BY
			subject_id,
			predicate
	`,
	getEdgesBySubjectID: `
		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.subject_id IN UNNEST(@ids)
			AND e.object_value IS NULL%[1]s]->(n:Node)
		RETURN 
			e.subject_id,
			e.predicate,
			e.object_id,
			'' as object_value,
			COALESCE(e.provenance, '') AS provenance,
			COALESCE(n.name, '') AS name,
			COALESCE(n.types, []) AS types
		UNION ALL
		MATCH -[e:Edge
		WHERE
			e.subject_id IN UNNEST(@ids)
			AND e.object_value IS NOT NULL%[1]s]-> 
		RETURN 
			e.subject_id,
			e.predicate,
			'' as object_id,
			e.object_value,
			e.provenance,
			'' AS name,
			ARRAY<STRING>[] AS types
		ORDER BY
			subject_id,
			predicate,
			object_id,
			object_value
	`,
	getChainedEdgesBySubjectID: fmt.Sprintf(`
		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))-[e:Edge
		WHERE
			e.predicate = @predicate]->{1,%d}(n:Node)
		WHERE 
			m != n
		RETURN DISTINCT 
			m.subject_id,
			n.subject_id as object_id,
			'' as object_value,
			COALESCE(n.name, '') AS name
		UNION ALL
		MATCH -[e:Edge
		WHERE
			e.subject_id IN UNNEST(@ids)
			AND e.object_value IS NOT NULL
			AND e.predicate = @predicate]-> 
		RETURN 
			e.subject_id,
			'' AS object_id,
			e.object_value,
			'' AS name
		NEXT
		RETURN
			subject_id,
			@result_predicate AS predicate,
			object_id,
			object_value,
			'' AS provenance,
			name, 
			ARRAY<STRING>[] AS types
		ORDER BY
			subject_id,
			predicate,
			object_id,
			object_value
	`, MAX_HOPS),
	getEdgesByObjectID: `
		GRAPH DCGraph MATCH <-[e:Edge
		WHERE
			e.object_id IN UNNEST(@ids)
			AND e.subject_id != e.object_id%s]-(n:Node) 
		RETURN 
			e.object_id AS subject_id,
			e.predicate,
			e.subject_id AS object_id,
			'' AS object_value,
			COALESCE(e.provenance, '') AS provenance,
			COALESCE(n.name, '') AS name,
			COALESCE(n.types, []) AS types
		ORDER BY
			subject_id,
			predicate,
			object_id
	`,
	getChainedEdgesByObjectID: fmt.Sprintf(`
		GRAPH DCGraph MATCH (m:Node
		WHERE m.subject_id IN UNNEST(@ids))<-[e:Edge
		WHERE
			e.predicate = @predicate]-{1,%d}(n:Node) 
		WHERE
			m!= n	
		RETURN DISTINCT 
			m.subject_id,
			n.subject_id AS object_id,
			COALESCE(n.name, '') AS name
		NEXT
		RETURN
			subject_id, 
			@result_predicate AS predicate,
			object_id,
			'' AS object_value,
			'' AS provenance, 
			name, 
			ARRAY<STRING>[] AS types
		ORDER BY
			subject_id,
			predicate,
			object_id
		`, MAX_HOPS),
	filterProps: `
		AND e.predicate IN UNNEST(@props)
	`,
	filterObjects: `
		NEXT 
		MATCH -[e:Edge 
		WHERE
			e.predicate = @prop%[1]d
			AND (
				e.object_id IN UNNEST(@val%[1]d)
				OR e.object_value IN UNNEST(@val%[1]d)
			)]-> 
		WHERE
			e.subject_id = object_id
		RETURN
			subject_id,
			predicate,
			object_id,
			object_value,
			provenance,
			name,
			types			
	`,
	getObsByVariableAndEntity: `
		SELECT
			variable_measured,
			observation_about,
			observations,
			provenance,
			COALESCE(observation_period, '') AS observation_period,
			COALESCE(measurement_method, '') AS measurement_method,
			COALESCE(unit, '') AS unit,
			COALESCE(scaling_factor, '') AS scaling_factor,
			import_name,
			provenance_url
		FROM Observation
		WHERE
			variable_measured IN UNNEST(@variables) AND
			observation_about IN UNNEST(@entities)
	`,
	getObsByVariableAndContainedInPlace: `
		SELECT
			obs.variable_measured,
			obs.observation_about,
			obs.observations,
			obs.provenance,
			COALESCE(obs.observation_period, '') AS observation_period,
			COALESCE(obs.measurement_method, '') AS measurement_method,
			COALESCE(obs.unit, '') AS unit,
			COALESCE(obs.scaling_factor, '') AS scaling_factor,
			obs.import_name, 
			obs.provenance_url
		FROM GRAPH_TABLE (
				DCGraph MATCH <-[e:Edge
				WHERE
					e.object_id = @ancestor
					AND e.subject_id != e.object_id
					AND e.predicate = 'linkedContainedInPlace']-
				RETURN 
				e.subject_id as object_id
				NEXT
				MATCH -[e:Edge 
				WHERE
					e.predicate = 'typeOf'
					AND e.object_id = @childPlaceType]-> 
				WHERE e.subject_id = object_id
				RETURN object_id
			)result
		INNER JOIN (
		SELECT
			*
		FROM Observation
		WHERE
			variable_measured IN UNNEST(@variables))obs
		ON 
			result.object_id = obs.observation_about
	`,
	searchNodesByQuery: `
		GRAPH DCGraph
		MATCH (n:Node)
		WHERE 
			SEARCH(n.name_tokenlist, @query)
		RETURN n.subject_id, n.name, n.types, SCORE(n.name_tokenlist, @query, enhance_query => TRUE) AS score 
		ORDER BY score + IF(n.name = @query, 1, 0) DESC, n.name ASC
		LIMIT 100
	`,
	searchNodesByQueryAndTypes: `
		GRAPH DCGraph
		MATCH (n:Node)
		WHERE 
			SEARCH(n.name_tokenlist, @query)
			AND ARRAY_INCLUDES_ANY(n.types, @types)
		RETURN n.subject_id, n.name, n.types, SCORE(n.name_tokenlist, @query, enhance_query => TRUE) AS score
		ORDER BY score + IF(n.name = @query, 1, 0) DESC, n.name ASC
		LIMIT 100
	`,
}
