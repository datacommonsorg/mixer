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

import (
	"fmt"
)

// SQL / GQL statements executed by the SpannerClient
var statements = struct {
	// Fetch Properties for out arcs.
	getPropsBySubjectID string
	// Fetch Properties for in arcs.
	getPropsByObjectID string
	// Fetch Edges for out arcs with a single hop.
	getEdgesBySubjectID string
	// Fetch Edges for out arcs with chaining.
	getChainedEdgesBySubjectID string
	// Fetch Edges for in arcs with a single hop.
	getEdgesByObjectID string
	// Fetch Edges for in arcs with chaining.
	getChainedEdgesByObjectID string
	// Subquery to filter edges by predicate.
	filterPredicate string
	// Subquery to filter edges by object properties.
	filterProperty string
	// Subquery to filter edges by object values.
	filterValue string
	// Default subquery to return Edges.
	returnEdges string
	// Default subquery to return Edges for arcs with chaining.
	returnChainedEdges string
	// Subquery to return Edges with filters.
	returnFilterEdges string
	// Subquery to return Edges for arcs with chaining and filters.
	returnFilterChainedEdges string
	// Subquery to apply page offset.
	applyOffset string
	// Subquery to apply page limit.
	applyLimit string
	// Fetch Observations.
	getObs string
	// Filter by variable dcids.
	selectVariableDcids string
	// Filter by entity dcids.
	selectEntityDcids string
	// Fetch observations for variable + contained in place.
	getObsByVariableAndContainedInPlace string
	// Search nodes by name only.
	searchNodesByQuery string
	// Subquery to filter search results by types.
	filterTypes string
	// Resolve dcid to dcid (ie dcid search).
	resolveDcidToDcid string
	// Resolve dcid to other property.
	resolveDcidToProp string
	// Resolve other property to dcid.
	resolvePropToDcid string
	// Resolve one property to another.
	resolvePropToProp string
}{
	getPropsBySubjectID: `		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.subject_id IN UNNEST(@ids)]->
		RETURN DISTINCT
			e.subject_id,
			e.predicate
		ORDER BY
			e.subject_id,
			e.predicate`,
	getPropsByObjectID: `		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.object_id IN UNNEST(@ids)]->
		RETURN DISTINCT
			e.object_id AS subject_id,
			e.predicate
		ORDER BY
			subject_id,
			predicate`,
	getEdgesBySubjectID: `		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))-[e:Edge%s]->(n:Node)%s
		ORDER BY
			subject_id,
			predicate,
			value,
			provenance`,
	getChainedEdgesBySubjectID: `		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))-[e:Edge
		WHERE
			e.predicate = @predicate]->{1,%d}(n:Node)%s
		ORDER BY
			subject_id,
			value`,
	getEdgesByObjectID: `		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))<-[e:Edge%s]-(n:Node)%s
		ORDER BY
			subject_id,
			predicate,
			value,
			provenance`,
	getChainedEdgesByObjectID: `		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id IN UNNEST(@ids))<-[e:Edge
		WHERE
			e.predicate = @predicate]-{1,%d}(n:Node)%s
		ORDER BY
			subject_id,
			value`,
	filterPredicate: `
		WHERE
			e.predicate IN UNNEST(@predicates)`,
	filterProperty: `
		,(n)-[filter%[1]d:Edge
		WHERE
			filter%[1]d.predicate = @prop%[1]d%s]->`,
	filterValue: `
			AND filter%[1]d.object_id IN UNNEST(@val%[1]d)`,
	returnEdges: `
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			n.value,
			n.bytes,
			n.name,
			n.types`,
	returnChainedEdges: `
		RETURN
			m.subject_id,
			@result_predicate AS predicate,
			'' AS provenance,
			n.value,
			n.bytes,
			n.name,
			n.types`,
	returnFilterEdges: `
		RETURN
		  m.subject_id,
			n.subject_id AS value,
			e.predicate,
			e.provenance
		NEXT MATCH (n)
		WHERE
		  n.subject_id = value
		RETURN
		  subject_id,
			predicate,
			provenance,
			n.value,
			n.bytes,
			n.name,
			n.types`,
	returnFilterChainedEdges: `
		RETURN
			m.subject_id,
			n.subject_id AS value
		NEXT MATCH (n)
		WHERE
		  n.subject_id = value
		RETURN
		  subject_id,
			@result_predicate AS predicate,
			'' AS provenance,
			n.value,
			n.bytes,
			n.name,
			n.types`,
	applyOffset: `
		OFFSET %d`,
	applyLimit: fmt.Sprintf(`
		LIMIT %d`, PAGE_SIZE+1),
	getObs: `		SELECT
			variable_measured,
			observation_about,
			observations,
			import_name,
			observation_period,
			measurement_method,
			unit,
			scaling_factor,
			provenance_url,
			is_dc_aggregate,
			facet_id
		FROM 
			Observation`,
	selectVariableDcids: `variable_measured IN UNNEST(@variables)`,
	selectEntityDcids:   `observation_about IN UNNEST(@entities)`,
	getObsByVariableAndContainedInPlace: `		SELECT
			obs.variable_measured,
			obs.observation_about,
			obs.observations,
			obs.import_name,
			obs.observation_period,
			obs.measurement_method,
			obs.unit,
			obs.scaling_factor,
			obs.provenance_url,
			obs.is_dc_aggregate,
			obs.facet_id
		FROM 
			GRAPH_TABLE (
				DCGraph MATCH <-[e:Edge
				WHERE
					e.object_id = @ancestor
					AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]->
				RETURN
					e.subject_id as object_id
			)result
		INNER JOIN (%s)obs
		ON 
			result.object_id = obs.observation_about`,
	searchNodesByQuery: `
		GRAPH DCGraph MATCH (n:Node)
		WHERE
			SEARCH(n.name_tokenlist, @query)%s
		RETURN
			n.subject_id, 
			n.name,
			n.types, 
			SCORE(n.name_tokenlist, @query, enhance_query => TRUE) AS score 
		ORDER BY 
			score + IF(n.name = @query, 1, 0) DESC,
			n.name ASC
		LIMIT %d`,
	filterTypes: `
		AND ARRAY_INCLUDES_ANY(n.types, @types)`,
	resolveDcidToDcid: `		GRAPH DCGraph MATCH (n:Node
		WHERE
			n.subject_id IN UNNEST(@nodes))
		RETURN
			n.subject_id AS node,
			n.subject_id AS candidate
	`,
	resolveDcidToProp: `		GRAPH DCGraph MATCH -[o:Edge
		WHERE 
			o.subject_id IN UNNEST(@nodes)
			AND o.predicate = @outProp]->(n:Node)
		RETURN
			o.subject_id AS node,
			n.value AS candidate`,
	resolvePropToDcid: `		GRAPH DCGraph MATCH <-[i:Edge
		WHERE
			i.object_id IN UNNEST(@nodes)
			AND i.predicate = @inProp]-
		RETURN
			i.object_id AS node,
			i.subject_id AS candidate
	`,
	resolvePropToProp: `		GRAPH DCGraph MATCH <-[i:Edge
		WHERE
			i.object_id IN UNNEST(@nodes)
			AND i.predicate = @inProp]-()-[o:Edge
		WHERE
			o.predicate = @outProp]->(n:Node)
		RETURN
			i.object_id AS node,
			n.value AS candidate
		`,
}
