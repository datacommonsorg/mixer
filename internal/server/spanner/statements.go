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

// SQL / GQL statements executed by the SpannerClient
var statements = struct {
	// Fetch latest CompletionTimestamp from IngestionHistory table.
	getCompletionTimestamp string
	// Filter by single parameter value.
	getParam string
	// Filter by multiple parameter values.
	getParams string
	// Fetch Properties for out arcs.
	getPropsBySubjectID string
	// Fetch Properties for in arcs.
	getPropsByObjectID string
	// Prefix for a graph query.
	graphPrefix string
	// Prefix for a graph query that matches any path.
	graphPrefixAny string
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
	// Subquery to filter edges by multiple predicates.
	filterPredicates string
	// Subquery to filter edges by object properties.
	filterProperty string
	// Subquery to filter edges by an object value.
	filterValue string
	// Subquery to filter edges by multiple object values.
	filterValues string
	// Default subquery to return Edges.
	returnEdges string
	// Default subquery to return Edges for arcs with chaining.
	returnChainedEdges string
	// Subquery to return Edges with filters.
	returnFilterEdges string
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
	// Get variables for given entity.
	getStatVarsByEntity string
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
	// Generic node pattern.
	node string
	// Generic subquery for filtering a Node.
	nodeFilter string
	// Generic triple pattern.
	triple string
	// Get data from Cache table.
	getCacheData string
	// Fetch event dates for a given type and location.
	getEventCollectionDate string
	// Fetch events for a given type, location and date.
	getEventCollectionDcids string
	// Fetch events for a given type, location and date, along with magnitude property.
	getEventCollectionDcidsWithMagnitude string
	// Fetch number of descendent stat vars for given variable groups and entities.
	countDescendentStatVars string
	// Filter descendent stat vars by import.
	filterDescendentStatVarsByImport string
	// Filter descendent stat vars by num_entities_existences.
	filterDescendentStatVarsByNumEntitiesExistence string
}{
	getCompletionTimestamp: `		SELECT
		CompletionTimestamp
		FROM
			IngestionHistory
		WHERE
			IngestionFailure = FALSE
		ORDER BY 
			CompletionTimestamp DESC
		LIMIT 1`,
	getParam:  `= @%s`,
	getParams: `IN UNNEST(@%s)`,
	getPropsBySubjectID: `		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.subject_id %s]->
		RETURN DISTINCT
			e.subject_id,
			e.predicate
		ORDER BY
			e.subject_id,
			e.predicate`,
	getPropsByObjectID: `		GRAPH DCGraph MATCH -[e:Edge
		WHERE
			e.object_id %s]->
		RETURN DISTINCT
			e.object_id AS subject_id,
			e.predicate
		ORDER BY
			subject_id,
			predicate`,
	graphPrefix:    `		GRAPH DCGraph MATCH `,
	graphPrefixAny: `		GRAPH DCGraph MATCH ANY `,
	getEdgesBySubjectID: `(m:Node
		WHERE
			m.subject_id %[1]s)-[e:Edge%[2]s]->(n:Node)`,
	getChainedEdgesBySubjectID: `(m:Node
		WHERE
			m.subject_id %s)-[e:Edge
		WHERE
			e.predicate = @predicate]->{1,%d}(n:Node)`,
	getEdgesByObjectID: `(m:Node
		WHERE
			m.subject_id %[1]s)<-[e:Edge%[2]s]-(n:Node)`,
	getChainedEdgesByObjectID: `(m:Node
		WHERE
			m.subject_id %s)<-[e:Edge
		WHERE
			e.predicate = @predicate]-{1,%d}(n:Node)`,
	filterPredicate: `
		WHERE
			e.predicate = @predicate`,
	filterPredicates: `
		WHERE
			e.predicate IN UNNEST(@predicate)`,
	filterProperty: `(n)-[filter%[1]d:Edge
		WHERE
			filter%[1]d.predicate = @prop%[1]d%s]->`,
	filterValue: `
			AND filter%[1]d.object_id = @val%[1]d`,
	filterValues: `
			AND filter%[1]d.object_id IN UNNEST(@val%[1]d)`,
	returnEdges: `
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			predicate,
			n.subject_id,
			provenance`,
	returnChainedEdges: `
		RETURN DISTINCT
			m.subject_id,
			n.subject_id AS object_id
		NEXT MATCH (n)
		WHERE
		  n.subject_id = object_id
		RETURN
		  	subject_id,
			@result_predicate AS predicate,
			'' AS provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			object_id`,
	returnFilterEdges: `
		RETURN
		  	m.subject_id,
			n.subject_id AS object_id,
			e.predicate,
			e.provenance
		NEXT MATCH (n)
		WHERE
		  n.subject_id = object_id
		RETURN
		  	subject_id,
			predicate,
			provenance,
			n.value,
			n.bytes,
			n.name,
			n.types
		ORDER BY
			subject_id,
			predicate,
			object_id,
			provenance`,
	applyOffset: `
		OFFSET %d`,
	applyLimit: `
		LIMIT %d`,
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
	selectVariableDcids: `variable_measured %s`,
	selectEntityDcids:   `observation_about %s`,
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
	getStatVarsByEntity: `		SELECT DISTINCT
			variable_measured,
			observation_about
		FROM
			Observation`,
	searchNodesByQuery: `		GRAPH DCGraph MATCH (n:Node)
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
			n.subject_id AS candidate`,
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
			i.subject_id AS candidate`,
	resolvePropToProp: `		GRAPH DCGraph MATCH <-[i:Edge
		WHERE
			i.object_id IN UNNEST(@nodes)
			AND i.predicate = @inProp]-()-[o:Edge
		WHERE
			o.predicate = @outProp]->(n:Node)
		RETURN
			i.object_id AS node,
			n.value AS candidate`,
	node: `(%[1]s:Node%[2]s)`,
	nodeFilter: `
		WHERE
			%[1]s.subject_id IN UNNEST(@%[1]s)`,
	triple: `(%[1]s:Node%[2]s)-[:Edge {predicate: @predicate%[3]d}]->(%[4]s:Node%[5]s)`,
	getCacheData: `		SELECT
			key,
			provenance,
			TO_JSON_STRING(value) AS value,
		FROM
			Cache
		WHERE
			type = @type
			AND key %s`,
	getEventCollectionDate: `		@{force_join_order=true}
		GRAPH DCGraph MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: @placeID}]->(), (event)-[:Edge {predicate: 'typeOf', object_id: @eventType}]->(), (event)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		RETURN DISTINCT 
			SUBSTR(dateNode.value, 1, 7) AS month
		ORDER BY 
			month`,
	getEventCollectionDcids: `		@{force_join_order=true}
		GRAPH DCGraph MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: @placeID}]->(), (event)-[:Edge {predicate: 'typeOf', object_id: @eventType}]->(), (event)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		WHERE 
			SUBSTR(dateNode.value, 1, 7) = @date
		RETURN DISTINCT 
			event.subject_id AS dcid`,
	getEventCollectionDcidsWithMagnitude: `		@{force_join_order=true}
		GRAPH DCGraph MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: @placeID}]->(), (event)-[:Edge {predicate: 'typeOf', object_id: @eventType}]->(), (event)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		WHERE 
			SUBSTR(dateNode.value, 1, 7) = @date
		MATCH (event)-[magEdge:Edge {predicate: @magnitudeProp}]->()
		RETURN DISTINCT 
			event.subject_id AS dcid,
			magEdge.object_id AS magnitude`,
	countDescendentStatVars: `		SELECT
			e.object_id,
			COUNT(e.subject_id) AS descendent_stat_vars
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
			SELECT variable_measured
			FROM Observation%[1]s
			GROUP BY variable_measured%[2]s	
		) o ON o.variable_measured = e.subject_id
		WHERE
		 	e.predicate = 'linkedMemberOf'
			AND e.object_id %[3]s
			GROUP BY e.object_id`,
	filterDescendentStatVarsByImport: `
			JOIN Edge@{FORCE_INDEX=InEdge} e1
			ON import_name = SUBSTR(e1.subject_id, 9)
			WHERE e1.predicate = @importPredicate
				AND e1.object_id %s`,
	filterDescendentStatVarsByNumEntitiesExistence: `
			HAVING COUNT(DISTINCT %s) >= @numEntitiesExistence`,
}
