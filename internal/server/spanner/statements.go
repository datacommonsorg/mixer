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
	// Fetch StatVarGroupNode info.
	getStatVarGroupNode string
	// Attach single stat var group.
	attachSVG string
	// Attach multiple stat var groups.
	attachSVGs string
	// Fetch all children of a stat var group.
	getSVGChildren string
	// Fetch filtered count of descendent stat vars for a given variable group.
	getFilteredChildSVGs string
	// Fetch filtered descendent stat vars for a given variable group.
	getFilteredChildSVs string
	// Fetch filtered count of descendent stat vars for a given topic.
	getFilteredTopic string
	// Filter descendent stat vars by import.
	filterDescendentStatVarsByImport string
	// Filter descendent stat vars by num_entities_existences.
	filterDescendentStatVarsByNumEntitiesExistence string
	// Extract embedding values for a retrieval query.
	getEmbeddingFromQuery string
	// Search nodes using vector search.
	vectorSearchNode string
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
			Observation@{FORCE_INDEX=_BASE_TABLE}`,
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
	getStatVarGroupNode: `		WITH ChildSVGs AS (
			SELECT DISTINCT
				subject_id AS child_svg, 
				object_id AS svg
			FROM Edge
			WHERE predicate = 'specializationOf'
			AND object_id %[1]s
			UNION ALL
			%[2]s
		),
		UniqueChildSVGs AS (
			SELECT DISTINCT child_svg FROM ChildSVGs
		),
		ChildSVGCounts AS (
			SELECT 
				e.object_id AS child_svg, 
				COUNT(e.subject_id) AS descendent_stat_var_count
			FROM UniqueChildSVGs u
			JOIN@{JOIN_METHOD=APPLY_JOIN} Edge e 
			ON e.object_id = u.child_svg
			WHERE e.predicate = 'linkedMemberOf' 
			GROUP BY e.object_id
		),
		ChildSVs AS (
			SELECT DISTINCT
				subject_id AS child_sv, 
				object_id AS svg
			FROM Edge
			WHERE predicate = 'memberOf'
			AND object_id %[1]s
		),
		UniqueChildSVs AS (
			SELECT DISTINCT child_sv FROM ChildSVs
		)
		SELECT 
			svg.svg,
			n.subject_id, 
			n.name, 
			c.descendent_stat_var_count,
			FALSE AS has_data,
			'' AS definition
		FROM ChildSVGs svg
		JOIN ChildSVGCounts c 
		ON svg.child_svg = c.child_svg
		JOIN Node n 
		ON n.subject_id = svg.child_svg
		UNION ALL
		SELECT 
			sv.svg,
			n.subject_id, 
			n.name, 
			-1 AS descendent_stat_var_count,
			EXISTS (
				SELECT 1 
				FROM Observation o 
				WHERE o.variable_measured = sv.child_sv
				LIMIT 1
			) AS has_data,
			IFNULL((
				SELECT n_def.value
				FROM Edge e_def
				JOIN Node n_def ON e_def.object_id = n_def.subject_id
				WHERE e_def.subject_id = sv.child_sv
				AND e_def.predicate = 'definition'
				LIMIT 1
			), '') AS definition
		FROM ChildSVs sv
		JOIN Node n 
		ON n.subject_id = sv.child_sv`,
	attachSVG: `SELECT 
				@nodes AS child_svg,
				@nodes AS svg`,
	attachSVGs: `SELECT
				node AS child_svg,
				node AS svg
				FROM UNNEST(@nodes) AS node`,
	getSVGChildren: `		SELECT DISTINCT
			n.subject_id,
			n.name,
			e.predicate,
			IFNULL((
				SELECT n_def.value
				FROM Edge e_def
				JOIN Node n_def ON e_def.object_id = n_def.subject_id
				WHERE e_def.subject_id = n.subject_id
				AND e_def.predicate = 'definition'
				LIMIT 1
			), '') AS definition
		FROM Node n
		JOIN (
			SELECT subject_id, predicate FROM Edge@{FORCE_INDEX=InEdge}
			WHERE predicate IN ('memberOf', 'specializationOf')
				AND object_id = @node
		) e ON n.subject_id = e.subject_id`,
	getFilteredChildSVs: `		SELECT
			n.subject_id,
			n.name,
			(
				SELECT n_def.value
				FROM Edge e_def
				JOIN Node n_def ON e_def.object_id = n_def.subject_id
				WHERE e_def.subject_id = n.subject_id
				AND e_def.predicate = 'definition'
				LIMIT 1
			) AS definition
		FROM Node n
		JOIN (
			SELECT
				e.subject_id AS subject_id
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
				SELECT variable_measured
				FROM Observation %[1]s
				GROUP BY variable_measured%[2]s
			) o ON o.variable_measured = e.subject_id
			WHERE e.subject_id IN (
				SELECT DISTINCT subject_id
				FROM Edge
				WHERE object_id = @node
					AND predicate = 'memberOf'
			)
			GROUP BY
				e.subject_id
		) e_existence 
			ON n.subject_id = e_existence.subject_id`,
	getFilteredChildSVGs: `		SELECT
			n.subject_id,
			n.name,
			e_counts.descendent_stat_var_count
		FROM Node n
		JOIN (
			SELECT
				e.object_id AS subject_id,
				COUNT(e.subject_id) AS descendent_stat_var_count
			FROM Edge e
			JOIN@{JOIN_TYPE=HASH_JOIN} (
				SELECT variable_measured
				FROM Observation %[1]s
				GROUP BY variable_measured%[2]s
			) o ON o.variable_measured = e.subject_id
			WHERE e.predicate = 'linkedMemberOf'
				AND e.object_id IN (
					SELECT DISTINCT subject_id
					FROM Edge
					WHERE object_id = @node
						AND predicate = 'specializationOf'
					UNION ALL
					SELECT @node AS subject_id
				)
			GROUP BY
				e.object_id
		) e_counts
			ON n.subject_id = e_counts.subject_id`,
	getFilteredTopic: `		SELECT
			COUNT(e.subject_id) AS descendent_stat_var_count
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
			SELECT variable_measured
			FROM Observation %[1]s
			GROUP BY variable_measured%[2]s
		) o ON o.variable_measured = e.subject_id
		WHERE e.predicate = 'linkedMember'
			AND e.object_id = @node
		GROUP BY
			e.object_id`,
	filterDescendentStatVarsByImport: `
				JOIN Edge@{FORCE_INDEX=InEdge} e1
				ON import_name = SUBSTR(e1.subject_id, 9)
				WHERE e1.predicate = @predicate
					AND e1.object_id = @import`,
	filterDescendentStatVarsByNumEntitiesExistence: `
				HAVING COUNT(DISTINCT %s) >= @numEntitiesExistence`,
	getEmbeddingFromQuery: `		SELECT embeddings.values
		FROM ML.PREDICT(MODEL @model_name, (SELECT @search_label AS content, @task_type AS task_type))`,
	vectorSearchNode: `		SELECT
			subject_id,
			embedding_content,
			1 - COSINE_DISTANCE(@embeddings, embeddings) AS cosine_similarity
		FROM
			NodeEmbeddings
		WHERE
			embeddings IS NOT NULL
			AND APPROX_COSINE_DISTANCE(@embeddings, embeddings, options => JSON @options) > @distance_threshold
		ORDER BY
			APPROX_COSINE_DISTANCE(@embeddings, embeddings, options => JSON @options)
		LIMIT @limit`,
}
