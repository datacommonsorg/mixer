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

// Query statements used by the SQLClient.
package sqldb

// SQL statements executed by the SQLClient
var statements = struct {
	getObsByVariableAndEntity                 string
	getObsByVariableEntityAndDate             string
	getObsByVariableAndEntityType             string
	getObsByVariableEntityTypeAndDate         string
	getStatVarSummaries                       string
	getKeyValue                               string
	getAllStatVarGroups                       string
	getAllStatVars                            string
	getEntityCountByVariableDateAndProvenance string
	getSubjectPredicates                      string
	getObjectPredicates                       string
	getExistingStatVarGroups                  string
	getAllEntitiesOfType                      string
	getContainedInPlace                       string
	getEntityVariables                        string
	getAllEntitiesAndVariables                string
	getTableColumns                           string
	getObsCountByVariableAndEntity            string
	getEntityInfoTriples                      string
	getSubjectTriples                         string
	getObjectTriples                          string
	getAllProvenances                         string
	getAllImports                             string
}{
	getObsByVariableAndEntity: `
		SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties 
		FROM observations
		WHERE 
			entity IN (:entities)
			AND variable IN (:variables)
			AND value != ''
		ORDER BY date ASC;
	`,
	getObsByVariableEntityAndDate: `
		SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties 
		FROM observations
		WHERE 
			entity IN (:entities)
			AND variable IN (:variables)
			AND value != ''
			AND date = :date
		ORDER BY date ASC;
	`,
	getObsByVariableAndEntityType: `
		SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties 
		FROM observations AS o
		JOIN (
			SELECT DISTINCT subject_id 
			FROM triples 
			WHERE predicate = 'typeOf' AND object_id = :entityType
		) AS t ON o.entity = t.subject_id
		WHERE o.value != ''
		AND o.variable IN (:variables)
		ORDER BY date ASC;
	`,
	getObsByVariableEntityTypeAndDate: `
		SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties 
		FROM observations AS o
		JOIN (
			SELECT DISTINCT subject_id 
			FROM triples 
			WHERE predicate = 'typeOf' AND object_id = :entityType
		) AS t ON o.entity = t.subject_id
		WHERE o.value != ''
		AND o.variable IN (:variables)
		AND o.date = :date
		ORDER BY date ASC;
	`,
	getStatVarSummaries: `
		WITH entity_types
			AS (SELECT 
					o.variable               AS variable,
					t.object_id              AS entity_type,
					Count(DISTINCT o.entity) AS entity_count,
					Min(o.value + 0.0)       AS min_value,
					Max(o.value + 0.0)       AS max_value
				FROM   
					observations o
					JOIN triples t
					ON o.entity = t.subject_id
				WHERE  
					o.variable IN (:variables)
					AND t.predicate = 'typeOf'
				GROUP  BY variable, entity_type
				ORDER  BY entity_count DESC),
			entities
			AS (SELECT 
					DISTINCT o.variable   variable,
					t.object_id  entity_type,
					t.subject_id entity_id
				FROM   
					triples t
					JOIN observations o
					ON o.entity = t.subject_id
				WHERE  
					t.predicate = 'typeOf'
					AND t.object_id IN (SELECT entity_type FROM entity_types)
					AND o.variable IN (SELECT DISTINCT variable FROM entity_types)),
			sample_entities
			AS (SELECT variable, entity_type, entity_id
				FROM   (SELECT 
							*,
							Row_number() OVER (partition BY variable, entity_type) AS row_num
						FROM   entities) AS entities_with_row_num
				WHERE  row_num <= 3),
			grouped_entities
			AS (SELECT 
					variable,
					entity_type,
					Group_concat(entity_id) AS sample_entity_ids
				FROM   sample_entities
				GROUP  BY variable, entity_type),
			aggregate
			AS (SELECT 
					variable,
					entity_type,
					entity_count,
					min_value,
					max_value,
					sample_entity_ids
				FROM   
					entity_types
					JOIN grouped_entities using(variable, entity_type))
		SELECT *
		FROM   aggregate;
	`,
	getKeyValue: `
		SELECT value
		FROM key_value_store
		WHERE lookup_key = :key;
	`,
	getAllStatVarGroups: `
		SELECT t1.subject_id svg_id, t2.object_value svg_name, t3.object_id svg_parent_id
		FROM 
			triples t1 
			JOIN triples t2 ON t1.subject_id = t2.subject_id
			JOIN triples t3 ON t1.subject_id = t3.subject_id
		WHERE 
			t1.predicate="typeOf"
			AND t1.object_id="StatVarGroup"
			AND t2.predicate="name"
			AND t3.predicate="specializationOf";
	`,
	getAllStatVars: `
		SELECT t1.subject_id sv_id, t2.object_value sv_name, t3.object_id AS svg_id, COALESCE(t4.object_value, '') sv_description
		FROM 
			triples t1
			JOIN triples t2 ON t1.subject_id = t2.subject_id
			JOIN triples t3 ON t1.subject_id = t3.subject_id
			LEFT JOIN triples t4 ON t1.subject_id = t4.subject_id AND t4.predicate = "description"
		WHERE 
			t1.predicate="typeOf"
			AND t1.object_id="StatisticalVariable"
			AND t2.predicate="name"
			AND t3.predicate="memberOf";
	`,
	getEntityCountByVariableDateAndProvenance: `
		SELECT
			variable,
			date,
			provenance,
			COUNT(DISTINCT entity) num_entities
		FROM
			observations
		WHERE
			entity IN (:entities)
			AND variable IN (:variables)
		GROUP BY
				variable,
				date,
				provenance
		ORDER BY
				variable,
				date,
				provenance;
	`,
	getSubjectPredicates: `
		SELECT DISTINCT subject_id node, predicate FROM triples WHERE subject_id IN (:entities);
	`,
	getObjectPredicates: `
		SELECT DISTINCT object_id node, predicate FROM triples WHERE object_id IN (:entities);
	`,
	getExistingStatVarGroups: `
		SELECT DISTINCT(subject_id) FROM triples
		WHERE 
			predicate = "typeOf"
			AND subject_id IN (:groups)
			AND object_id = 'StatVarGroup';
	`,
	getAllEntitiesOfType: `
		SELECT subject_id, object_id
		FROM triples
		WHERE 
			predicate = 'typeOf'
			AND object_id = :type;
	`,
	getContainedInPlace: `
		SELECT t1.subject_id, t2.object_id
		FROM 
			triples t1
			JOIN triples t2
			ON t1.subject_id = t2.subject_id
		WHERE 
			t1.predicate = 'typeOf'
			AND t1.object_id = :childPlaceType
			AND t2.predicate = 'containedInPlace'
			AND t2.object_id IN (:parentPlaces);
	`,
	getEntityVariables: `
		SELECT entity, GROUP_CONCAT(DISTINCT variable) variables
		FROM observations 
		WHERE entity in (:entities)
		GROUP BY entity;
	`,
	getAllEntitiesAndVariables: `
		SELECT DISTINCT entity, variable
		FROM observations;
	`,
	// Query for column names in a table. Table name must be added via string interpolation.
	getTableColumns: `
		SELECT * FROM %s LIMIT 0;
	`,
	// Entity and variable CTEs must be added via string interpolation.
	getObsCountByVariableAndEntity: `
		WITH entity_list(entity) AS (%s),
		variable_list(variable) AS (%s),
		all_pairs AS (
			SELECT e.entity, v.variable
			FROM entity_list e
			CROSS JOIN variable_list v
		)
		SELECT a.entity, a.variable, COUNT(o.date) num_obs
		FROM all_pairs a
		LEFT JOIN observations o ON a.entity = o.entity AND a.variable = o.variable
		GROUP BY a.entity, a.variable;
	`,
	getEntityInfoTriples: `
		SELECT subject_id, predicate, COALESCE(object_id, '') object_id, COALESCE(object_value, '') object_value
		FROM triples
		WHERE subject_id IN (:entities) AND predicate IN ('name', 'typeOf');
	`,
	getSubjectTriples: `
		WITH node_list(node) AS (%s),
		prop_list(prop) AS (%s),
		all_pairs AS (
			SELECT n.node, p.prop
			FROM node_list n
			CROSS JOIN prop_list p
		)
		SELECT subject_id, predicate, COALESCE(object_id, '') object_id, COALESCE(object_value, '') object_value
		FROM all_pairs a
		INNER JOIN triples t ON a.node = t.subject_id AND a.prop = t.predicate
		GROUP BY a.node, a.prop, subject_id, predicate, object_id, object_value;
	`,
	getObjectTriples: `
		WITH node_list(node) AS (%s),
		prop_list(prop) AS (%s),
		all_pairs AS (
			SELECT n.node, p.prop
			FROM node_list n
			CROSS JOIN prop_list p
		)
		SELECT subject_id, predicate, COALESCE(object_id, '') object_id, COALESCE(object_value, '') object_value
		FROM all_pairs a
		INNER JOIN triples t ON a.node = t.object_id AND a.prop = t.predicate
		GROUP BY a.node, a.prop, subject_id, predicate, object_id, object_value;
	`,
	getAllProvenances: `
		SELECT t1.subject_id provenance_id, t2.object_value provenance_name, t3.object_value provenance_url
		FROM 
			triples AS t1
			JOIN triples AS t2 ON t1.subject_id = t2.subject_id
			JOIN triples AS t3 ON t1.subject_id = t3.subject_id
		WHERE 
			t1.predicate = "typeOf"
			AND t1.object_id = "Provenance"
			AND t2.predicate = "name"
			AND t3.predicate = "url"
	`,
	// Gets info about all imports into the DB sorted from newest to oldest.
	// Limits to the most recent 100 imports to keep the number of results bounded.
	getAllImports: `
		SELECT imported_at, status, metadata
		FROM imports
		ORDER BY imported_at DESC
		LIMIT 100;
	`,
}
