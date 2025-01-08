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
	getObsByVariableAndEntity     string
	getObsByVariableEntityAndDate string
	getStatVarSummaries           string
	getKeyValue                   string
	getAllStatVarGroups           string
	getAllStatVars                string
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
}
