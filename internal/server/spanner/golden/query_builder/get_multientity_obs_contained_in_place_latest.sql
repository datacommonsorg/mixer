		WITH places AS (
			SELECT result.object_id AS place_id
			FROM GRAPH_TABLE ( 
				DCGraph MATCH <-[e:Edge WHERE e.object_id = 'geoId/10' AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: 'County'}]-> 
				RETURN e.subject_id AS object_id 
			) result
		)
		SELECT 
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facets_id AS facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, str_value, attributes
						FROM Observation_final_v2 o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facets_id = t.facets_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value, CAST(NULL AS JSON) AS attributes FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} TimeSeries_final_v2 t
			ON t.variable_measured IN ('AirPollutant_Cancer_Risk')
			AND t.entity1 = p.place_id