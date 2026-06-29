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
			t.facet_id,
			t.provenance,
			COALESCE(
				(
					SELECT ARRAY(
						SELECT AS STRUCT date, value AS str_value
						FROM Observation o
						WHERE o.variable_measured = t.variable_measured
							AND o.entity1 = t.entity1
							AND o.extra_entities_id = t.extra_entities_id
							AND o.facet_id = t.facet_id
						ORDER BY date DESC
						LIMIT 1
					)
				),
				ARRAY(SELECT AS STRUCT CAST(NULL AS STRING) AS date, CAST(NULL AS STRING) AS str_value FROM UNNEST([1]) WHERE FALSE)
			) AS dates_and_values,
			t.facet AS facets
		FROM places p
		JOIN@{JOIN_METHOD=APPLY_JOIN, FORCE_JOIN_ORDER=TRUE} TimeSeries t
			ON t.variable_measured IN ('AirPollutant_Cancer_Risk')
			AND t.entity1 = p.place_id