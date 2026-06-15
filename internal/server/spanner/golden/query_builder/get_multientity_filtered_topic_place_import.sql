		SELECT
			e.object_id AS subject_id,
			COUNT(e.subject_id) AS descendent_stat_var_count
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=HASH_JOIN} (
					SELECT ts.variable_measured
					FROM (
						SELECT t.variable_measured, t.entity1 AS entity, t.provenance
						FROM TimeSeries_final_v2 AS t
						WHERE t.entity1 IN ('country/CAN','country/IND')
						UNION ALL
						SELECT t.variable_measured, t.entity2 AS entity, t.provenance
						FROM TimeSeries_final_v2@{FORCE_INDEX=TimeSeriesFinalV2ByEntity2} AS t
						WHERE t.entity2 IN ('country/CAN','country/IND')
							AND t.entity2 IS NOT NULL
						UNION ALL
						SELECT t.variable_measured, t.entity3 AS entity, t.provenance
						FROM TimeSeries_final_v2@{FORCE_INDEX=TimeSeriesFinalV2ByEntity3} AS t
						WHERE t.entity3 IN ('country/CAN','country/IND')
							AND t.entity3 IS NOT NULL
							AND t.entity2 IS NOT NULL
					) AS ts
					JOIN Edge@{FORCE_INDEX=InEdge} e1
					ON ts.provenance = e1.subject_id
					WHERE e1.predicate = 'source'
					  AND e1.object_id = 'dc/s/WorldBank'
					GROUP BY ts.variable_measured
				) o ON o.variable_measured = e.subject_id
		WHERE e.predicate = 'linkedMember'
			AND e.object_id = 'dc/topic/Demographics'
		GROUP BY
			e.object_id