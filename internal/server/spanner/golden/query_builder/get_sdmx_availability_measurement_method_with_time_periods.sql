		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		SELECT DISTINCT t.measurement_method AS value
		FROM TimeSeries t
		JOIN@{JOIN_METHOD=MERGE_JOIN} Observation o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		WHERE (t.variable_measured = 'Count_TimeSeries')
			AND o.date IN ('2020','2023')
			AND t.measurement_method IS NOT NULL
			AND t.measurement_method != ''
		ORDER BY value
