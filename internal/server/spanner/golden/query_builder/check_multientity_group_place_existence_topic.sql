		WITH
		group_members AS (
			SELECT DISTINCT
				e.object_id AS variable_group,
				e.subject_id AS variable_measured
			FROM Edge@{FORCE_INDEX=InEdge} e
			WHERE e.predicate = 'linkedMember'
			  AND e.object_id = 'dc/t/Place/Population'
		),
		slot1 AS (
			SELECT DISTINCT
				gm.variable_group AS variable,
				ts.entity1 AS entity
			FROM group_members gm
			JOIN TimeSeries AS ts
				ON gm.variable_measured = ts.variable_measured
			WHERE ts.entity1 IN ('geoId/01001','geoId/02013')
		),
		slot2 AS (
			SELECT DISTINCT
				gm.variable_group AS variable,
				ts.entity2 AS entity
			FROM group_members gm
			JOIN TimeSeries@{FORCE_INDEX=TimeSeriesByEntity2} AS ts
				ON gm.variable_measured = ts.variable_measured
			WHERE ts.entity2 IN ('geoId/01001','geoId/02013')
			  AND ts.entity2 IS NOT NULL
		),
		slot3 AS (
			SELECT DISTINCT
				gm.variable_group AS variable,
				ts.entity3 AS entity
			FROM group_members gm
			JOIN TimeSeries@{FORCE_INDEX=TimeSeriesByEntity3} AS ts
				ON gm.variable_measured = ts.variable_measured
			WHERE ts.entity3 IN ('geoId/01001','geoId/02013')
			  AND ts.entity3 IS NOT NULL
			  AND ts.entity2 IS NOT NULL
		)
		SELECT DISTINCT variable, entity
		FROM (
			SELECT variable, entity FROM slot1
			UNION ALL
			SELECT variable, entity FROM slot2
			UNION ALL
			SELECT variable, entity FROM slot3
		) AS combined
		ORDER BY variable, entity