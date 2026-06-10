		WITH 
		slot1 AS (
			SELECT DISTINCT t.variable_measured, t.entity1 AS entity 
			FROM TimeSeries_final_v2 AS t 
			WHERE t.entity1 IN ('geoId/01001','geoId/02013')
		),
		slot2 AS (
			SELECT DISTINCT t.variable_measured, t.entity2 AS entity 
			FROM TimeSeries_final_v2@{FORCE_INDEX=TimeSeriesFinalV2ByEntity2} AS t 
			WHERE t.entity2 IN ('geoId/01001','geoId/02013') AND t.entity2 IS NOT NULL
		),
		slot3 AS (
			SELECT DISTINCT t.variable_measured, t.entity3 AS entity 
			FROM TimeSeries_final_v2@{FORCE_INDEX=TimeSeriesFinalV2ByEntity3} AS t 
			WHERE t.entity3 IN ('geoId/01001','geoId/02013') AND t.entity3 IS NOT NULL AND t.entity2 IS NOT NULL
		)
		SELECT variable_measured, entity FROM slot1
		UNION ALL
		SELECT variable_measured, entity FROM slot2
		UNION ALL
		SELECT variable_measured, entity FROM slot3