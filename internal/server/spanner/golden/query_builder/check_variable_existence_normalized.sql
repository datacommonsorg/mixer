		SELECT DISTINCT
			ts.variable_measured,
			a.value AS entity
		FROM 
			TimeSeries@{FORCE_INDEX=TimeSeriesByVariableMeasured} ts
		JOIN
			TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributeValue} a ON ts.id = a.id
		WHERE ts.variable_measured = 'AirPollutant_Cancer_Risk' AND a.value IN ('geoId/01001','geoId/02013')