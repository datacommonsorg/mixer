		SELECT
			ts.variable_measured,
			ARRAY(
				SELECT STRUCT(date, value)
				FROM StatVarObservation
				WHERE id = ts.id
				ORDER BY date ASC
			) as dates_and_values,
			ARRAY(
				SELECT STRUCT(property, value)
				FROM TimeSeriesAttribute
				WHERE id = ts.id
			) as attributes
		FROM 
			TimeSeries@{FORCE_INDEX=TimeSeriesByVariableMeasured} ts
		WHERE ts.variable_measured = 'AirPollutant_Cancer_Risk' AND ts.id IN (SELECT id FROM TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributePropertyValue} WHERE property = 'observationAbout' AND value IN ('geoId/01001','geoId/02013'))