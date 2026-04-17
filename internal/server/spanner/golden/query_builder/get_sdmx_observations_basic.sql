		SELECT
			ts.variable_measured,
			ts.provenance,
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
			TimeSeries ts
		WHERE ts.variable_measured = 'Count_Person' AND ts.id IN (SELECT id FROM TimeSeriesAttribute WHERE property = 'observationAbout' AND value = 'country/USA')