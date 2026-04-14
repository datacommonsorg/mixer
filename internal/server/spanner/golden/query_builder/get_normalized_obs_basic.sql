		SELECT
			ts.variable_measured,
			ts.id,
			ts.provenance,
			svo.date,
			svo.value
		FROM 
			TimeSeries ts
		JOIN 
			StatVarObservation svo ON ts.id = svo.id
		WHERE ts.variable_measured = 'Count_Person' AND ts.id IN (SELECT id FROM TimeSeriesAttribute WHERE property = 'observationAbout' AND value = 'geoId/06')
		ORDER BY svo.date ASC