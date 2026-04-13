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
		WHERE ts.variable_measured IN ('Count_Person','Count_Household') AND ts.id IN (SELECT id FROM TimeSeriesAttribute WHERE property = 'observationAbout' AND value IN ('geoId/06','geoId/08'))
		ORDER BY svo.date ASC