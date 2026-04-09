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
		WHERE ts.variable_measured = 'AirPollutant_Cancer_Risk' AND ts.id IN (SELECT id FROM TimeSeriesAttribute WHERE property = 'observationAbout' AND value IN ('geoId/01001','geoId/02013'))
		ORDER BY svo.date ASC