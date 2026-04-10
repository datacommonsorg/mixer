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
			GRAPH_TABLE (
				DCGraph MATCH <-[e:Edge
				WHERE
					e.object_id = 'geoId/10'
					AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: 'County'}]->
				RETURN
					e.subject_id as object_id
			) result
		JOIN
			TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributeValue} tsa
		ON
			tsa.value = result.object_id
		JOIN
			TimeSeries@{FORCE_INDEX=TimeSeriesByVariableMeasured} ts
		ON
			ts.id = tsa.id
		WHERE ts.variable_measured = 'AirPollutant_Cancer_Risk'