		SELECT
			obs.variable_measured,
			obs.observation_about,
			obs.observations,
			obs.import_name,
			obs.observation_period,
			obs.measurement_method,
			obs.unit,
			obs.scaling_factor,
			obs.provenance_url,
			obs.facet_id
		FROM 
			GRAPH_TABLE (
				DCGraph MATCH <-[e:Edge
				WHERE
					e.object_id = 'geoId/10'
					AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: 'County'}]->
				RETURN
					e.subject_id as object_id
			)result
		INNER JOIN (		SELECT
			variable_measured,
			observation_about,
			observations,
			import_name,
			observation_period,
			measurement_method,
			unit,
			scaling_factor,
			provenance_url,
			facet_id
		FROM 
			Observation
		WHERE
			variable_measured IN ('Count_Person','Median_Age_Person'))obs
		ON 
			result.object_id = obs.observation_about