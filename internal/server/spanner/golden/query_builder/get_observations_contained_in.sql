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
					e.object_id = @ancestor
					AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]->
				RETURN
					e.subject_id as object_id
			)result
		INNER JOIN (		SELECT
			*
		FROM 
			Observation
		WHERE
			variable_measured IN UNNEST(@variables))obs
		ON 
			result.object_id = obs.observation_about