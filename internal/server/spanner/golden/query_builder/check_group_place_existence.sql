		SELECT DISTINCT e.object_id AS variable, o.observation_about AS entity
		FROM Edge@{FORCE_INDEX=InEdge} e
		JOIN@{JOIN_TYPE=APPLY_JOIN} Observation@{FORCE_INDEX=VariableMeasuredObservationAbout} o ON e.subject_id = o.variable_measured
		WHERE e.predicate = 'linkedMemberOf'
		  AND e.object_id IN ('dc/g/Demographics')
		  AND o.observation_about IN ('geoId/06')
		ORDER BY variable, entity