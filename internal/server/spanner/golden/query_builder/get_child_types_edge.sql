		SELECT DISTINCT
			contained.object_id AS ancestor,
			typed.object_id AS child_type
		FROM Edge@{FORCE_INDEX=InEdge} contained
		JOIN@{FORCE_JOIN_ORDER=TRUE} Edge typed ON typed.subject_id = contained.subject_id
		WHERE
			contained.predicate = 'linkedContainedInPlace'
			AND contained.object_id IN ('country/USA','geoId/06')
			AND typed.predicate = 'typeOf'