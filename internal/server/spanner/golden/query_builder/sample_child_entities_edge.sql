		SELECT
			a AS ancestor,
			ARRAY(
				SELECT contained.subject_id
				FROM Edge@{FORCE_INDEX=InEdge} contained
				JOIN@{FORCE_JOIN_ORDER=TRUE} Edge typed ON typed.subject_id = contained.subject_id
				WHERE
					contained.predicate = 'linkedContainedInPlace'
					AND contained.object_id = a
					AND typed.predicate = 'typeOf'
					AND typed.object_id = 'County'
				ORDER BY FARM_FINGERPRINT(contained.subject_id)
				LIMIT 10
			) AS children
		FROM UNNEST(['country/USA','geoId/06']) AS a