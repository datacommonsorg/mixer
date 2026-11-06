		SELECT DISTINCT
			ancestor,
			child_type
		FROM LinkedEdge
		WHERE
			predicate = 'containedInPlace'
			AND ancestor IN ('country/USA','geoId/06')