		SELECT
			a AS ancestor,
			ARRAY(
				SELECT child
				FROM LinkedEdge
				WHERE
					predicate = 'containedInPlace'
					AND ancestor = a
					AND child_type = 'County'
				ORDER BY FARM_FINGERPRINT(child)
				LIMIT 10
			) AS children
		FROM UNNEST(['country/USA','geoId/06']) AS a