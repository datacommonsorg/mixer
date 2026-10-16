		@{SCAN_METHOD=COLUMNAR}
		SELECT
			e.subject_id,
			'containedInPlace+' as predicate,
			e.object_id,
			'' AS provenance,
			IF (dest.subject_id IS NULL, FALSE, TRUE) AS resolved,
            IFNULL(dest.value, '') AS value,
            dest.bytes,
            IFNULL(dest.name, '') AS name,
            IFNULL(dest.types, []) AS types
		FROM (
			SELECT DISTINCT 
				ancestor AS subject_id, 
				child AS object_id 
			FROM LinkedEdge
			WHERE ancestor = 'country/USA'
				AND predicate = 'containedInPlace'
				AND child_type = 'County'
			ORDER BY
				subject_id,
				object_id
		LIMIT 501
		)e
		LEFT JOIN Node dest ON e.object_id = dest.subject_id
		ORDER BY
            subject_id,
            object_id
	