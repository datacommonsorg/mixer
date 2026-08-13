		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH ANY (m:Node
		WHERE
			m.subject_id = 'dc/g/UN')<-[e:Edge
		WHERE
			e.predicate = 'specializationOf']-{1,10}(n:Node)
        RETURN DISTINCT
            m.subject_id,
            n.subject_id AS object_id
        ORDER BY
            subject_id,
            object_id
		LIMIT 501
        NEXT LET dest = (
			SELECT AS STRUCT
				TRUE AS resolved,
				n.value,
				n.bytes,
				n.name,
				n.types,
			FROM Node n
			WHERE n.subject_id = object_id
		)
        RETURN
            subject_id,
            'specializationOf+' AS predicate,
			object_id,
            '' AS provenance,
			IFNULL(dest.resolved, FALSE) AS resolved,
            IFNULL(dest.value, '') AS value,
            dest.bytes,
            IFNULL(dest.name, '') AS name,
            IFNULL(dest.types, []) AS types
        ORDER BY
            subject_id,
            object_id