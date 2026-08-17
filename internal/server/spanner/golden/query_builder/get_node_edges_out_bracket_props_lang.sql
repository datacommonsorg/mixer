		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'country/CAN')-[e:Edge
		WHERE
			e.predicate IN ('name','nameWithLanguage')]->(n:Node) WHERE (e.predicate != 'nameWithLanguage' OR ENDS_WITH(n.value, '@es'))  
        RETURN
            m.subject_id,
            e.predicate,
            n.subject_id as object_id,
            e.provenance
        ORDER BY
            subject_id, 
            predicate,
            object_id,
            provenance
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
            predicate,
			object_id,
            provenance,
			IFNULL(dest.resolved, FALSE) AS resolved,
            IFNULL(dest.value, '') AS value,
            dest.bytes,
            IFNULL(dest.name, '') AS name,
            IFNULL(dest.types, []) AS types
        ORDER BY
            subject_id,
            predicate,
            object_id,
            provenance