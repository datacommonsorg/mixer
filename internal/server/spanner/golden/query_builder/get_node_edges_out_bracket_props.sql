		@{SCAN_METHOD=COLUMNAR}
		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'geoId/5129600')-[e:Edge
		WHERE
			e.predicate IN ('containedInPlace','geoJsonCoordinatesDP3')]->(n:Node)  
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
            IFNULL(dest.value, '') AS value,
            dest.bytes,
            IFNULL(dest.name, '') AS name,
            IFNULL(dest.types, []) AS types
        ORDER BY
            subject_id,
            predicate,
            object_id,
            provenance