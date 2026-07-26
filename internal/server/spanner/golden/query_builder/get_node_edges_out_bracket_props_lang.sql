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
        NEXT MATCH (n:Node)
        WHERE
          n.subject_id = object_id
        RETURN
            subject_id,
            predicate,
            provenance,
            IFNULL(n.value, '') AS value,
            n.bytes,
            IFNULL(n.name, '') AS name,
            IFNULL(n.types, []) AS types
        ORDER BY
            subject_id,
            predicate,
            n.subject_id,
            provenance