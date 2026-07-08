		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'country/CAN')-[e:Edge
		WHERE
			e.predicate IN ('name','nameWithLanguage')]->(n:Node) WHERE (e.predicate != 'nameWithLanguage' OR ENDS_WITH(n.value, '@es'))
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			n.value,
			n.bytes,
			IFNULL(n.name, '') AS name,
			n.types
		ORDER BY
			subject_id,
			predicate,
			n.subject_id,
			provenance
		LIMIT 501