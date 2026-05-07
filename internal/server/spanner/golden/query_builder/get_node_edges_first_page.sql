		GRAPH DCGraph MATCH (m:Node
		WHERE
			m.subject_id = 'StatisticalVariable')<-[e:Edge
		WHERE
			e.predicate = 'typeOf']-(n:Node)
		RETURN
			m.subject_id,
			e.predicate,
			e.provenance,
			n.value,
			n.bytes,
			IFNULL(n.name, "") AS name,
			n.types
		ORDER BY
			subject_id,
			predicate,
			n.subject_id,
			provenance
		LIMIT 501