		GRAPH DCGraph MATCH -[o:Edge
		WHERE 
			o.subject_id IN ('country/USA','undata-geo:G00003340','Count_Person','foo')
			AND o.predicate = 'unDataCode']->(n:Node)
		RETURN
			o.subject_id AS node,
			n.value AS candidate