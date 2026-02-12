		GRAPH DCGraph MATCH ANY (a0:Node
		WHERE
			a0.subject_id IN ('geoId/06')),
		(a1:Node)
		WHERE
			a1 = a0
		RETURN
			a0.value AS a0,
			a1.value AS a1