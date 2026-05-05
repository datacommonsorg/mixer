		@{force_join_order=true}
		GRAPH DCGraph
		MATCH (event:Node)-[:Edge {predicate: 'typeOf', object_id: 'FloodEvent'}]->()
		WITH DISTINCT event
		MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: 'country/GBR'}]->()
		MATCH (event:Node)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		RETURN DISTINCT 
			SUBSTR(dateNode.value, 1, 7) AS month
		ORDER BY 
			month