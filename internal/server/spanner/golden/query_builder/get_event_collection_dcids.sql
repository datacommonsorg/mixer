		@{force_join_order=true}
		GRAPH DCGraph MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: 'country/LBR'}]->(), (event)-[:Edge {predicate: 'typeOf', object_id: 'FireEvent'}]->(), (event)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		OPTIONAL MATCH (event)-[:Edge {predicate: 'area'}]->(magNode:Node)
		WHERE 
			SUBSTR(dateNode.value, 1, 7) = '2020-10'
		RETURN DISTINCT 
			event.subject_id AS dcid,
			magNode.value AS magnitude