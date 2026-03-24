		@{force_join_order=true}
		GRAPH DCGraph MATCH (event:Node)-[:Edge {predicate: 'affectedPlace', object_id: 'geoId/37'}]->(), (event)-[:Edge {predicate: 'typeOf', object_id: 'HeatTemperatureEvent'}]->(), (event)-[:Edge {predicate: 'startDate'}]->(dateNode:Node)
		WHERE 
			SUBSTR(dateNode.value, 1, 7) = '2022-01'
		MATCH (event)-[magEdge:Edge {predicate: 'differenceTemperature'}]->()
		RETURN DISTINCT 
			event.subject_id AS dcid,
			magEdge.object_id AS magnitude