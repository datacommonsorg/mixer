		GRAPH DCGraph MATCH <-[i:Edge
		WHERE
			i.object_id IN ('country/USA','country/USA:cb700a8CE4DPjAH7ZvHnLqh3pBgC06H8ve/V3dYC5NU=','undata-geo:G00003340','undata-geo:G0000:1097KFH3cEHxUmrcl0S9d1Ej9JGRpZ2HMBwUhEuAQOc=','Count_Person','Count_Person:3afS2X/HYWHB+Ank1y64XWV6AMy5rYdkE1zO29uHQ2Q=','foo','foo:LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=')
			AND i.predicate = 'unDataCode']-()-[o:Edge
		WHERE
			o.predicate = 'wikidataId']->(n:Node)
		RETURN
			i.object_id AS node,
			n.value AS candidate