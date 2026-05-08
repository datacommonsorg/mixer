		SELECT subject_id
		FROM Node
		WHERE 'Topic' IN UNNEST(types)
			AND subject_id IN ('dc/g/Demographics','dc/topic/Demographics','WHO/Root')