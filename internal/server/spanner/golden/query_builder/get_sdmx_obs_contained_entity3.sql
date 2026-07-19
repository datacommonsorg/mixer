		@{SCAN_METHOD=COLUMNAR, EXECUTION_METHOD=BATCH}
		WITH contained_places_0 AS (
			SELECT DISTINCT contained.subject_id AS place_id
			FROM Edge typed
			JOIN@{FORCE_JOIN_ORDER=TRUE} Edge contained ON contained.subject_id = typed.subject_id
			WHERE contained.predicate = 'linkedContainedInPlace'
				AND contained.object_id = 'northamerica'
				AND typed.predicate = 'typeOf'
				AND typed.object_id = 'TransportMode'
		),
		series AS (
			SELECT
				t.variable_measured,
				t.entity1,
				t.extra_entities_id,
				t.facet_id,
				t.provenance,
				t.facet,
				t.entities
			FROM contained_places_0 anchor
			JOIN@{JOIN_METHOD=APPLY_JOIN} TimeSeries@{FORCE_INDEX=TimeSeriesByEntity3} t
				ON t.entity3 = anchor.place_id
				AND t.variable_measured = 'var1'
			WHERE t.entity3 IS NOT NULL
				AND t.entity2 IS NOT NULL
		)
		SELECT
			t.variable_measured,
			t.entity1 AS observation_about,
			t.facet_id,
			ANY_VALUE(t.provenance) AS provenance,
			ARRAY_AGG(STRUCT(o.date AS date, o.value AS str_value)) AS dates_and_values,
			ANY_VALUE(t.facet) AS facets,
			ANY_VALUE(t.entities) AS entities
		FROM series t
		JOIN@{JOIN_METHOD=APPLY_JOIN} Observation o
		USING (variable_measured, entity1, extra_entities_id, facet_id)
		GROUP BY
			t.variable_measured,
			t.entity1,
			t.extra_entities_id,
			t.facet_id