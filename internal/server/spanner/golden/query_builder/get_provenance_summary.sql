		SELECT
			key,
			provenance,
			TO_JSON_STRING(value) AS value
		FROM
			KeyValueStore
		WHERE
			type = 'ProvenanceSummary'
			AND key IN ('Count_Household_FamilyHousehold','Count_Household_HasComputer','foo')