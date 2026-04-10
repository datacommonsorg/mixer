// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Query statements used by the SpannerClient for the normalized schema.
package spanner

// SQL statements executed by the normalized SpannerClient
var statementsNormalized = struct {
	// Fetch Observations with coalesced dates, values and attributes.
	getObs string
	
	// Filter by variable dcids.
	selectVariableDcids string
	
	// Filter by entity dcids (by looking up in TimeSeriesAttribute).
	selectEntityDcids string

	// Fetch distinct variable and entity pairs.
	getStatVarsByEntity string

	// Fetch observations for entities contained in a place.
	getObsByContainedInPlace string
}{
	getObs: `		SELECT
			ts.variable_measured,
			ARRAY(
				SELECT STRUCT(date, value)
				FROM StatVarObservation
				WHERE id = ts.id
				ORDER BY date ASC
			) as dates_and_values,
			ARRAY(
				SELECT STRUCT(property, value)
				FROM TimeSeriesAttribute
				WHERE id = ts.id
			) as attributes
		FROM 
			TimeSeries@{FORCE_INDEX=TimeSeriesByVariableMeasured} ts`,
	selectVariableDcids: "ts.variable_measured %s",
	
	// Uses the index on TimeSeriesAttribute(property, value).
	// For now we assume property is 'observationAbout'.
	selectEntityDcids:   "ts.id IN (SELECT id FROM TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributePropertyValue} WHERE property = 'observationAbout' AND value %s)", 
	
	// We're intentionally not filtering by property since we'll be supporting multiple entities.
	// If we want to restrict queries only to entities (vs other attributes), the schema should
	// support a new property_type field and we can filter by property_type = "entity".
	getStatVarsByEntity: `		SELECT DISTINCT
			ts.variable_measured,
			a.value AS entity
		FROM 
			TimeSeries@{FORCE_INDEX=TimeSeriesByVariableMeasured} ts
		JOIN
			TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributePropertyValue} a ON ts.id = a.id`,

	getObsByContainedInPlace: `		SELECT
			ts.variable_measured,
			ARRAY(
				SELECT STRUCT(date, value)
				FROM StatVarObservation
				WHERE id = ts.id
				ORDER BY date ASC
			) as dates_and_values,
			ARRAY(
				SELECT STRUCT(property, value)
				FROM TimeSeriesAttribute
				WHERE id = ts.id
			) as attributes
		FROM 
			GRAPH_TABLE (
				DCGraph MATCH <-[e:Edge
				WHERE
					e.object_id = @ancestor
					AND e.predicate = 'linkedContainedInPlace']-()-[{predicate: 'typeOf', object_id: @childPlaceType}]->
				RETURN
					e.subject_id as object_id
			) result
		JOIN
			TimeSeriesAttribute@{FORCE_INDEX=TimeSeriesAttributeValue} tsa
		ON
			tsa.value = result.object_id
		JOIN
			TimeSeries@{FORCE_INDEX=TimeSeriesByVariableMeasured} ts
		ON
			ts.id = tsa.id`,
}

