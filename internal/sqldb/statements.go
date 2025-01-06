// Copyright 2025 Google LLC
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

// Query statements used by the SQLClient.
package sqldb

// SQL statements executed by the SQLClient
var statements = struct {
	getObsByVariableAndEntity     string
	getObsByVariableEntityAndDate string
}{
	getObsByVariableAndEntity: `
		SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties 
		FROM observations
		WHERE 
			entity IN (:entities)
			AND variable IN (:variables)
			AND value != ''
		ORDER BY date ASC;
	`,
	getObsByVariableEntityAndDate: `
		SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties 
		FROM observations
		WHERE 
			entity IN (:entities)
			AND variable IN (:variables)
			AND value != ''
			AND date = :date
		ORDER BY date ASC;
	`,
}
