// Copyright 2024 Google LLC
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

package sqldb

// SQL table and column names.
const (
	TableObservations  = "observations"
	TableTriples       = "triples"
	TableKeyValueStore = "key_value_store"

	ColumnEntity            = "entity"
	ColumnVariable          = "variable"
	ColumnDate              = "date"
	ColumnValue             = "value"
	ColumnProvenance        = "provenance"
	ColumnUnit              = "unit"
	ColumnScalingFactor     = "scaling_factor"
	ColumnMeasurementMethod = "measurement_method"
	ColumnObservationPeriod = "observation_period"
	ColumnProperties        = "properties"
)

// allTables is an array of all expected tables in the SQL database.
var allTables = []string{TableObservations, TableTriples, TableKeyValueStore}

// allObservationsTableColumns is an array of all column names in the observations table.
var allObservationsTableColumns = []string{
	ColumnEntity,
	ColumnVariable,
	ColumnDate,
	ColumnValue,
	ColumnProvenance,
	ColumnUnit,
	ColumnScalingFactor,
	ColumnMeasurementMethod,
	ColumnObservationPeriod,
	ColumnProperties,
}
