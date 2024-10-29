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

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/datacommonsorg/mixer/internal/util"
)

// SQL table and column names.
const (
	TableObservations       = "observations"
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

// allObservationsTableColumns is a set of all column names in the observations table.
var allObservationsTableColumns = map[string]struct{}{
	ColumnEntity:            {},
	ColumnVariable:          {},
	ColumnDate:              {},
	ColumnValue:             {},
	ColumnProvenance:        {},
	ColumnUnit:              {},
	ColumnScalingFactor:     {},
	ColumnMeasurementMethod: {},
	ColumnObservationPeriod: {},
	ColumnProperties:        {},
}

// CheckSchema checks if the schema of the SQL DB is what is expected by the service.
// It returns an error if it is not.
func CheckSchema(db *sql.DB) error {
	observationColumns, err := getTableColumns(db, TableObservations)
	if err != nil {
		return err
	}

	missingObservationColumns := util.GetMissingStrings(observationColumns, allObservationsTableColumns)
	if len(missingObservationColumns) != 0 {
		return fmt.Errorf(`The following columns are missing in the %s table: %v

Run a data management job to update your database schema.
Guide: https://docs.datacommons.org/custom_dc/troubleshooting.html#schema-check-failed.

`,
			TableObservations, missingObservationColumns)
	}

	log.Printf("SQL schema check succeeded.")
	return nil
}

// CreateTables creates tables in MySQL database if they are not present.
func CreateTables(sqlClient *sql.DB) error {
	tripleStatement := `
	CREATE TABLE IF NOT EXISTS triples (
		subject_id varchar(255),
		predicate varchar(255),
		object_id varchar(255),
		object_value TEXT
	);
	`
	_, err := sqlClient.Exec(tripleStatement)
	if err != nil {
		return err
	}

	observationStatement := `
	CREATE TABLE IF NOT EXISTS observations (
		entity varchar(255),
		variable varchar(255),
		date varchar(255),
		value varchar(255),
		provenance varchar(255),
		unit varchar(255),
		scaling_factor varchar(255),
		measurement_method varchar(255),
		observation_period varchar(255),
		properties TEXT
	);
	`
	_, err = sqlClient.Exec(observationStatement)
	if err != nil {
		return err
	}

	keyValueStoreStatement := `
	CREATE TABLE IF NOT EXISTS key_value_store (
		lookup_key varchar(255),
		value longtext
	);
	`
	_, err = sqlClient.Exec(keyValueStoreStatement)
	if err != nil {
		return err
	}
	return nil
}

func getTableColumns(db *sql.DB, tableName string) ([]string, error) {
	// LIMIT 0 to avoid fetching data
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)

	// Execute the query
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Get the column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting column names for %s table: %w", tableName, err)
	}

	return columns, nil
}
