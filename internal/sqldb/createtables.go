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

	"github.com/go-sql-driver/mysql"
	"modernc.org/sqlite"
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
var allObservationsTableColumns = map[string]bool{
	ColumnEntity:            true,
	ColumnVariable:          true,
	ColumnDate:              true,
	ColumnValue:             true,
	ColumnProvenance:        true,
	ColumnUnit:              true,
	ColumnScalingFactor:     true,
	ColumnMeasurementMethod: true,
	ColumnObservationPeriod: true,
	ColumnProperties:        true,
}

// SQLDriver represents the type of SQL driver to use.
type SQLDriver int

// Enum values for SQLDriver.
const (
	SQLDriverUnknown SQLDriver = iota // SQLDriverUnknown = 0
	SQLDriverSQLite                   // SQLDriverSQLite = 1
	SQLDriverMySQL                    // SQLDriverMySQL = 2
)

func GetSQLDriver(sqlClient *sql.DB) SQLDriver {
	switch driver := sqlClient.Driver().(type) {
	case *sqlite.Driver:
		return SQLDriverSQLite
	case *mysql.MySQLDriver:
		return SQLDriverMySQL
	default:
		log.Printf("invalid sql driver: %v", driver)
		return SQLDriverUnknown
	}
}

// CheckSchema checks if the schema of the SQL DB is what is expected by the service.
// It returns an error if it is not.
func CheckSchema(db *sql.DB) error {
	observationColumns, err := getTableColumns(db, TableObservations)
	if err != nil {
		return err
	}

	_, err = allColumnsExistInSet(observationColumns, allObservationsTableColumns, TableObservations)
	if err != nil {
		return err
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
	switch GetSQLDriver(db) {
	case SQLDriverSQLite:
		return getSQLiteTableColumns(db, tableName)
	case SQLDriverMySQL:
		return getMySQLTableColumns(db, tableName)
	default:
		return nil, fmt.Errorf("cannot get columns of table: %s", tableName)
	}
}

// allColumnsExistInSet checks if all columns in a set exist in a given slice.
// It returns an error is they don't.
func allColumnsExistInSet(gotColumns []string, wantColumns map[string]bool, tableName string) (bool, error) {
	existingColumns := make(map[string]bool)
	for _, col := range gotColumns {
		existingColumns[col] = true
	}

	var missingColumns []string
	for col := range wantColumns {
		if _, ok := existingColumns[col]; !ok {
			missingColumns = append(missingColumns, col)
		}
	}

	if len(missingColumns) > 0 {
		// TODO: Add pointer to schema-update mode doc once it's there.
		return false, fmt.Errorf(`
The following columns are missing in the %s table: %v
Rerun the data docker to update the schema before starting the service`,
			tableName, missingColumns)

	}

	return true, nil
}

// getSQLiteTableColumns retrieves all column names from a SQLite table.
func getSQLiteTableColumns(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, fmt.Errorf("error querying table info: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		var ignore interface{} // Use an interface{} to ignore other columns
		if err := rows.Scan(&ignore, &colName, &ignore, &ignore, &ignore, &ignore); err != nil {
			return nil, fmt.Errorf("error scanning column name: %w", err)
		}
		columns = append(columns, colName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over columns: %w", err)
	}

	fmt.Printf("COLUMNS: %v", columns)
	return columns, nil
}

// getMySQLTableColumns retrieves all column names from a MySQL table.
func getMySQLTableColumns(db *sql.DB, tableName string) ([]string, error) {
	// Use "SHOW COLUMNS" to get column information in MySQL
	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("error querying table info: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		// You can ignore other columns returned by SHOW COLUMNS
		if err := rows.Scan(&colName, nil, nil, nil, nil, nil); err != nil {
			return nil, fmt.Errorf("error scanning column name: %w", err)
		}
		columns = append(columns, colName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over columns: %w", err)
	}

	return columns, nil
}
