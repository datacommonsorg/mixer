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
)

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
		provenance varchar(255)
	);
	`
	_, err = sqlClient.Exec(observationStatement)
	if err != nil {
		return err
	}

	keyValueStoreStatement := `
	CREATE TABLE IF NOT EXISTS key_value_store (
		lookup_key varchar(255),
		value TEXT
	);
	`
	_, err = sqlClient.Exec(keyValueStoreStatement)
	if err != nil {
		return err
	}
	return nil
}
