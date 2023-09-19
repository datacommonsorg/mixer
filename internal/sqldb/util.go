// Copyright 2023 Google LLC
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

import "database/sql"

func CreateTables(sqlClient *sql.DB) error {
	tripleStatement := `
	CREATE TABLE IF NOT EXISTS triples (
		subject_id TEXT,
		predicate TEXT,
		object_id TEXT,
		object_value TEXT
	);
	`
	_, err := sqlClient.Exec(tripleStatement)
	if err != nil {
		return err
	}

	observationStatement := `
	CREATE TABLE IF NOT EXISTS observations (
		entity TEXT,
		variable TEXT,
		date TEXT,
		value TEXT,
		provenance TEXT
	);
	`
	_, err = sqlClient.Exec(observationStatement)
	if err != nil {
		return err
	}
	return nil
}

func ClearTables(sqlClient *sql.DB) error {
	_, err := sqlClient.Exec(
		`
			DELETE FROM observations;
		`,
	)
	if err != nil {
		return err
	}
	_, err = sqlClient.Exec(
		`
			DELETE FROM triples;
		`,
	)
	if err != nil {
		return err
	}
	return nil
}
