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

// Queries executed by the SQLClient.
package sqldb

import (
	"github.com/jmoiron/sqlx"
)

const (
	latestDate = "LATEST"
)

// GetObservations retrieves observations from SQL given a list of variables and entities and a date.
func (sc *SQLClient) GetObservations(variables []string, entities []string, date string) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || len(entities) == 0 {
		return observations, nil
	}

	var stmt statement

	switch {
	case date != "" && date != latestDate:
		stmt = statement{
			query: statements.getObsByVariableEntityAndDate,
			args: map[string]interface{}{
				"variables": variables,
				"entities":  entities,
				"date":      date,
			},
		}
	default:
		stmt = statement{
			query: statements.getObsByVariableAndEntity,
			args: map[string]interface{}{
				"variables": variables,
				"entities":  entities,
			},
		}
	}

	err := sc.queryAndCollect(
		stmt,
		&observations,
	)
	if err != nil {
		return nil, err
	}

	return observations, nil
}

func (sc *SQLClient) queryAndCollect(
	stmt statement,
	dest interface{},
) error {
	// Convert named query and maps of args to placeholder query and list of args.
	query, args, err := sqlx.Named(stmt.query, stmt.args)
	if err != nil {
		return err
	}

	// Expand slice values.
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}

	// Transform query to the driver's placeholder type.
	query = sc.dbx.Rebind(query)

	return sc.dbx.Select(dest, query, args...)
}

// statement struct includes the sql query and named args used to execute a sql query.
type statement struct {
	query string
	args  map[string]interface{}
}
