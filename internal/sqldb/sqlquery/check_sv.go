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

package sqlquery

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/datacommonsorg/mixer/internal/util"
)

// CheckVariables check and returns variables that have data in SQL database.
func CheckVariables(sqlClient *sql.DB, variables []string) ([]string, error) {
	defer util.TimeTrack(time.Now(), "SQL: CheckVariables")
	result := []string{}
	query := fmt.Sprintf(
		`
			SELECT DISTINCT(variable) FROM observations o
			WHERE o.variable IN (%s)
		`,
		util.SQLInParam(len(variables)),
	)
	// Execute query
	rows, err := sqlClient.Query(
		query,
		util.ConvertArgs(variables)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Process the query result
	for rows.Next() {
		var sv string
		err = rows.Scan(&sv)
		if err != nil {
			return nil, err
		}
		result = append(result, sv)
	}
	return result, nil
}
