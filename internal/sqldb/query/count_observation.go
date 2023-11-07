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

package query

import (
	"database/sql"
	"fmt"

	"github.com/datacommonsorg/mixer/internal/util"
)

// CountObservation count observation for given entity and variable pair in SQL database.
func CountObservation(
	sqlClient *sql.DB,
	entities []string,
	variables []string,
) (map[string]map[string]int, error) {
	entityParam, err := util.SQLListParam(sqlClient, len(entities))
	if err != nil {
		return nil, err
	}
	variableParam, err := util.SQLListParam(sqlClient, len(variables))
	if err != nil {
		return nil, err
	}
	// Query the observation count for entity, variable pairs
	query := fmt.Sprintf(
		`
			WITH entity_list(entity) AS (
					%s
			),
			variable_list(variable) AS (
					%s
			),
			all_pairs AS (
					SELECT e.entity, v.variable
					FROM entity_list e
					CROSS JOIN variable_list v
			)
			SELECT a.entity, a.variable, COUNT(o.date)
			FROM all_pairs a
			LEFT JOIN observations o ON a.entity = o.entity AND a.variable = o.variable
			GROUP BY a.entity, a.variable;
		`,
		entityParam,
		variableParam,
	)
	args := entities
	args = append(args, variables...)
	// Execute query
	rows, err := sqlClient.Query(query, util.ConvertArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := map[string]map[string]int{}
	for rows.Next() {
		var e, v string
		var count int
		err = rows.Scan(&e, &v, &count)
		if err != nil {
			return nil, err
		}
		if _, ok := result[v]; !ok {
			result[v] = map[string]int{}
		}
		result[v][e] = count
	}
	return result, nil
}
