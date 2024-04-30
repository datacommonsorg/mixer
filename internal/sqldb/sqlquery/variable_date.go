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

package sqlquery

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/datacommonsorg/mixer/internal/util"
)

// DateEntityCount returns number of entities (from given candidates) with data for
// each observation date.
func DateEntityCount(
	sqlClient *sql.DB,
	variables []string,
	entities []string,
) (map[string]map[string]map[string]int, error) {
	defer util.TimeTrack(time.Now(), "SQL: DateEntityCount")
	// Construct the query
	query := fmt.Sprintf(
		`
			SELECT
				variable,
				date,
				provenance,
				COUNT(DISTINCT entity) AS num_entities
			FROM
				observations
			WHERE
				entity IN (%s)
				AND variable IN (%s)
			GROUP BY
					variable,
					date,
					provenance
			ORDER BY
					variable,
					date,
					provenance;
		`,
		util.SQLInParam(len(entities)),
		util.SQLInParam(len(variables)),
	)
	// Execute query
	args := entities
	args = append(args, variables...)
	rows, err := sqlClient.Query(query, util.ConvertArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Process the query result
	// Keyed by variable, date, provenance; Value is the count
	result := map[string]map[string]map[string]int{}
	for rows.Next() {
		var v, d, prov string
		var count int
		err = rows.Scan(&v, &d, &prov, &count)
		if err != nil {
			return nil, err
		}
		if _, ok := result[v]; !ok {
			result[v] = map[string]map[string]int{}
		}
		if _, ok := result[v][d]; !ok {
			result[v][d] = map[string]int{}
		}
		result[v][d][prov] = count
	}
	return result, nil
}
