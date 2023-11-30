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
	"time"

	"github.com/datacommonsorg/mixer/internal/util"
)

// EntityVariableExistence returns all existent entity, variable pairs
func EntityVariableExistence(sqlClient *sql.DB) (map[util.EntityVariable]struct{}, error) {
	defer util.TimeTrack(time.Now(), "SQL: EntityVariable")
	query := "SELECT DISTINCT entity, variable FROM observations o"
	// Execute query
	rows, err := sqlClient.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Process the query result
	result := map[util.EntityVariable]struct{}{}
	for rows.Next() {
		var e, v string
		err = rows.Scan(&e, &v)
		if err != nil {
			return nil, err
		}
		result[util.EntityVariable{E: e, V: v}] = struct{}{}
		// Also track which variables (across all entities) have data.
		result[util.EntityVariable{V: v}] = struct{}{}
	}
	return result, nil
}
