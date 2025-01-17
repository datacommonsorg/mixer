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
	"context"

	"github.com/datacommonsorg/mixer/internal/sqldb"
)

// CountObservation count observation for given entity and variable pair in SQL database.
func CountObservation(
	ctx context.Context,
	sqlClient *sqldb.SQLClient,
	entities []string,
	variables []string,
) (map[string]map[string]int, error) {
	rows, err := sqlClient.GetObservationCount(ctx, variables, entities)
	if err != nil {
		return nil, err
	}

	result := map[string]map[string]int{}
	for _, row := range rows {
		e, v, count := row.Entity, row.Variable, row.Count
		if _, ok := result[v]; !ok {
			result[v] = map[string]int{}
		}
		result[v][e] = count
	}
	return result, nil
}
