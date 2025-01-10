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
	"context"
	"time"

	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/util"
)

// DateEntityCount returns number of entities (from given candidates) with data for
// each observation date.
func DateEntityCount(
	ctx context.Context,
	sqlClient *sqldb.SQLClient,
	variables []string,
	entities []string,
) (map[string]map[string]map[string]int, error) {
	defer util.TimeTrack(time.Now(), "SQL: DateEntityCount")
	rows, err := sqlClient.GetEntityCount(ctx, variables, entities)
	if err != nil {
		return nil, err
	}

	// Process the query result
	// Keyed by variable, date, provenance; Value is the count
	result := map[string]map[string]map[string]int{}
	for _, row := range rows {
		if _, ok := result[row.Variable]; !ok {
			result[row.Variable] = map[string]map[string]int{}
		}
		if _, ok := result[row.Variable][row.Date]; !ok {
			result[row.Variable][row.Date] = map[string]int{}
		}
		result[row.Variable][row.Date][row.Provenance] = row.Count
	}
	return result, nil
}
