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
	"github.com/datacommonsorg/mixer/internal/util"
)

// EntityVariableExistence returns all existent entity, variable pairs
func EntityVariableExistence(ctx context.Context, sqlClient *sqldb.SQLClient) (map[util.EntityVariable]struct{}, error) {
	rows, err := sqlClient.GetAllEntitiesAndVariables(ctx)
	if err != nil {
		return nil, err
	}
	// Process the query result
	result := map[util.EntityVariable]struct{}{}
	for _, row := range rows {
		var e, v = row.Entity, row.Variable
		result[util.EntityVariable{E: e, V: v}] = struct{}{}
		// Also track which variables (across all entities) have data.
		result[util.EntityVariable{V: v}] = struct{}{}
	}
	return result, nil
}
