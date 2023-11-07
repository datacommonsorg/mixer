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
	"fmt"

	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
)

// CheckVariableGroups check and returns variable groups that in SQL database.
func CheckVariableGroups(st *store.Store, variableGroups []string) ([]string, error) {
	result := []string{}
	// Find all the sv that are in the sqlite database
	query := fmt.Sprintf(
		`
				SELECT DISTINCT(subject_id) FROM triples
				WHERE predicate = "typeOf"
				AND subject_id IN (%s)
				AND object_id = 'StatVarGroup';
			`,
		util.SQLInParam(len(variableGroups)),
	)
	// Execute query
	rows, err := st.SQLClient.Query(
		query,
		util.ConvertArgs(variableGroups)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Process the query result
	for rows.Next() {
		var svg string
		err = rows.Scan(&svg)
		if err != nil {
			return nil, err
		}
		result = append(result, svg)
	}
	return result, nil
}
