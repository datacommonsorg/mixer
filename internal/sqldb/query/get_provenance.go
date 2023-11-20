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

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// GetProvenances returns all the provenance name and url in SQL database.
func GetProvenances(sqlClient *sql.DB) (map[string]*pb.Facet, error) {
	result := map[string]*pb.Facet{}
	query :=
		`
			SELECT t1.subject_id, t2.object_value, t3.object_value
			FROM triples AS t1
			JOIN triples AS t2 ON t1.subject_id = t2.subject_id
			JOIN triples AS t3 ON t1.subject_id = t3.subject_id
			WHERE t1.predicate = "typeOf"
			AND t1.object_id = "Provenance"
			AND t2.predicate = "name"
			AND t3.predicate = "url"
		`
	// Execute query
	rows, err := sqlClient.Query(
		query,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Process the query result
	for rows.Next() {
		var id, name, url string
		err = rows.Scan(&id, &name, &url)
		if err != nil {
			return nil, err
		}
		result[id] = &pb.Facet{
			ImportName:    name,
			ProvenanceUrl: url,
		}
	}
	return result, nil
}
