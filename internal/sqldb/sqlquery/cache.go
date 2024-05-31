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
	"log"

	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	SVGCacheKey = "c/cache/svgs"
)

// GetCacheData gets cache data for the specified key from the sql DB.
// If not found, returns false.
// If found, unmarshals the value into the specified proto and returns true.
func GetCacheData(sqlClient *sql.DB, key string, out protoreflect.ProtoMessage) (bool, error) {
	query :=
		`
					SELECT object_value
					FROM triples
					WHERE subject_id = $1 and predicate = "value" and object_value <> "";
				`

	stmt, err := sqlClient.Prepare(query)
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	var value string
	err = stmt.QueryRow(key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("No cache data found: %s", key)
			return false, nil
		}
		return false, err
	}

	bytes, err := util.UnzipAndDecode(value)
	if err != nil {
		return false, err
	}

	err = proto.Unmarshal(bytes, out)
	if err != nil {
		return false, err
	}

	return true, nil
}
