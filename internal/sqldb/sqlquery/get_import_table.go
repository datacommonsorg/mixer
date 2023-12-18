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
	"encoding/json"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
)

// Represents "metadata" column of import table
type ImportMetadata struct {
	NumObs  int32
	NumVars int32
}

// GetImportTableData gets rows of import table
func GetImportTableData(sqlClient *sql.DB) (*pb.GetImportTableDataResponse, error) {
	defer util.TimeTrack(time.Now(), "SQL: GetImportTableData")
	query :=
		`
			SELECT imported_at, status, metadata
			FROM imports
			ORDER BY imported_at
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
	var allRows []*pb.GetImportTableDataResponse_ImportData
	for rows.Next() {
		var importedAt, status, rawMetadata string
		err = rows.Scan(&importedAt, &status, &rawMetadata)
		if err != nil {
			return nil, err
		}

		// Convert metadata from text to struct
		var metadata ImportMetadata
		err = json.Unmarshal([]byte(rawMetadata), &metadata)
		if err != nil {
			return nil, err
		}

		rowData := &pb.GetImportTableDataResponse_ImportData{
			ImportedAt: importedAt,
			Status:     status,
			Metadata: &pb.GetImportTableDataResponse_ImportData_ImportMetadata{
				NumObs:  metadata.NumObs,
				NumVars: metadata.NumVars,
			},
		}
		allRows = append(allRows, rowData)
	}
	result := &pb.GetImportTableDataResponse{
		Data: allRows,
	}
	return result, nil
}
