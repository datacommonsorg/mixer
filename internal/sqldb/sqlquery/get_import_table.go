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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/sqldb"
)

// Represents "metadata" column of import table
type ImportMetadata struct {
	NumObs  *int32
	NumVars *int32
}

// GetImportTableData gets rows of imports table
func GetImportTableData(ctx context.Context, sqlClient *sqldb.SQLClient) (*pb.GetImportTableDataResponse, error) {
	if !sqldb.IsConnected(sqlClient) {
		return &pb.GetImportTableDataResponse{}, nil
	}
	rows, err := sqlClient.GetAllImports(ctx)
	if err != nil {
		return nil, err
	}
	// Process the query result
	var allRows []*pb.GetImportTableDataResponse_ImportData
	for _, row := range rows {

		rowData := &pb.GetImportTableDataResponse_ImportData{
			ImportedAt: row.ImportedAt,
			Status:     row.Status,
			Metadata: &pb.GetImportTableDataResponse_ImportData_ImportMetadata{
				NumObs:  row.Metadata.NumObs,
				NumVars: row.Metadata.NumVars,
			},
		}
		allRows = append(allRows, rowData)
	}
	result := &pb.GetImportTableDataResponse{
		Data: allRows,
	}
	return result, nil
}
