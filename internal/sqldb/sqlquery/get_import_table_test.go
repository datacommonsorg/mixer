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
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/go-test/deep"
)

func TestGetImportTableData(t *testing.T) {
	sqlClient, err := sql.Open("sqlite", "../../../test/test_get_import_table_data.db")
	if err != nil {
		t.Fatalf("Could not open testing database: %s", err)
	}

	var numObs, numVars int32 = 58, 4
	for _, c := range []struct {
		want *pb.GetImportTableDataResponse
	}{
		{
			&pb.GetImportTableDataResponse{
				Data: []*pb.GetImportTableDataResponse_ImportData{
					0: {
						ImportedAt: "2023-12-12T14:06:18.036077Z",
						Status:     "SUCCESS",
						Metadata: &pb.GetImportTableDataResponse_ImportData_ImportMetadata{
							NumObs:  &numObs,
							NumVars: &numVars,
						},
					},
				},
			},
		},
	} {
		expect, err := GetImportTableData(sqlClient)
		if err != nil {
			t.Fatalf("Error execute CountObservation(): %s", err)
		}
		if diff := deep.Equal(c.want, expect); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}
