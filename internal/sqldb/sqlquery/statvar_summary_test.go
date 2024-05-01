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
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/go-test/deep"
)

func TestStatVarSummary(t *testing.T) {
	sqlClient, err := sql.Open("sqlite3", "../../../test/sqlquery/statvar_summary/datacommons.db")
	if err != nil {
		t.Fatalf("Could not open testing database: %v", err)
	}

	want := map[string]*pb.StatVarSummary{
		"var1": {
			PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
				"Country": {
					PlaceCount: 2,
					MinValue:   fptr(5),
					MaxValue:   fptr(7),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "country/USA", Name: "country/USA"},
						1: {Dcid: "country/CHN", Name: "country/CHN"},
					},
				},
				"State": {
					PlaceCount: 2,
					MinValue:   fptr(1),
					MaxValue:   fptr(4),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "geoId/01", Name: "geoId/01"},
						1: {Dcid: "geoId/02", Name: "geoId/02"},
					},
				},
			},
		},
		"var2": {
			PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
				"Country": {
					PlaceCount: 3,
					MinValue:   fptr(15),
					MaxValue:   fptr(17),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "country/USA", Name: "country/USA"},
						1: {Dcid: "country/CHN", Name: "country/CHN"},
						2: {Dcid: "country/JPN", Name: "country/JPN"},
					},
				},
				"State": {
					PlaceCount: 2,
					MinValue:   fptr(11),
					MaxValue:   fptr(13),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "geoId/01", Name: "geoId/01"},
						1: {Dcid: "geoId/03", Name: "geoId/03"},
					},
				},
			},
		},
	}

	got, err := GetStatVarSummary(sqlClient, []string{"var1", "var2"})
	if err != nil {
		t.Fatalf("Error getting stat var summaries: %v", err)
	}

	if diff := deep.Equal(got, want); diff != nil {
		t.Errorf("Unexpected diff: %v", diff)
	}
}

func fptr(x float64) *float64 {
	return &x
}
