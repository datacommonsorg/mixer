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

	"github.com/go-test/deep"
)

func TestQuery(t *testing.T) {
	sqlClient, err := sql.Open("sqlite", "../../../test/datacommons.db")
	if err != nil {
		t.Fatalf("Could not open testing database: %s", err)
	}

	for _, c := range []struct {
		entities  []string
		variables []string
		want      map[string]map[string]int
	}{
		{
			[]string{"geoId/06"},
			[]string{"test_var_1"},
			map[string]map[string]int{
				"test_var_1": {"geoId/06": 11},
			},
		},
		{
			[]string{"geoId/06", "geoId/05", "geoId/22"},
			[]string{"test_var_1", "test_var_2"},
			map[string]map[string]int{
				"test_var_1": {
					"geoId/05": 8,
					"geoId/06": 11,
					"geoId/22": 1,
				},
				"test_var_2": {
					"geoId/05": 2,
					"geoId/06": 2,
					"geoId/22": 0,
				},
			},
		},
	} {
		expect, err := CountObservation(sqlClient, c.entities, c.variables)
		if err != nil {
			t.Fatalf("Error execute CountObservation(): %s", err)
		}
		if diff := deep.Equal(c.want, expect); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}
