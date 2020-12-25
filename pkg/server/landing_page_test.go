// Copyright 2020 Google LLC
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

package server

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestGetCohort(t *testing.T) {
	for _, c := range []struct {
		placeDcid string
		placeType string
		want      string
	}{
		{
			"country/USA",
			"Country",
			"PlacePagesComparisonCountriesCohort",
		},
		{
			"country/FRA",
			"Country",
			"PlacePagesComparisonCountriesCohort",
		},
		{
			"geoId/06",
			"State",
			"PlacePagesComparisonStateCohort",
		},
		{
			"geoId/06858",
			"State",
			"PlacePagesComparisonCountyCohort",
		},
		{
			"geoId/0685812",
			"City",
			"PlacePagesComparisonCityCohort",
		},
		{
			"nuts/FR101",
			"City",
			"PlacePagesComparisonWorldCitiesCohort",
		},
		{
			"nuts/FR101",
			"City",
			"PlacePagesComparisonWorldCitiesCohort",
		},
		{
			"nuts/DE1",
			"EurostatNUTS1",
			"",
		},
	} {
		result, _ := getCohort(c.placeType, c.placeDcid)
		if diff := cmp.Diff(result, c.want); diff != "" {
			t.Errorf("getCohort() got diff: %v", diff)
			continue
		}
	}
}
