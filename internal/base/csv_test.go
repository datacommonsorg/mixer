// Copyright 2021 Google LLC
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

package base

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseMcf(t *testing.T) {

	for _, c := range []struct {
		file string
		want [][]string
	}{
		{
			"pop_obs.csv",
			[][]string{
				{
					"City", " Gender", " AgeRange", " Count", " Year", " StateName", " StateId", "CountyId",
				},
				{
					"New York City", " dcs:Male", " [10 20 Years]", " 20000000", " 2012", " New York", "", " geoId/NYCounty",
				},
				{
					"San Francisco", " dcs:Female", " [20 30 Years]", " 10000000.0", " 2007", "", "geoId/CA", " geoId/SFCounty",
				},
			},
		},
	} {
		records := ReadCsvFile("testdata/" + c.file)
		if diff := cmp.Diff(records, c.want); diff != "" {
			t.Errorf("ReadCsvFile got diff: %v", diff)
			continue
		}
	}
}
