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

package tmcf

import (
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseMcf(t *testing.T) {

	for _, c := range []struct {
		file string
		want map[string]*TableSchema
	}{
		{
			"svo.tmcf",
			map[string]*TableSchema{
				"FBI_Crime": {
					ColumnInfo: map[string][]*Column{
						"Count_CriminalActivities_MurderAndNonNegligentManslaughter": {{Node: "E1", Property: "value"}},
						"Count_CriminalActivities_ViolentCrime":                      {{Node: "E0", Property: "value"}},
						"GeoId": {
							{Node: "E0", Property: "observationAbout"},
							{Node: "E1", Property: "observationAbout"},
						},
						"Year": {
							{Node: "E0", Property: "observationDate"},
							{Node: "E1", Property: "observationDate"},
						},
					},
					NodeSchema: map[string]map[string]string{
						"E0": {
							"measurementMethod": "FBI_Crime",
							"observationPeriod": "P1M",
							"typeOf":            "StatVarObservation",
							"variableMeasured":  "Count_CriminalActivities_ViolentCrime",
						},
						"E1": {
							"measurementMethod": "FBI_Crime",
							"typeOf":            "StatVarObservation",
							"variableMeasured":  "Count_CriminalActivities_MurderAndNonNegligentManslaughter",
						}},
				},
			},
		},
	} {
		tmcf, err := ioutil.ReadFile("testdata/" + c.file)
		if err != nil {
			t.Fatalf("reading tmcf: %s", err)
		}
		tableSchema, err := ParseTmcf(string(tmcf))
		if err != nil {
			t.Fatalf("parsing tmcf file: %s", err)
		}
		if diff := cmp.Diff(tableSchema, c.want); diff != "" {
			t.Errorf("ParseTmcf got diff: %v", diff)
			continue
		}
	}
}
