// Copyright 2019 Google LLC
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

package mcf

import (
	"testing"

	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/go-test/deep"
)

func TestParseMapping(t *testing.T) {
	for _, c := range []struct {
		mcf         string
		wantMapping []*types.Mapping
		wantErr     bool
	}{
		{
			`Node: E:Source->E1
			 typeOf: Source
			 dcid: C:Source->id
			 functionalDeps: dcid`,
			[]*types.Mapping{
				{
					Pred: "typeOf",
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.Source`"}},
					Obj:  "Source",
				},
				{
					Pred: "dcid",
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.Source`"}},
					Obj:  types.Column{Name: "id", Table: types.Table{Name: "`dc_v3.Source`"}},
				},
				{
					Pred: types.FuncDeps{},
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.Source`"}},
					Obj:  []string{"dcid"},
				},
			},
			false,
		},
		{
			`Node: E:Triple->E1
			 dcid: C:Triple->Col.subject_id
			 C:Triple->Col.predicate: E:Triple->E2
			 C:Triple->Col.predicate: C:Triple->Col.object_value`,
			[]*types.Mapping{
				{
					Pred: "dcid",
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.Triple`"}},
					Obj:  types.Column{Name: "Col.subject_id", Table: types.Table{Name: "`dc_v3.Triple`"}},
				},
				{
					Pred: types.Column{Name: "Col.predicate", Table: types.Table{Name: "`dc_v3.Triple`"}},
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.Triple`"}},
					Obj:  types.Entity{ID: "E2", Table: types.Table{Name: "`dc_v3.Triple`"}},
				},
				{
					Pred: types.Column{Name: "Col.predicate", Table: types.Table{Name: "`dc_v3.Triple`"}},
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.Triple`"}},
					Obj:  types.Column{Name: "Col.object_value", Table: types.Table{Name: "`dc_v3.Triple`"}},
				},
			},
			false,
		},
		{
			`Node: E:Place->E1
			 dcid: C:Place->Col.dcid
			 # Next entity
			 Node: E:Place->E2
			 name: C:Place->Col.new_name`,
			[]*types.Mapping{
				{
					Pred: "dcid",
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.Place`"}},
					Obj:  types.Column{Name: "Col.dcid", Table: types.Table{Name: "`dc_v3.Place`"}},
				},
				{
					Pred: "name",
					Sub:  types.Entity{ID: "E2", Table: types.Table{Name: "`dc_v3.Place`"}},
					Obj:  types.Column{Name: "Col.new_name", Table: types.Table{Name: "`dc_v3.Place`"}},
				},
			},
			false,
		},
		{
			`Node: E:MonthlyWeather->E1
			 typeOf: WeatherObservation
			 measuredProperty: "barometricPressure"
			 observationPeriod: "P1M"`,
			[]*types.Mapping{
				{
					Pred: "typeOf",
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.MonthlyWeather`"}},
					Obj:  "WeatherObservation",
				},
				{
					Pred: "measuredProperty",
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.MonthlyWeather`"}},
					Obj:  "barometricPressure",
				},
				{
					Pred: "observationPeriod",
					Sub:  types.Entity{ID: "E1", Table: types.Table{Name: "`dc_v3.MonthlyWeather`"}},
					Obj:  "P1M",
				},
			},
			false,
		},
	} {
		gotMapping, err := ParseMapping(c.mcf, "dc_v3")
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseMapping(%s) = nil, want error", c.mcf)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseMapping(%s) = %s", c.mcf, err)
			continue
		}
		if diff := deep.Equal(c.wantMapping, gotMapping); diff != nil {
			t.Errorf("MCF: %s; unexpected parse diff %+v", c.mcf, diff)
		}
	}
}
