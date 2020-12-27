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

package translator

import (
	"testing"

	"github.com/datacommonsorg/mixer/internal/base"

	"github.com/go-test/deep"
)

func TestSplit(t *testing.T) {
	for _, c := range []struct {
		queryString string
		sep         rune
		want        []string
		wantErr     bool
	}{
		{
			"Select ?a, typeOf ?a City",
			',',
			[]string{"Select ?a", "typeOf ?a City"},
			false,
		},
		{
			"Select ?a, typeOf ?a City,",
			',',
			[]string{"Select ?a", "typeOf ?a City"},
			false,
		},
		{
			`Select ?a, typeOf ?a City, name ?a "San Jose"`,
			',',
			[]string{`Select ?a`, `typeOf ?a City`, `name ?a "San Jose"`},
			false,
		},
		{
			`Select ?a, typeOf ?a City, name ?a "San Jose, \"CA\""`,
			',',
			[]string{`Select ?a`, `typeOf ?a City`, `name ?a "San Jose, \"CA\""`},
			false,
		},
		{
			`Select ?a, typeOf ?a City, name ?a "San Jose, CA" "SJ in CA" `,
			',',
			[]string{`Select ?a`, `typeOf ?a City`, `name ?a "San Jose, CA" "SJ in CA"`},
			false,
		},
		{
			`Select ?a, typeOf ?a City, name ?a "San Jose `,
			',',
			nil,
			true,
		},
		{
			`numConstraints ?pop 2`,
			' ',
			[]string{`numConstraints`, `?pop`, `2`},
			false,
		},
	} {
		results, err := split(c.queryString, c.sep)
		if c.wantErr {
			if err == nil {
				t.Errorf("split(%s) = nil, want error", c.queryString)
			}
			continue
		}
		if diff := deep.Equal(c.want, results); diff != nil {
			t.Errorf("Query string: %s; unexpected nodes diff %v", c.queryString, diff)
			continue
		}
	}
}

func TestParseQuery(t *testing.T) {
	for _, c := range []struct {
		queryString string
		wantNodes   []base.Node
		wantQueries []*base.Query
		wantErr     bool
	}{
		{
			"Select ?a typeOf ?a City",
			nil,
			nil,
			true,
		},
		{
			"Query ?a typeOf ?a City",
			nil,
			nil,
			true,
		},
		{
			"Select ?a  ?b, typeOf ?a City, typeOf ?b State",
			[]base.Node{base.NewNode("?a"), base.NewNode("?b")},
			[]*base.Query{
				base.NewQuery("typeOf", "?a", "City"),
				base.NewQuery("typeOf", "?b", "State"),
			},
			false,
		},
		{
			"select ?a, typeOf ?a City, containedInPlace ?a ?b",
			[]base.Node{base.NewNode("?a")},
			[]*base.Query{
				base.NewQuery("typeOf", "?a", "City"),
				base.NewQuery("containedInPlace", "?a", base.NewNode("?b")),
			},
			false,
		},
		{
			`select ?a, name ?a "San Jose, CA" "SJ in CA"`,
			[]base.Node{base.NewNode("?a")},
			[]*base.Query{base.NewQuery("name", "?a", []string{`"San Jose, CA"`, `"SJ in CA"`})},
			false,
		},
		{
			`SELECT ?Unemployment,typeOf ?pop StatisticalPopulation,typeOf ?o Observation,` +
				`dcid ?pop dc/p/qep2q2lcc3rcc dc/p/gmw3cn8tmsnth  dc/p/92cxc027krdcd,` +
				`observedNode ?o ?pop,measuredValue ?o ?Unemployment`,
			[]base.Node{base.NewNode("?Unemployment")},
			[]*base.Query{
				base.NewQuery("typeOf", "?pop", "StatisticalPopulation"),
				base.NewQuery("typeOf", "?o", "Observation"),
				base.NewQuery("dcid", "?pop", []string{
					"dc/p/qep2q2lcc3rcc", "dc/p/gmw3cn8tmsnth", "dc/p/92cxc027krdcd"}),
				base.NewQuery("observedNode", "?o", base.NewNode("?pop")),
				base.NewQuery("measuredValue", "?o", base.NewNode("?Unemployment")),
			},
			false,
		},
	} {
		gotNodes, gotQueries, err := ParseQuery(c.queryString)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseQuery(%s) = nil, want error", c.queryString)
			}
			continue
		}
		if err != nil {
			t.Errorf("Lookup(%s) = %s", c.queryString, err)
			continue
		}
		if diff := deep.Equal(c.wantNodes, gotNodes); diff != nil {
			t.Errorf("Query string: %s; unexpected nodes diff %+v", c.queryString, diff)
			continue
		}
		if diff := deep.Equal(c.wantQueries, gotQueries); diff != nil {
			t.Errorf("Query string: %s; unexpected queries diff %+v", c.queryString, diff)
			continue
		}
	}
}

func TestParseMapping(t *testing.T) {
	for _, c := range []struct {
		mcf         string
		wantMapping []*base.Mapping
		wantErr     bool
	}{
		{
			`Node: E:Source->E1
			 typeOf: Source
			 dcid: C:Source->id
			 functionalDeps: dcid`,
			[]*base.Mapping{
				{
					Pred: "typeOf",
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.Source`"}},
					Obj:  "Source",
				},
				{
					Pred: "dcid",
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.Source`"}},
					Obj:  base.Column{Name: "id", Table: base.Table{Name: "`dc_v3.Source`"}},
				},
				{
					Pred: base.FuncDeps{},
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.Source`"}},
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
			[]*base.Mapping{
				{
					Pred: "dcid",
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.Triple`"}},
					Obj:  base.Column{Name: "Col.subject_id", Table: base.Table{Name: "`dc_v3.Triple`"}},
				},
				{
					Pred: base.Column{Name: "Col.predicate", Table: base.Table{Name: "`dc_v3.Triple`"}},
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.Triple`"}},
					Obj:  base.Entity{ID: "E2", Table: base.Table{Name: "`dc_v3.Triple`"}},
				},
				{
					Pred: base.Column{Name: "Col.predicate", Table: base.Table{Name: "`dc_v3.Triple`"}},
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.Triple`"}},
					Obj:  base.Column{Name: "Col.object_value", Table: base.Table{Name: "`dc_v3.Triple`"}},
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
			[]*base.Mapping{
				{
					Pred: "dcid",
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.Place`"}},
					Obj:  base.Column{Name: "Col.dcid", Table: base.Table{Name: "`dc_v3.Place`"}},
				},
				{
					Pred: "name",
					Sub:  base.Entity{ID: "E2", Table: base.Table{Name: "`dc_v3.Place`"}},
					Obj:  base.Column{Name: "Col.new_name", Table: base.Table{Name: "`dc_v3.Place`"}},
				},
			},
			false,
		},
		{
			`Node: E:MonthlyWeather->E1
			 typeOf: WeatherObservation
			 measuredProperty: "barometricPressure"
			 observationPeriod: "P1M"`,
			[]*base.Mapping{
				{
					Pred: "typeOf",
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.MonthlyWeather`"}},
					Obj:  "WeatherObservation",
				},
				{
					Pred: "measuredProperty",
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.MonthlyWeather`"}},
					Obj:  "barometricPressure",
				},
				{
					Pred: "observationPeriod",
					Sub:  base.Entity{ID: "E1", Table: base.Table{Name: "`dc_v3.MonthlyWeather`"}},
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
