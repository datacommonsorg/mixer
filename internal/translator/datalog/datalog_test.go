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

package datalog

import (
	"testing"

	"github.com/datacommonsorg/mixer/internal/translator/types"
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
		wantNodes   []types.Node
		wantQueries []*types.Query
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
			[]types.Node{types.NewNode("?a"), types.NewNode("?b")},
			[]*types.Query{
				types.NewQuery("typeOf", "?a", "City"),
				types.NewQuery("typeOf", "?b", "State"),
			},
			false,
		},
		{
			"select ?a, typeOf ?a City, containedInPlace ?a ?b",
			[]types.Node{types.NewNode("?a")},
			[]*types.Query{
				types.NewQuery("typeOf", "?a", "City"),
				types.NewQuery("containedInPlace", "?a", types.NewNode("?b")),
			},
			false,
		},
		{
			`select ?a, name ?a "San Jose, CA" "SJ in CA"`,
			[]types.Node{types.NewNode("?a")},
			[]*types.Query{types.NewQuery("name", "?a", []string{`"San Jose, CA"`, `"SJ in CA"`})},
			false,
		},
		{
			`SELECT ?Unemployment,typeOf ?pop StatisticalPopulation,typeOf ?o Observation,` +
				`dcid ?pop dc/p/qep2q2lcc3rcc dc/p/gmw3cn8tmsnth  dc/p/92cxc027krdcd,` +
				`observedNode ?o ?pop,measuredValue ?o ?Unemployment`,
			[]types.Node{types.NewNode("?Unemployment")},
			[]*types.Query{
				types.NewQuery("typeOf", "?pop", "StatisticalPopulation"),
				types.NewQuery("typeOf", "?o", "Observation"),
				types.NewQuery("dcid", "?pop", []string{
					"dc/p/qep2q2lcc3rcc", "dc/p/gmw3cn8tmsnth", "dc/p/92cxc027krdcd"}),
				types.NewQuery("observedNode", "?o", types.NewNode("?pop")),
				types.NewQuery("measuredValue", "?o", types.NewNode("?Unemployment")),
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
