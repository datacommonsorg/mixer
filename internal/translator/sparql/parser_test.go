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

package sparql

import (
	"strings"
	"testing"

	"github.com/go-test/deep"
)

func TestParsePrologue(t *testing.T) {
	for _, c := range []struct {
		query   string
		want    *Prologue
		wantErr bool
	}{
		{
			"BASE <https://schema.org/>",
			&Prologue{Base: "<https://schema.org/>", Prefix: map[string]string{}},
			false,
		},
		{
			"PREFIX rdf: <https:www.w3.org/1999/02/22-rdf-syntax-ns#>",
			&Prologue{Prefix: map[string]string{"rdf:": "<https:www.w3.org/1999/02/22-rdf-syntax-ns#>"}},
			false,
		},
		{
			`BASE <https://schema.org/>
			 PREFIX rdf: <https:www.w3.org/1999/02/22-rdf-syntax-ns#>`,
			&Prologue{
				Base:   "<https://schema.org/>",
				Prefix: map[string]string{"rdf:": "<https:www.w3.org/1999/02/22-rdf-syntax-ns#>"},
			},
			false,
		},
	} {
		result, err := NewParser(strings.NewReader(c.query)).parsePrologue()
		if c.wantErr {
			if err == nil {
				t.Errorf("parsePrologue(%s) = nil, want error", c.query)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}

func TestParseSelect(t *testing.T) {
	for _, c := range []struct {
		query   string
		want    *Select
		wantErr bool
	}{
		{
			"Query ?name ?person",
			nil,
			true,
		},
		{
			"SELECT DISTINCT ?name ?person",
			&Select{[]string{"?name", "?person"}, true},
			false,
		},
		{
			`SELECT ?name ?person
			WHERE {}`,
			&Select{[]string{"?name", "?person"}, false},
			false,
		},
	} {
		result, err := NewParser(strings.NewReader(c.query)).parseSelect()
		if c.wantErr {
			if err == nil {
				t.Errorf("parseSelect(%s) = nil, want error", c.query)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}

func TestParseWhere(t *testing.T) {
	for _, c := range []struct {
		query   string
		want    *Where
		wantErr bool
	}{
		{
			"Ask ?name ?person",
			nil,
			true,
		},
		{
			"Where {?person rdf:name ?name}",
			&Where{[]Triple{{"?person", "rdf:name", []string{"?name"}}}},
			false,
		},
		{
			"Where {?person rdf:name ?name . ?person rdf:address ?address }",
			&Where{[]Triple{
				{"?person", "rdf:name", []string{"?name"}},
				{"?person", "rdf:address", []string{"?address"}},
			}},
			false,
		},
		{
			`Where { ?a name ("San Jose, CA" "SJ in CA") }`,
			&Where{[]Triple{
				{"?a", "name", []string{"\"San Jose, CA\"", "\"SJ in CA\""}},
			}},
			false,
		},
	} {
		result, err := NewParser(strings.NewReader(c.query)).parseWhere()
		if c.wantErr {
			if err == nil {
				t.Errorf("parseWhere(%s) = nil, want error", c.query)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}

func TestParseOrderBy(t *testing.T) {
	for _, c := range []struct {
		query   string
		want    *Orderby
		wantErr bool
	}{
		{
			"Order By 3",
			nil,
			true,
		},
		{
			"Order By ?name",
			&Orderby{"?name", true},
			false,
		},
		{
			"Order By ASC(?age)",
			&Orderby{"?age", true},
			false,
		},
		{
			"Order By DESC(?pop)",
			&Orderby{"?pop", false},
			false,
		},
	} {
		result, err := NewParser(strings.NewReader(c.query)).parseOrderBy()
		if c.wantErr {
			if err == nil {
				t.Errorf("parseOrderBy(%s) = nil, want error", c.query)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}

func TestParseLimit(t *testing.T) {
	for _, c := range []struct {
		query   string
		want    int
		wantErr bool
	}{
		{
			"Order By ?name",
			0,
			true,
		},
		{
			"LIMIT 30.2",
			0,
			true,
		},
		{
			"LIMIT 10",
			10,
			false,
		},
	} {
		result, err := NewParser(strings.NewReader(c.query)).parseLimit()
		if c.wantErr {
			if err == nil {
				t.Errorf("parseLimit(%s) = nil, want error", c.query)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}

func TestParseTree(t *testing.T) {
	for _, c := range []struct {
		query   string
		want    *QueryTree
		wantErr bool
	}{
		{
			`BASE <http://schema.org/>
			 SELECT DISTINCT ?dcid
			 WHERE {
			 	?p typeOf Place .
				?p subType City .
				?p name "San Jose" .
				?p dcid ?dcid .
			 }
			 LIMIT 20
			`,
			&QueryTree{
				P: &Prologue{Base: "<http://schema.org/>", Prefix: map[string]string{}},
				S: &Select{[]string{"?dcid"}, true},
				W: &Where{[]Triple{
					{"?p", "typeOf", []string{"Place"}},
					{"?p", "subType", []string{"City"}},
					{"?p", "name", []string{"\"San Jose\""}},
					{"?p", "dcid", []string{"?dcid"}},
				}},
				L: 20,
			},
			false,
		},
		{
			`BASE <http://schema.org/>
			 SELECT ?a
			 WHERE {
			 	?a name ("San Jose, CA" "SJ in CA")
			 }
			 ORDER BY ?a LIMIT 10
			`,
			&QueryTree{
				P: &Prologue{Base: "<http://schema.org/>", Prefix: map[string]string{}},
				S: &Select{[]string{"?a"}, false},
				W: &Where{[]Triple{
					{"?a", "name", []string{"\"San Jose, CA\"", "\"SJ in CA\""}},
				}},
				O: &Orderby{"?a", true},
				L: 10,
			},
			false,
		},
	} {
		result, err := NewParser(strings.NewReader(c.query)).Parse()
		if c.wantErr {
			if err == nil {
				t.Errorf("Parse(%s) = nil, want error", c.query)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("Unexpected diff %v", diff)
		}
	}
}
