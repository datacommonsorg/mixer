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

// Package v2 is the version 2 of the Data Commons REST API.
package v2

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSplitExpr(t *testing.T) {
	for _, c := range []struct {
		query string
		parts []string
	}{
		{
			"<-",
			[]string{"<-"},
		},
		{
			"geoId/06->name",
			[]string{"geoId/06", "->", "name"},
		},
		{
			"->prop1->prop2",
			[]string{"->", "prop1", "->", "prop2"},
		},
		{
			"<-isMemberOf<-[dcid, displayName, definition]",
			[]string{"<-", "isMemberOf", "<-", "[dcid,displayName,definition]"},
		},
		{
			"->containedInPlace+->[name, typeOf]",
			[]string{
				"->", "containedInPlace+", "->", "[name,typeOf]",
			},
		},
		{
			"<-observationAbout{variableMeasured: Count_Person}->[value, date]",
			[]string{
				"<-",
				"observationAbout{variableMeasured:Count_Person}",
				"->",
				"[value,date]",
			},
		},
		{
			"<-specializationOf+<-memberOf->#",
			[]string{
				"<-",
				"specializationOf+",
				"<-",
				"memberOf",
				"->",
				"#",
			},
		},
		{
			" geoId/06 <- containedInPlace { typeOf : City } ",
			[]string{"geoId/06", "<-", "containedInPlace{typeOf:City}"},
		},
	} {
		result := splitExpr(c.query)
		if diff := cmp.Diff(result, c.parts); diff != "" {
			t.Errorf("split(%s) got diff %v", c.query, diff)
		}
	}
}

func TestParseArc(t *testing.T) {
	for _, c := range []struct {
		arrow string
		expr  string
		arc   *Arc
		valid bool
	}{
		{
			"<-",
			"",
			&Arc{
				Out: false,
			},
			true,
		},
		{
			"<-",
			"*",
			&Arc{
				Out:        false,
				SingleProp: "*",
			},
			true,
		},
		{
			"->",
			"?",
			&Arc{
				Out:        true,
				SingleProp: "?",
			},
			true,
		},
		{
			"->",
			"#",
			&Arc{
				Out:        true,
				SingleProp: "#",
			},
			true,
		},
		{
			"->",
			"prop1",
			&Arc{
				Out:        true,
				SingleProp: "prop1",
			},
			true,
		},
		{
			"<-",
			"[dcid, displayName, definition]",
			&Arc{
				Out:          false,
				BracketProps: []string{"dcid", "displayName", "definition"},
			},
			true,
		},
		{
			"<-",
			"[dcid]",
			&Arc{
				Out:          false,
				BracketProps: []string{"dcid"},
			},
			true,
		},
		{
			"->",
			"containedInPlace+",
			&Arc{
				Out:        true,
				SingleProp: "containedInPlace",
				Decorator:  "+",
			},
			true,
		},
		{
			"->",
			"containedInPlace+{typeOf: City}",
			&Arc{
				Out:        true,
				SingleProp: "containedInPlace",
				Decorator:  "+",
				Filter: map[string]map[string]struct{}{
					"typeOf": {"City": {}},
				},
			},
			true,
		},
		{
			"->",
			"description{typeOf: [City, County], label: Haha}",
			&Arc{
				Out:        true,
				SingleProp: "description",
				Filter: map[string]map[string]struct{}{
					"typeOf": {"City": {}, "County": {}},
					"label":  {"Haha": {}},
				},
			},
			true,
		},
		{
			"->",
			"description{label: Haha, typeOf: [City, County]}",
			&Arc{
				Out:        true,
				SingleProp: "description",
				Filter: map[string]map[string]struct{}{
					"typeOf": {"City": {}, "County": {}},
					"label":  {"Haha": {}},
				},
			},
			true,
		},
		{
			"->",
			"containedInPlace + { typeOf : City }",
			&Arc{
				Out:        true,
				SingleProp: "containedInPlace",
				Decorator:  "+",
				Filter: map[string]map[string]struct{}{
					"typeOf": {"City": {}},
				},
			},
			true,
		},
		{
			"<-",
			"observationAbout{variableMeasured:  Count_Person }",
			&Arc{
				Out:        false,
				SingleProp: "observationAbout",
				Filter: map[string]map[string]struct{}{
					"variableMeasured": {"Count_Person": {}},
				},
			},
			true,
		},
		{
			"<-",
			`prop{
				p1:v1,
				p2:v2
			}`,
			&Arc{
				Out:        false,
				SingleProp: "prop",
				Filter: map[string]map[string]struct{}{
					"p1": {"v1": {}},
					"p2": {"v2": {}},
				},
			},
			true,
		},
		{
			"<-",
			"[dcid",
			nil,
			false,
		},
		{
			"<-",
			"prop{dcid}",
			nil,
			false,
		},
	} {
		result, err := parseArc(c.arrow, c.expr)
		if !c.valid {
			if err == nil {
				t.Errorf("parseArc(%s) expect error, but got nil", c.expr)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseArc(%s) got error %v", c.expr, err)
			continue
		}
		if diff := cmp.Diff(result, c.arc); diff != "" {
			t.Errorf("v(%s) got diff %v", c.expr, diff)
		}
	}
}

func TestParseProperty(t *testing.T) {
	for _, c := range []struct {
		expr  string
		arc   []*Arc
		valid bool
	}{
		{
			"<-",
			[]*Arc{
				{
					Out: false,
				},
			},
			true,
		},
		{
			"->name->address",
			[]*Arc{
				{
					Out:        true,
					SingleProp: "name",
				},
				{
					Out:        true,
					SingleProp: "address",
				},
			},
			true,
		},
	} {
		result, err := ParseProperty(c.expr)
		if !c.valid {
			if err == nil {
				t.Errorf("ParseProperty(%s) expect error, but got nil", c.expr)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseProperty(%s) got error %v", c.expr, err)
			continue
		}
		if diff := cmp.Diff(result, c.arc); diff != "" {
			t.Errorf("ParseProperty(%s) got diff %v", c.expr, diff)
		}
	}
}

func TestParseLinkedNodes(t *testing.T) {
	for _, c := range []struct {
		expr  string
		g     *LinkedNodes
		valid bool
	}{
		{
			"geoId/06->name",
			&LinkedNodes{
				Subject: "geoId/06",
				Arcs: []*Arc{
					{
						SingleProp: "name",
						Out:        true,
					},
				},
			},
			true,
		},
	} {
		result, err := ParseLinkedNodes(c.expr)
		if !c.valid {
			if err == nil {
				t.Errorf("ParseLinkedNodes(%s) expect error, but got nil", c.expr)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseLinkedNodes(%s) got error %v", c.expr, err)
			continue
		}
		if diff := cmp.Diff(result, c.g); diff != "" {
			t.Errorf("v(%s) got diff %v", c.expr, diff)
		}
	}
}
