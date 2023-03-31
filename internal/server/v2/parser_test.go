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

func TestSplit(t *testing.T) {
	for _, c := range []struct {
		query string
		parts []string
	}{
		{
			"<-",
			[]string{},
		},
		{
			"->prop1->prop2",
			[]string{"->prop1", "->prop2"},
		},
		{
			"<-isMemberOf<-[dcid, displayName, definition]",
			[]string{"<-isMemberOf", "<-[dcid, displayName, definition]"},
		},
		{
			"->containedInPlace+->[name, typeOf]",
			[]string{
				"->containedInPlace+", "->[name, typeOf]",
			},
		},
		{
			"<-observationAbout{variableMeasured: Count_Person}->[value, date]",
			[]string{
				"<-observationAbout{variableMeasured: Count_Person}",
				"->[value, date]",
			},
		},
		{
			"<-specializationOf+<-memberOf->#",
			[]string{
				"<-specializationOf+", "<-memberOf", "->#",
			},
		},
	} {
		result, err := SplitArc(c.query)
		if err != nil {
			t.Errorf("split(%s) got error %v", c.query, err)
			continue
		}
		if diff := cmp.Diff(result, c.parts); diff != "" {
			t.Errorf("split(%s) got diff %v", c.query, diff)
		}
	}
}

func TestParseArc(t *testing.T) {
	for _, c := range []struct {
		s     string
		arc   *Arc
		valid bool
	}{
		{
			"<-",
			&Arc{
				Out: false,
			},
			true,
		},
		{
			"<-*",
			&Arc{
				Out:        false,
				SingleProp: "*",
			},
			true,
		},
		{
			"->?",
			&Arc{
				Out:        true,
				SingleProp: "?",
			},
			true,
		},
		{
			"->#",
			&Arc{
				Out:        true,
				SingleProp: "#",
			},
			true,
		},
		{
			"->prop1",
			&Arc{
				Out:        true,
				SingleProp: "prop1",
			},
			true,
		},
		{
			"<-[dcid, displayName, definition]",
			&Arc{
				Out:          false,
				BracketProps: []string{"dcid", "displayName", "definition"},
			},
			true,
		},
		{
			"<-[dcid]",
			&Arc{
				Out:          false,
				BracketProps: []string{"dcid"},
			},
			true,
		},
		{
			"->containedInPlace+",
			&Arc{
				Out:        true,
				SingleProp: "containedInPlace",
				Wildcard:   "+",
			},
			true,
		},
		{
			"->containedInPlace+{typeOf: City}",
			&Arc{
				Out:        true,
				SingleProp: "containedInPlace",
				Wildcard:   "+",
				Filter: map[string]string{
					"typeOf": "City",
				},
			},
			true,
		},
		{
			"<-observationAbout{variableMeasured:  Count_Person }",
			&Arc{
				Out:        false,
				SingleProp: "observationAbout",
				Filter: map[string]string{
					"variableMeasured": "Count_Person",
				},
			},
			true,
		},
		{
			`<-prop{
				p1:v1,
				p2:v2
			}`,
			&Arc{
				Out:        false,
				SingleProp: "prop",
				Filter: map[string]string{
					"p1": "v1",
					"p2": "v2",
				},
			},
			true,
		},
		{
			"<-[dcid",
			nil,
			false,
		},
		{
			"<-prop{dcid}",
			nil,
			false,
		},
	} {
		result, err := ParseArc(c.s)
		if !c.valid {
			if err == nil {
				t.Errorf("parseArc(%s) expect error, but got nil", c.s)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseArc(%s) got error %v", c.s, err)
			continue
		}
		if diff := cmp.Diff(result, c.arc, cmp.AllowUnexported(Arc{})); diff != "" {
			t.Errorf("v(%s) got diff %v", c.s, diff)
		}
	}
}
