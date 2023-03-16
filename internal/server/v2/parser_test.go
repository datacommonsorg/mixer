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
		result, err := splitArc(c.query)
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
				out: false,
			},
			true,
		},
		{
			"<-*",
			&Arc{
				out:  false,
				prop: "*",
			},
			true,
		},
		{
			"->?",
			&Arc{
				out:  true,
				prop: "?",
			},
			true,
		},
		{
			"->#",
			&Arc{
				out:  true,
				prop: "#",
			},
			true,
		},
		{
			"->prop1",
			&Arc{
				out:  true,
				prop: "prop1",
			},
			true,
		},
		{
			"<-[dcid, displayName, definition]",
			&Arc{
				out:   false,
				props: []string{"dcid", "displayName", "definition"},
			},
			true,
		},
		{
			"<-[dcid]",
			&Arc{
				out:   false,
				props: []string{"dcid"},
			},
			true,
		},
		{
			"->containedInPlace+",
			&Arc{
				out:      true,
				prop:     "containedInPlace",
				wildcard: "+",
			},
			true,
		},
		{
			"->containedInPlace+{typeOf: City}",
			&Arc{
				out:      true,
				prop:     "containedInPlace",
				wildcard: "+",
				filter: map[string]string{
					"typeOf": "City",
				},
			},
			true,
		},
		{
			"<-observationAbout{variableMeasured:  Count_Person }",
			&Arc{
				out:  false,
				prop: "observationAbout",
				filter: map[string]string{
					"variableMeasured": "Count_Person",
				},
			},
			true,
		},
		{
			"<-prop{p:v}",
			&Arc{
				out:  false,
				prop: "prop",
				filter: map[string]string{
					"p": "v",
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
		result, err := parseArc(c.s)
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
