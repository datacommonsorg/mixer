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

// Package v2.
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
