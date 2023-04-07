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

// Package properties is for V2 event API.
package event

import (
	"testing"

	v1e "github.com/datacommonsorg/mixer/internal/server/v1/event"
	"github.com/google/go-cmp/cmp"
)

func TestParseEventCollectionFilter(t *testing.T) {
	for _, c := range []struct {
		property   string
		filterExpr string
		wantErr    bool
		want       *v1e.FilterSpec
	}{
		{
			"area",
			"3.1#6.2#Acre",
			false,
			&v1e.FilterSpec{
				Prop:       "area",
				Unit:       "Acre",
				LowerLimit: 3.1,
				UpperLimit: 6.2,
			},
		},
		{"area", "3.1,6.2#Acre", true, nil},
		{"", "3.1#6.2#Acre", true, nil},
		{"are", "Abc#6.2#Acre", true, nil},
	} {
		got, err := ParseEventCollectionFilter(c.property, c.filterExpr)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseEventCollectionFilter(%s, %s) got no error, want error",
					c.property, c.filterExpr)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseEventCollectionFilter(%s, %s) = %s",
				c.property, c.filterExpr, err)
			continue
		}
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("ParseEventCollectionFilter(%s, %s) got diff: %s",
				c.property, c.filterExpr, diff)
		}
	}
}
