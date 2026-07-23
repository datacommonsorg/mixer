// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import "testing"

func TestQueryConfig(t *testing.T) {
	config := QueryConfig{ContainedInPlaceAncestorFirstTypes: []string{"Place"}}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := config.containedInPlaceAccessPath("Place"); got != containedInPlaceAncestorFirst {
		t.Errorf("containedInPlaceAccessPath(Place) = %v, want ancestor first", got)
	}
	if got := config.containedInPlaceAccessPath("County"); got != containedInPlaceTypeFirst {
		t.Errorf("containedInPlaceAccessPath(County) = %v, want type first", got)
	}
	if got := config.containedInPlaceAccessPath("County", "Place"); got != containedInPlaceAncestorFirst {
		t.Errorf("containedInPlaceAccessPath(County, Place) = %v, want ancestor first", got)
	}
	if got := config.containedInPlaceAccessPath("County", "City"); got != containedInPlaceTypeFirst {
		t.Errorf("containedInPlaceAccessPath(County, City) = %v, want type first", got)
	}
}

func TestQueryConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config QueryConfig
	}{
		{
			name: "empty ancestor-first type",
			config: QueryConfig{
				ContainedInPlaceAncestorFirstTypes: []string{" "},
			},
		},
		{
			name: "padded ancestor-first type",
			config: QueryConfig{
				ContainedInPlaceAncestorFirstTypes: []string{" Place "},
			},
		},
		{
			name: "negative entity scan threshold",
			config: QueryConfig{
				ContainedInPlaceEntityScanMinVariables: -1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
		})
	}
}
