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

import (
	"testing"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/google/go-cmp/cmp"
)

func TestContainedInPlaceQueryConfig(t *testing.T) {
	ancestorFirstTypes := []string{"Place"}
	config, err := validateAndCloneContainedInPlaceQueryConfig(ContainedInPlaceQueryConfig{
		AncestorFirstTypes: ancestorFirstTypes,
	})
	if err != nil {
		t.Fatal(err)
	}
	ancestorFirstTypes[0] = "County"
	if diff := cmp.Diff([]string{"Place"}, config.AncestorFirstTypes); diff != "" {
		t.Errorf("AncestorFirstTypes were not cloned (-want +got):\n%s", diff)
	}
	if got := config.accessPath("Place"); got != containedInPlaceAncestorFirst {
		t.Errorf("accessPath(Place) = %v, want ancestor first", got)
	}
	if got := config.accessPath("County"); got != containedInPlaceTypeFirst {
		t.Errorf("accessPath(County) = %v, want type first", got)
	}
}

func TestContainedInPlaceQueryConfigRejectsEmptyType(t *testing.T) {
	_, err := validateAndCloneContainedInPlaceQueryConfig(ContainedInPlaceQueryConfig{
		AncestorFirstTypes: []string{" "},
	})
	if err == nil {
		t.Fatal("validateAndCloneContainedInPlaceQueryConfig() error = nil, want error")
	}
}

func TestNodeContainedInPlaceAccessPath(t *testing.T) {
	config := ContainedInPlaceQueryConfig{AncestorFirstTypes: []string{"Place"}}
	for _, tc := range []struct {
		name     string
		arc      *v2.Arc
		wantPath containedInPlaceAccessPath
		wantOK   bool
	}{
		{
			name: "type first",
			arc: &v2.Arc{
				SingleProp: "linkedContainedInPlace",
				Filter:     map[string][]string{"typeOf": {"County"}},
			},
			wantPath: containedInPlaceTypeFirst,
			wantOK:   true,
		},
		{
			name: "ancestor first",
			arc: &v2.Arc{
				SingleProp: "linkedContainedInPlace",
				Filter:     map[string][]string{"typeOf": {"Place"}},
			},
			wantPath: containedInPlaceAncestorFirst,
			wantOK:   true,
		},
		{
			name: "unrelated property",
			arc: &v2.Arc{
				SingleProp: "memberOf",
				Filter:     map[string][]string{"typeOf": {"County"}},
			},
		},
		{
			name: "multiple filters",
			arc: &v2.Arc{
				SingleProp: "linkedContainedInPlace",
				Filter: map[string][]string{
					"name":   {"x"},
					"typeOf": {"County"},
				},
			},
		},
		{
			name: "multiple types",
			arc: &v2.Arc{
				SingleProp: "linkedContainedInPlace",
				Filter:     map[string][]string{"typeOf": {"County", "City"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotPath, gotOK := nodeContainedInPlaceAccessPath(tc.arc, config)
			if gotPath != tc.wantPath || gotOK != tc.wantOK {
				t.Errorf("nodeContainedInPlaceAccessPath() = (%v, %t), want (%v, %t)", gotPath, gotOK, tc.wantPath, tc.wantOK)
			}
		})
	}
}

func TestNodeContainedInPlaceAccessPathFromPropertyExpression(t *testing.T) {
	arcs, err := v2.ParseProperty("<-containedInPlace+{typeOf:County}")
	if err != nil {
		t.Fatal(err)
	}
	if len(arcs) != 1 {
		t.Fatalf("ParseProperty() returned %d arcs, want 1", len(arcs))
	}
	addOptimizationsToNodeRequest(arcs[0])

	gotPath, gotOK := nodeContainedInPlaceAccessPath(arcs[0], ContainedInPlaceQueryConfig{
		AncestorFirstTypes: []string{"Place"},
	})
	if gotPath != containedInPlaceTypeFirst || !gotOK {
		t.Errorf("nodeContainedInPlaceAccessPath() = (%v, %t), want (%v, true)", gotPath, gotOK, containedInPlaceTypeFirst)
	}
}
