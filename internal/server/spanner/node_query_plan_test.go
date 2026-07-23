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

func TestPlanNodeQuery(t *testing.T) {
	containedInPlaceArc := func() *v2.Arc {
		return &v2.Arc{
			SingleProp: linkedContainedInPlaceProperty,
			Filter:     map[string][]string{predTypeOf: {"County"}},
		}
	}

	for _, tc := range []struct {
		name        string
		arc         *v2.Arc
		queryConfig QueryConfig
		want        nodeQueryPlan
		wantErr     bool
	}{
		{
			name: "contained in place legacy fallback",
			arc:  containedInPlaceArc(),
			want: nodeQueryPlan{
				kind: nodeQueryContainedInPlace,
				containedInPlace: containedInPlacePlan{
					accessPath: containedInPlaceTypeFirst,
				},
			},
		},
		{
			name: "contained in place ancestor first",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{predTypeOf: {"Place"}},
			},
			queryConfig: QueryConfig{
				ContainedInPlaceAncestorFirstTypes: []string{"Place"},
			},
			want: nodeQueryPlan{
				kind: nodeQueryContainedInPlace,
				containedInPlace: containedInPlacePlan{
					accessPath: containedInPlaceAncestorFirst,
				},
			},
		},
		{
			name: "contained in place ancestor first when any type matches",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{predTypeOf: {"County", "Place"}},
			},
			queryConfig: QueryConfig{
				ContainedInPlaceAncestorFirstTypes: []string{"Place"},
			},
			want: nodeQueryPlan{
				kind: nodeQueryContainedInPlace,
				containedInPlace: containedInPlacePlan{
					accessPath: containedInPlaceAncestorFirst,
				},
			},
		},
		{
			name:    "nil arc",
			wantErr: true,
		},
		{
			name: "outgoing",
			arc: func() *v2.Arc {
				arc := containedInPlaceArc()
				arc.Out = true
				return arc
			}(),
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "unnormalized",
			arc: &v2.Arc{
				SingleProp: v2.ContainedInPlaceProperty,
				Decorator:  "+",
				Filter:     map[string][]string{predTypeOf: {"County"}},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "remaining decorator",
			arc: func() *v2.Arc {
				arc := containedInPlaceArc()
				arc.Decorator = "+"
				return arc
			}(),
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "unrelated property",
			arc: &v2.Arc{
				SingleProp: "memberOf",
				Filter:     map[string][]string{predTypeOf: {"County"}},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "bracket properties",
			arc: func() *v2.Arc {
				arc := containedInPlaceArc()
				arc.BracketProps = []string{"name"}
				return arc
			}(),
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "bracket filters",
			arc: func() *v2.Arc {
				arc := containedInPlaceArc()
				arc.BracketFilters = map[string]map[string][]string{"name": {}}
				return arc
			}(),
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "multiple filters",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter: map[string][]string{
					"name":     {"x"},
					predTypeOf: {"County"},
				},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "missing type filter",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{"name": {"x"}},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "no type values",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{predTypeOf: nil},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "multiple types",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{predTypeOf: {"County", "City"}},
			},
			want: nodeQueryPlan{
				kind: nodeQueryContainedInPlace,
				containedInPlace: containedInPlacePlan{
					accessPath: containedInPlaceTypeFirst,
				},
			},
		},
		{
			name: "blank type",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{predTypeOf: {" "}},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := planNodeQuery(tc.arc, tc.queryConfig)
			if tc.wantErr {
				if err == nil {
					t.Fatal("planNodeQuery() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("planNodeQuery() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(nodeQueryPlan{}, containedInPlacePlan{})); diff != "" {
				t.Errorf("planNodeQuery() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlanNodeQueryFromPropertyExpression(t *testing.T) {
	arcs, err := v2.ParseProperty("<-containedInPlace+{typeOf:[County,Place]}")
	if err != nil {
		t.Fatal(err)
	}
	if len(arcs) != 1 {
		t.Fatalf("ParseProperty() returned %d arcs, want 1", len(arcs))
	}
	addOptimizationsToNodeRequest(arcs[0])

	want := nodeQueryPlan{
		kind: nodeQueryContainedInPlace,
		containedInPlace: containedInPlacePlan{
			accessPath: containedInPlaceAncestorFirst,
		},
	}
	got, err := planNodeQuery(arcs[0], QueryConfig{
		ContainedInPlaceAncestorFirstTypes: []string{"Place"},
	})
	if err != nil {
		t.Fatalf("planNodeQuery() unexpected error: %v", err)
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(nodeQueryPlan{}, containedInPlacePlan{})); diff != "" {
		t.Errorf("planNodeQuery() mismatch (-want +got):\n%s", diff)
	}
}

func TestGetNodeEdgesByIDQueryRejectsNilArc(t *testing.T) {
	if _, err := GetNodeEdgesByIDQuery(nil, nil, 1, 0, QueryConfig{}); err == nil {
		t.Fatal("GetNodeEdgesByIDQuery() expected error, got nil")
	}
}

func TestBuildNodeEdgesByIDQueryRejectsUnsupportedPlan(t *testing.T) {
	arc := &v2.Arc{SingleProp: "name"}
	plan := nodeQueryPlan{kind: nodeQueryKind(100)}
	if _, err := buildNodeEdgesByIDQuery(nil, arc, 1, 0, plan); err == nil {
		t.Fatal("buildNodeEdgesByIDQuery() expected error, got nil")
	}
}

func TestBuildNodeEdgesByIDQueryRejectsUnsupportedContainedInPlacePlan(t *testing.T) {
	arc := &v2.Arc{SingleProp: linkedContainedInPlaceProperty}
	plan := nodeQueryPlan{
		kind: nodeQueryContainedInPlace,
		containedInPlace: containedInPlacePlan{
			accessPath: containedInPlaceAccessPath(100),
		},
	}
	if _, err := buildNodeEdgesByIDQuery(nil, arc, 1, 0, plan); err == nil {
		t.Fatal("buildNodeEdgesByIDQuery() expected error, got nil")
	}
}
