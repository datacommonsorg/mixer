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

	"github.com/datacommonsorg/mixer/internal/server/datasources"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/google/go-cmp/cmp"
)

func TestPlanNodeQuery(t *testing.T) {
	containedInPlaceArc := func() *v2.Arc {
		return &v2.Arc{
			SingleProp: linkedContainedInPlaceProperty,
			Filter:     map[string][]string{typeOfProperty: {"County"}},
		}
	}

	for _, tc := range []struct {
		name string
		arc  *v2.Arc
		want nodeQueryPlan
	}{
		{
			name: "contained in place",
			arc:  containedInPlaceArc(),
			want: nodeQueryPlan{
				kind:           nodeQueryContainedInPlace,
				childPlaceType: "County",
			},
		},
		{
			name: "nil arc",
			want: nodeQueryPlan{kind: nodeQueryGeneric},
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
				Filter:     map[string][]string{typeOfProperty: {"County"}},
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
				Filter:     map[string][]string{typeOfProperty: {"County"}},
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
					"name":         {"x"},
					typeOfProperty: {"County"},
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
				Filter:     map[string][]string{typeOfProperty: nil},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "multiple types",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{typeOfProperty: {"County", "City"}},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
		{
			name: "blank type",
			arc: &v2.Arc{
				SingleProp: linkedContainedInPlaceProperty,
				Filter:     map[string][]string{typeOfProperty: {" "}},
			},
			want: nodeQueryPlan{kind: nodeQueryGeneric},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, planNodeQuery(tc.arc), cmp.AllowUnexported(nodeQueryPlan{})); diff != "" {
				t.Errorf("planNodeQuery() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlanNodeQueryFromPropertyExpression(t *testing.T) {
	arcs, err := v2.ParseProperty("<-containedInPlace+{typeOf:County}")
	if err != nil {
		t.Fatal(err)
	}
	if len(arcs) != 1 {
		t.Fatalf("ParseProperty() returned %d arcs, want 1", len(arcs))
	}
	addOptimizationsToNodeRequest(arcs[0])

	want := nodeQueryPlan{
		kind:           nodeQueryContainedInPlace,
		childPlaceType: "County",
	}
	if diff := cmp.Diff(want, planNodeQuery(arcs[0]), cmp.AllowUnexported(nodeQueryPlan{})); diff != "" {
		t.Errorf("planNodeQuery() mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildPlannedNodeEdgesByIDQuery(t *testing.T) {
	ids := []string{"country/USA", "country/IND"}
	config := QueryConfig{ContainedInPlaceAncestorFirstTypes: []string{"Place"}}

	t.Run("contained in place", func(t *testing.T) {
		arc := &v2.Arc{
			SingleProp: linkedContainedInPlaceProperty,
			Filter:     map[string][]string{typeOfProperty: {"County"}},
		}
		got := buildPlannedNodeEdgesByIDQuery(ids, arc, datasources.DefaultPageSize, 0, config)
		want := GetNodeContainedInPlaceEdgesByIDQuery(ids, "County", datasources.DefaultPageSize, 0, config)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("buildPlannedNodeEdgesByIDQuery() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("generic", func(t *testing.T) {
		arc := &v2.Arc{SingleProp: "name"}
		got := buildPlannedNodeEdgesByIDQuery(ids, arc, datasources.DefaultPageSize, 0, config)
		want := GetNodeEdgesByIDQuery(ids, arc, datasources.DefaultPageSize, 0)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("buildPlannedNodeEdgesByIDQuery() mismatch (-want +got):\n%s", diff)
		}
	})
}
