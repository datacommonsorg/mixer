// Copyright 2025 Google LLC
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

package spanner

import (
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestAddOptimizationsToNodeRequest(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		description string
		in          *v2.Arc
		out         *v2.Arc
		artifacts   *nodeArtifacts
	}{
		{
			"no change",
			&v2.Arc{
				SingleProp: "name",
			},
			&v2.Arc{
				SingleProp: "name",
			},
			&nodeArtifacts{},
		},
		{
			"linkedContainedInPlace",
			&v2.Arc{
				SingleProp: "containedInPlace",
				Decorator:  "+",
				Filter: map[string][]string{
					"typeOf": {"State"},
				},
			},
			&v2.Arc{
				SingleProp: "linkedContainedInPlace",
				Filter: map[string][]string{
					"typeOf": {"State"},
				},
			},
			&nodeArtifacts{
				chainProp: "containedInPlace",
			},
		},
	} {
		actual := addOptimizationsToNodeRequest(c.in)

		if diff := cmp.Diff(c.in, c.out, cmpOpts); diff != "" {
			t.Errorf("ProcessNodeRequest(%v) got diff: %s", c.in, diff)
		}

		if !reflect.DeepEqual(actual, c.artifacts) {
			t.Errorf("Artifacts are not equal.\nExpected: %+v\nActual:   %+v", c.artifacts, actual)
		}
	}
}

func TestRemoveOptimizationsFromNodeResponse(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		description string
		in          *pbv2.NodeResponse
		artifacts   *nodeArtifacts
		out         *pbv2.NodeResponse
	}{
		{
			"no change",
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"name": {
								Nodes: []*pb.EntityInfo{
									{
										Value:        "name1",
										ProvenanceId: "dc/base/test",
									},
								},
							},
						},
					},
				},
			},
			&nodeArtifacts{},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"name": {
								Nodes: []*pb.EntityInfo{
									{
										Value:        "name1",
										ProvenanceId: "dc/base/test",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"linkedContainedInPlace",
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"linkedContainedInPlace": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "place1",
										ProvenanceId: "dc/base/GeneratedGraphs",
									},
								},
							},
						},
					},
				},
			},
			&nodeArtifacts{
				chainProp: "containedInPlace",
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"containedInPlace+": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "place1"},
								},
							},
						},
					},
				},
			},
		},
	} {
		removeOptimizationsFromNodeResponse(c.in, c.artifacts)

		if diff := cmp.Diff(c.in, c.out, cmpOpts); diff != "" {
			t.Errorf("ProcessNodeResponse(%v) got diff: %s", c.in, diff)
		}
	}
}

func TestFilterObservationsByDateAndFacet(t *testing.T) {
	tests := []struct {
		name     string
		obs      []*Observation
		date     string
		expected []*Observation
	}{
		// Test: Filter matches some data.
		// Situation: Two entities are provided. One has data for the requested date (2012), the other only has data for 2011.
		// Expectation: Only the entity with data for 2012 is returned, and its observations are filtered to that date.
		{
			name: "Filter matches some data",
			obs: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
						{Date: "2012", Value: "20"},
					},
				},
				{
					ObservationAbout: "entity2",
					Observations: TimeSeries{
						{Date: "2011", Value: "15"},
					},
				},
			},
			date: "2012",
			expected: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2012", Value: "20"},
					},
				},
			},
		},
		// Test: Filter matches no data.
		// Situation: Two entities are provided, but neither has data for the requested date (2012).
		// Expectation: No observations are returned (empty slice).
		{
			name: "Filter matches no data",
			obs: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
					},
				},
				{
					ObservationAbout: "entity2",
					Observations: TimeSeries{
						{Date: "2011", Value: "15"},
					},
				},
			},
			date:     "2012",
			expected: nil,
		},
		// Test: Empty date filter keeps all.
		// Situation: Date filter is empty.
		// Expectation: All observations are retained as they were.
		{
			name: "Empty date filter keeps all",
			obs: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
					},
				},
			},
			date: "",
			expected: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filtered := filterObservationsByDateAndFacet(tc.obs, tc.date, nil)
			if diff := cmp.Diff(tc.expected, filtered); diff != "" {
				t.Errorf("Test %s: unexpected filtered results (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

