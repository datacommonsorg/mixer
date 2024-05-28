// Copyright 2024 Google LLC
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

package cache

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/go-test/deep"
)

func TestParseStatVarGroups(t *testing.T) {
	for _, tc := range []struct {
		jsonData   string
		wantStruct *StatVarGroups
		wantProto  *pb.StatVarGroups
	}{
		{
			jsonData: `
			{
				"statVarGroups": {
					"svg1": {
						"absoluteName": "SVG1",
						"childStatVarGroups": [
							{
								"id": "svg11",
								"specializedEntity": "Specialized SVG11"
							},
							{
								"id": "svg12",
								"specializedEntity": "Specialized SVG12"
							}
						],
						"descendentStatVarCount": 2
					},
					"svg11": {
						"absoluteName": "SVG11",
						"childStatVars": [
							{
								"id": "sv1",
								"displayName": "SV1",
								"searchNames": ["sv1", "SV1"]
							}
						],
						"descendentStatVarCount": 1
					},
					"svg12": {
						"absoluteName": "SVG12",
						"childStatVars": [
							{
								"id": "sv2",
								"displayName": "SV2"
							}
						],
						"descendentStatVarCount": 1
					}
				}
			}
			`,
			wantStruct: &StatVarGroups{
				StatVarGroups: map[string]*StatVarGroupNode{
					"svg1": {
						AbsoluteName: "SVG1",
						ChildStatVarGroups: []*ChildSVG{
							{Id: "svg11", SpecializedEntity: "Specialized SVG11"},
							{Id: "svg12", SpecializedEntity: "Specialized SVG12"},
						},
						DescendentStatVarCount: 2,
					},
					"svg11": {
						AbsoluteName: "SVG11",
						ChildStatVars: []*ChildSV{
							{Id: "sv1", DisplayName: "SV1", SearchNames: []string{"sv1", "SV1"}},
						},
						DescendentStatVarCount: 1,
					},
					"svg12": {
						AbsoluteName: "SVG12",
						ChildStatVars: []*ChildSV{
							{Id: "sv2", DisplayName: "SV2"},
						},
						DescendentStatVarCount: 1,
					},
				},
			},
			wantProto: &pb.StatVarGroups{
				StatVarGroups: map[string]*pb.StatVarGroupNode{
					"svg1": {
						AbsoluteName: "SVG1",
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svg11", SpecializedEntity: "Specialized SVG11"},
							{Id: "svg12", SpecializedEntity: "Specialized SVG12"},
						},
						DescendentStatVarCount: 2,
					},
					"svg11": {
						AbsoluteName: "SVG11",
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{Id: "sv1", DisplayName: "SV1", SearchNames: []string{"sv1", "SV1"}},
						},
						DescendentStatVarCount: 1,
					},
					"svg12": {
						AbsoluteName: "SVG12",
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{Id: "sv2", DisplayName: "SV2"},
						},
						DescendentStatVarCount: 1,
					},
				},
			},
		},
	} {
		gotStruct, err := parseStatVarGroups(tc.jsonData)
		if err != nil {
			t.Fatalf("Error parsing svgs: %v", err)
		}

		if diff := deep.Equal(gotStruct, tc.wantStruct); diff != nil {
			t.Errorf("Unexpected struct diff: %v", diff)
		}

		if diff := deep.Equal(gotStruct.toProto(), tc.wantProto); diff != nil {
			t.Errorf("Unexpected proto diff: %v", diff)
		}
	}
}
