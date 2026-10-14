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

package emulator

import (
	"context"
	"testing"

	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// Fixture hierarchy (see testdata/seed.sql):
//
//	dc/g/Farm "Farms"
//	├── Count_Farm (geoId/01, geoId/02; dc/base/USDA_Census)
//	├── Count_FarmShipment (geoId/04 -> geoId/01, geoId/04 -> geoId/05; dc/base/USDA_Census)
//	└── dc/g/Farm_Area "Farm Area"
//	    ├── Area_Farm (geoId/01; dc/base/OtherAg)
//	    └── Area_Farm_Irrigated (no data)
//
//	dc/topic/Agriculture: Count_Farm, Area_Farm
//	dc/topic/Empty: no members
//
// dc/base/USDA_Census has source dc/s/USDA and is part of dc/d/USDA_AgCensus.
// dc/base/OtherAg has source dc/s/Other.

const countFarmDefinition = "mp=count,pt=Farm"

func TestGetStatVarGroupNode(t *testing.T) {
	s := requireSuite(t)
	farmRows := func(countFarmDef string) []*mixerspanner.StatVarGroupNode {
		return []*mixerspanner.StatVarGroupNode{
			{SVG: "dc/g/Farm", SubjectID: "Count_Farm", Name: "Number of Farms", DescendentStatVarCount: -1, HasData: true, Definition: countFarmDef},
			{SVG: "dc/g/Farm", SubjectID: "Count_FarmShipment", Name: "Farm Product Shipments", DescendentStatVarCount: -1, HasData: true},
			{SVG: "dc/g/Farm", SubjectID: "dc/g/Farm", Name: "Farms", DescendentStatVarCount: 4},
			{SVG: "dc/g/Farm", SubjectID: "dc/g/Farm_Area", Name: "Farm Area", DescendentStatVarCount: 2},
		}
	}
	farmAreaRows := []*mixerspanner.StatVarGroupNode{
		{SVG: "dc/g/Farm_Area", SubjectID: "Area_Farm", Name: "Area of Farms", DescendentStatVarCount: -1, HasData: true},
		{SVG: "dc/g/Farm_Area", SubjectID: "Area_Farm_Irrigated", Name: "Area of Irrigated Farms", DescendentStatVarCount: -1, HasData: false},
		{SVG: "dc/g/Farm_Area", SubjectID: "dc/g/Farm_Area", Name: "Farm Area", DescendentStatVarCount: 2},
	}

	tests := []struct {
		name               string
		nodes              []string
		includeDefinitions bool
		want               []*mixerspanner.StatVarGroupNode
	}{
		{
			name:  "child group and stat vars",
			nodes: []string{"dc/g/Farm"},
			want:  farmRows(""),
		},
		{
			name:               "definitions",
			nodes:              []string{"dc/g/Farm"},
			includeDefinitions: true,
			want:               farmRows(countFarmDefinition),
		},
		{
			name:  "stat var without data",
			nodes: []string{"dc/g/Farm_Area"},
			want:  farmAreaRows,
		},
		{
			name:  "multiple nodes",
			nodes: []string{"dc/g/Farm", "dc/g/Farm_Area"},
			want:  append(farmRows(""), farmAreaRows...),
		},
	}

	less := func(a, b *mixerspanner.StatVarGroupNode) bool {
		if a.SVG != b.SVG {
			return a.SVG < b.SVG
		}
		return a.SubjectID < b.SubjectID
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := s.spannerClient.GetStatVarGroupNode(context.Background(), test.nodes, test.includeDefinitions)
			if err != nil {
				t.Fatalf("GetStatVarGroupNode() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got, cmpopts.SortSlices(less)); diff != "" {
				t.Errorf("GetStatVarGroupNode() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetFilteredStatVarGroupNode(t *testing.T) {
	s := requireSuite(t)
	farmChildren := func(countFarmDef string) []*mixerspanner.SVGChild {
		return []*mixerspanner.SVGChild{
			{SubjectID: "Count_Farm", Name: "Number of Farms", Predicate: "memberOf", Definition: countFarmDef},
			{SubjectID: "Count_FarmShipment", Name: "Farm Product Shipments", Predicate: "memberOf"},
			{SubjectID: "dc/g/Farm_Area", Name: "Farm Area", Predicate: "specializationOf"},
		}
	}
	countFarm := &mixerspanner.ChildSV{SubjectID: "Count_Farm", Name: "Number of Farms"}
	countFarmShipment := &mixerspanner.ChildSV{SubjectID: "Count_FarmShipment", Name: "Farm Product Shipments"}
	farmGroup := func(count int64) *mixerspanner.ChildSVG {
		return &mixerspanner.ChildSVG{SubjectID: "dc/g/Farm", Name: "Farms", DescendentStatVarCount: count}
	}
	farmAreaGroup := func(count int64) *mixerspanner.ChildSVG {
		return &mixerspanner.ChildSVG{SubjectID: "dc/g/Farm_Area", Name: "Farm Area", DescendentStatVarCount: count}
	}

	tests := []struct {
		name                 string
		node                 string
		constrainedPlaces    []string
		constrainedImport    string
		numEntitiesExistence int
		includeDefinitions   bool
		want                 *mixerspanner.FilteredStatVarGroupNode
	}{
		{
			name:                 "single place",
			node:                 "dc/g/Farm",
			constrainedPlaces:    []string{"geoId/02"},
			numEntitiesExistence: 1,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(""),
				ChildSV:  []*mixerspanner.ChildSV{countFarm},
				ChildSVG: []*mixerspanner.ChildSVG{farmGroup(1)},
			},
		},
		{
			name:                 "existence threshold across places",
			node:                 "dc/g/Farm",
			constrainedPlaces:    []string{"geoId/01", "geoId/02"},
			numEntitiesExistence: 2,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(""),
				ChildSV:  []*mixerspanner.ChildSV{countFarm},
				ChildSVG: []*mixerspanner.ChildSVG{farmGroup(1)},
			},
		},
		{
			name:                 "place matched through second entity slot",
			node:                 "dc/g/Farm",
			constrainedPlaces:    []string{"geoId/05"},
			numEntitiesExistence: 1,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(""),
				ChildSV:  []*mixerspanner.ChildSV{countFarmShipment},
				ChildSVG: []*mixerspanner.ChildSVG{farmGroup(1)},
			},
		},
		{
			name:                 "source only",
			node:                 "dc/g/Farm",
			constrainedImport:    "dc/s/USDA",
			numEntitiesExistence: 1,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(""),
				ChildSV:  []*mixerspanner.ChildSV{countFarm, countFarmShipment},
				ChildSVG: []*mixerspanner.ChildSVG{farmGroup(2)},
			},
		},
		{
			name:                 "place and dataset",
			node:                 "dc/g/Farm",
			constrainedPlaces:    []string{"geoId/01"},
			constrainedImport:    "dc/d/USDA_AgCensus",
			numEntitiesExistence: 1,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(""),
				ChildSV:  []*mixerspanner.ChildSV{countFarm, countFarmShipment},
				ChildSVG: []*mixerspanner.ChildSVG{farmGroup(2)},
			},
		},
		{
			name:                 "child stat var group counts",
			node:                 "dc/g/Farm_Area",
			constrainedPlaces:    []string{"geoId/01"},
			numEntitiesExistence: 1,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: []*mixerspanner.SVGChild{
					{SubjectID: "Area_Farm", Name: "Area of Farms", Predicate: "memberOf"},
					{SubjectID: "Area_Farm_Irrigated", Name: "Area of Irrigated Farms", Predicate: "memberOf"},
				},
				ChildSV:  []*mixerspanner.ChildSV{{SubjectID: "Area_Farm", Name: "Area of Farms"}},
				ChildSVG: []*mixerspanner.ChildSVG{farmAreaGroup(1)},
			},
		},
		{
			name:                 "definitions",
			node:                 "dc/g/Farm",
			constrainedPlaces:    []string{"geoId/02"},
			numEntitiesExistence: 1,
			includeDefinitions:   true,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(countFarmDefinition),
				ChildSV:  []*mixerspanner.ChildSV{{SubjectID: "Count_Farm", Name: "Number of Farms", Definition: countFarmDefinition}},
				ChildSVG: []*mixerspanner.ChildSVG{farmGroup(1)},
			},
		},
		{
			name:                 "place shared by both groups and entity slots",
			node:                 "dc/g/Farm",
			constrainedPlaces:    []string{"geoId/01"},
			numEntitiesExistence: 1,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(""),
				ChildSV:  []*mixerspanner.ChildSV{countFarm, countFarmShipment},
				ChildSVG: []*mixerspanner.ChildSVG{farmGroup(3), farmAreaGroup(1)},
			},
		},
		{
			name:                 "threshold above constraint count skips filtering",
			node:                 "dc/g/Farm",
			constrainedPlaces:    []string{"geoId/01"},
			numEntitiesExistence: 2,
			want: &mixerspanner.FilteredStatVarGroupNode{
				SVGChild: farmChildren(""),
			},
		},
	}

	opts := []cmp.Option{
		cmpopts.EquateEmpty(),
		cmpopts.SortSlices(func(a, b *mixerspanner.SVGChild) bool { return a.SubjectID < b.SubjectID }),
		cmpopts.SortSlices(func(a, b *mixerspanner.ChildSV) bool { return a.SubjectID < b.SubjectID }),
		cmpopts.SortSlices(func(a, b *mixerspanner.ChildSVG) bool { return a.SubjectID < b.SubjectID }),
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := s.spannerClient.GetFilteredStatVarGroupNode(
				context.Background(),
				[]string{test.node},
				test.constrainedPlaces,
				test.constrainedImport,
				test.numEntitiesExistence,
				test.includeDefinitions,
			)
			if err != nil {
				t.Fatalf("GetFilteredStatVarGroupNode() error = %v", err)
			}
			want := map[string]*mixerspanner.FilteredStatVarGroupNode{test.node: test.want}
			if diff := cmp.Diff(want, got, opts...); diff != "" {
				t.Errorf("GetFilteredStatVarGroupNode() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetFilteredTopic(t *testing.T) {
	s := requireSuite(t)
	tests := []struct {
		name                 string
		nodes                []string
		constrainedPlaces    []string
		constrainedImport    string
		numEntitiesExistence int
		want                 map[string]int
	}{
		{
			name:                 "existence threshold across places",
			nodes:                []string{"dc/topic/Agriculture", "dc/topic/Empty"},
			constrainedPlaces:    []string{"geoId/01", "geoId/02"},
			numEntitiesExistence: 2,
			want:                 map[string]int{"dc/topic/Agriculture": 1, "dc/topic/Empty": 0},
		},
		{
			name:                 "single place",
			nodes:                []string{"dc/topic/Agriculture"},
			constrainedPlaces:    []string{"geoId/01"},
			numEntitiesExistence: 1,
			want:                 map[string]int{"dc/topic/Agriculture": 2},
		},
		{
			name:                 "source",
			nodes:                []string{"dc/topic/Agriculture"},
			constrainedImport:    "dc/s/USDA",
			numEntitiesExistence: 1,
			want:                 map[string]int{"dc/topic/Agriculture": 1},
		},
		{
			name:                 "place and dataset",
			nodes:                []string{"dc/topic/Agriculture"},
			constrainedPlaces:    []string{"geoId/01"},
			constrainedImport:    "dc/d/USDA_AgCensus",
			numEntitiesExistence: 1,
			want:                 map[string]int{"dc/topic/Agriculture": 1},
		},
		{
			name:                 "threshold above constraint count",
			nodes:                []string{"dc/topic/Agriculture"},
			constrainedPlaces:    []string{"geoId/01"},
			numEntitiesExistence: 2,
			want:                 map[string]int{"dc/topic/Agriculture": 0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := s.spannerClient.GetFilteredTopic(
				context.Background(),
				test.nodes,
				test.constrainedPlaces,
				test.constrainedImport,
				test.numEntitiesExistence,
			)
			if err != nil {
				t.Fatalf("GetFilteredTopic() error = %v", err)
			}
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("GetFilteredTopic() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
