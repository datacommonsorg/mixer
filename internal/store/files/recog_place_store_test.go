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

// Package files contains code for files.
package files

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestLoadRecogPlaceStore(t *testing.T) {
	recogPlaceStore, err := LoadRecogPlaceStore()
	if err != nil {
		t.Fatalf("LoadRecogPlaceStore() = %s", err)
	}

	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		key  string
		want *pb.RecogPlaces
	}{
		{
			"sunnyvale",
			&pb.RecogPlaces{
				Places: []*pb.RecogPlace{
					{
						Names: []*pb.RecogPlace_Name{
							{
								Parts: []string{"sunnyvale"},
							},
						},
						Dcid: "geoId/0677000",
						ContainingPlaces: []string{"Earth", "country/USA", "geoId/06",
							"geoId/06085", "northamerica", "usc/PacificDivision",
							"usc/WestRegion", "wikidataId/Q213205", "wikidataId/Q3271856"},
						Population: 152258,
					},
					{
						Names: []*pb.RecogPlace_Name{
							{
								Parts: []string{"sunnyvale"},
							},
						},
						Dcid: "geoId/4871156",
						ContainingPlaces: []string{"Earth", "country/USA", "geoId/48",
							"geoId/48113", "northamerica", "usc/SouthRegion",
							"usc/WestSouthCentralDivision"},
						Population: 8062,
					},
				},
			},
		},
		{
			"chlamydia",
			&pb.RecogPlaces{
				Places: []*pb.RecogPlace{
					{
						Names: []*pb.RecogPlace_Name{
							{
								Parts: []string{"chlamydia"},
							},
						},
						Dcid:             "Chlamydia",
						ContainingPlaces: []string{""},
						Population:       0,
					},
				},
			},
		},
	} {
		got, ok := recogPlaceStore.RecogPlaceMap[c.key]
		if !ok {
			t.Errorf("Cannot find in RecogPlaceMap: %s", c.key)
			continue
		}

		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("Key %s got diff: %v", c.key, diff)
		}
	}

	for _, c := range []struct {
		key  string
		want *pb.RecogPlaces
	}{
		{
			"US",
			&pb.RecogPlaces{
				Places: []*pb.RecogPlace{
					{
						Names: []*pb.RecogPlace_Name{
							{
								Parts: []string{"united", "states"},
							},
							{
								Parts: []string{"america"},
							},
							{
								Parts: []string{"usa"},
							},
							{
								Parts: []string{"american", "city"},
							},
							{
								Parts: []string{"american", "cities"},
							},
							{
								Parts: []string{"american", "county"},
							},
							{
								Parts: []string{"american", "counties"},
							},
							{
								Parts: []string{"american", "country"},
							},
							{
								Parts: []string{"american", "countries"},
							},
							{
								Parts: []string{"american", "district"},
							},
							{
								Parts: []string{"american", "districts"},
							},
							{
								Parts: []string{"american", "province"},
							},
							{
								Parts: []string{"american", "provinces"},
							},
							{
								Parts: []string{"american", "region"},
							},
							{
								Parts: []string{"american", "regions"},
							},
							{
								Parts: []string{"american", "state"},
							},
							{
								Parts: []string{"american", "states"},
							},
						},
						Dcid:             "country/USA",
						ContainingPlaces: []string{"Earth", "northamerica"},
						Population:       331893745,
					},
				},
			},
		},
		{
			"ME",
			&pb.RecogPlaces{
				Places: []*pb.RecogPlace{
					{
						Names: []*pb.RecogPlace_Name{
							{
								Parts: []string{"montenegro"},
							},
							{
								Parts: []string{"montenegrin", "city"},
							},
							{
								Parts: []string{"montenegrin", "cities"},
							},
							{
								Parts: []string{"montenegrin", "county"},
							},
							{
								Parts: []string{"montenegrin", "counties"},
							},
							{
								Parts: []string{"montenegrin", "country"},
							},
							{
								Parts: []string{"montenegrin", "countries"},
							},
							{
								Parts: []string{"montenegrin", "district"},
							},
							{
								Parts: []string{"montenegrin", "districts"},
							},
							{
								Parts: []string{"montenegrin", "province"},
							},
							{
								Parts: []string{"montenegrin", "provinces"},
							},
							{
								Parts: []string{"montenegrin", "region"},
							},
							{
								Parts: []string{"montenegrin", "regions"},
							},
							{
								Parts: []string{"montenegrin", "state"},
							},
							{
								Parts: []string{"montenegrin", "states"},
							},
						},
						Dcid:             "country/MNE",
						ContainingPlaces: []string{"Earth", "europe"},
						Population:       619211,
					},
					{
						Names: []*pb.RecogPlace_Name{
							{
								Parts: []string{"maine"},
							},
						},
						Dcid: "geoId/23",
						ContainingPlaces: []string{"Earth", "country/USA", "northamerica",
							"usc/NewEnglandDivision", "usc/NortheastRegion"},
						Population: 1372247,
					},
				},
			},
		},
	} {
		got, ok := recogPlaceStore.AbbreviatedNameToPlaces[c.key]
		if !ok {
			t.Errorf("Cannot find in AbbreviatedNameToPlaces: %s", c.key)
			continue
		}

		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("Key %s got diff: %v", c.key, diff)
		}
	}
}
