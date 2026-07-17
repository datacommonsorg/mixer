// Copyright 2020 Google LLC
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

package golden

import (
	"context"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestV2GetLocationsRankingsLegacy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "legacy")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile   string
			placeType    string
			withinPlace  string
			isPerCapita  bool
			statVarDcids []string
		}{
			{
				"california.json",
				"County",
				"geoId/06",
				false,
				[]string{
					"Count_Person",
					"Median_Age_Person",
					"Count_CriminalActivities_CombinedCrime",
				},
			},
		} {
			req := &pb.GetLocationsRankingsRequest{
				PlaceType:    c.placeType,
				WithinPlace:  c.withinPlace,
				IsPerCapita:  c.isPerCapita,
				StatVarDcids: c.statVarDcids,
			}
			response, err := mixer.V2GetLocationsRankings(ctx, req)
			if err != nil {
				t.Errorf("could not call V2GetLocationsRankings: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(response, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetLocationsRankingsResponse
			if err := test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(response, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"V2GetLocationsRankingsLegacy", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}

func TestV2GetLocationsRankings(t *testing.T) {
	t.Parallel()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Dir(filename)

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string

			// Use a canned response for now just to validate the new response format.
			// TODO: Update to test the actual implementation once completed.
			response *pb.GetLocationsRankingsResponse
		}{
			{
				"california.json",
				&pb.GetLocationsRankingsResponse{
					Data: map[string]*pb.RelatedPlacesInfo{
						"Count_Person": {
							RankTop_100: []*pb.RelatedPlacesInfo_Ranking{
								{
									Info: [](*pb.RelatedPlacesInfo_Ranking_RankInfo){
										{
											Rank:      1,
											Value:     proto.Float64(100),
											PlaceDcid: "countyA",
											Date:      "2026",
										},
										{
											Rank:      2,
											Value:     proto.Float64(99),
											PlaceDcid: "countyB",
											Date:      "2026",
										},
									},
									FacetId: "2",
								},
								{
									Info: [](*pb.RelatedPlacesInfo_Ranking_RankInfo){
										{
											Rank:      1,
											Value:     proto.Float64(98),
											PlaceDcid: "countyC",
											Date:      "2026",
										},
										{
											Rank:      2,
											Value:     proto.Float64(97),
											PlaceDcid: "countyD",
											Date:      "2026",
										},
									},
									FacetId: "1",
								},
							},
							RankBottom_100: []*pb.RelatedPlacesInfo_Ranking{
								{
									Info: [](*pb.RelatedPlacesInfo_Ranking_RankInfo){
										{
											Rank:      1,
											Value:     proto.Float64(1),
											PlaceDcid: "countyZ",
											Date:      "2026",
										},
										{
											Rank:      2,
											Value:     proto.Float64(2),
											PlaceDcid: "countyY",
											Date:      "2026",
										},
									},
									FacetId: "2",
								},
								{
									Info: [](*pb.RelatedPlacesInfo_Ranking_RankInfo){
										{
											Rank:      1,
											Value:     proto.Float64(3),
											PlaceDcid: "countyX",
											Date:      "2026",
										},
										{
											Rank:      2,
											Value:     proto.Float64(4),
											PlaceDcid: "countyW",
											Date:      "2026",
										},
									},
									FacetId: "1",
								},
							},
						},
					},
					Facets: map[string]*pb.Facet{
						"1": {
							ImportName: "Test_Import1",
						},
						"2": {
							ImportName: "Test_Import2",
						},
					},
				},
			},
		} {
			if test.GenerateGolden {
				test.UpdateProtoGolden(c.response, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetLocationsRankingsResponse
			if err := test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(c.response, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"V2GetLocationsRankings", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
