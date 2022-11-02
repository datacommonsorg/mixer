// Copyright 2021 Google LLC
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
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestResolveEntities(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "resolve_entities")

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			req        *pb.ResolveEntitiesRequest
			goldenFile string
		}{
			{
				&pb.ResolveEntitiesRequest{
					Entities: []*pb.EntitySubGraph{
						// BySubGraph: An entity resolved by wikidataId.
						{
							SourceId: "newId/SantaClaraCountyId",
							GraphRepresentation: &pb.EntitySubGraph_SubGraph{
								SubGraph: &pb.McfGraph{
									Nodes: map[string]*pb.McfGraph_PropertyValues{
										"newId/SantaClaraCountyId": {
											Pvs: map[string]*pb.McfGraph_Values{
												"wikidataId": {
													TypedValues: []*pb.McfGraph_TypedValue{
														{
															Type:  pb.ValueType_TEXT.Enum(),
															Value: proto.String("Q110739"),
														},
													},
												},
												"newId": {
													TypedValues: []*pb.McfGraph_TypedValue{
														{
															Type:  pb.ValueType_TEXT.Enum(),
															Value: proto.String("SantaClaraCountyId"),
														},
													},
												},
											},
										},
									},
								},
							},
						},
						// BySubGraph: A new entity that cannot be resolved to a DC entity.
						{
							SourceId: "newId/MarsPlanetId",
							GraphRepresentation: &pb.EntitySubGraph_SubGraph{
								SubGraph: &pb.McfGraph{
									Nodes: map[string]*pb.McfGraph_PropertyValues{
										"newId/MarsPlanetId": {
											Pvs: map[string]*pb.McfGraph_Values{
												"planetId": {
													TypedValues: []*pb.McfGraph_TypedValue{
														{
															Type:  pb.ValueType_TEXT.Enum(),
															Value: proto.String("Mars"),
														},
													},
												},
											},
										},
									},
								},
							},
						},
						// BySubGraph: An entity with conflicting IDs: isoCode is used for resolving.
						{
							SourceId: "newId/VietnamId",
							GraphRepresentation: &pb.EntitySubGraph_SubGraph{
								SubGraph: &pb.McfGraph{
									Nodes: map[string]*pb.McfGraph_PropertyValues{
										"newId/VietnamId": {
											Pvs: map[string]*pb.McfGraph_Values{
												"isoCode": {
													TypedValues: []*pb.McfGraph_TypedValue{
														{
															Type:  pb.ValueType_TEXT.Enum(),
															Value: proto.String("VN"),
														},
													},
												},
												"wikidataId": {
													TypedValues: []*pb.McfGraph_TypedValue{
														{
															Type: pb.ValueType_TEXT.Enum(),
															// Wrong and conflicting wikidataId.
															Value: proto.String("Q110739"),
														},
													},
												},
											},
										},
									},
								},
							},
						},
						// ByEntityIds: An entity with conflicting IDs: geoId is used for resolving.
						{
							SourceId: "newId/SunnyvaleId",
							GraphRepresentation: &pb.EntitySubGraph_EntityIds{
								EntityIds: &pb.EntityIds{
									Ids: []*pb.IdWithProperty{
										{
											Prop: "geoId",
											Val:  "0677000",
										},
										{
											Prop: "wikidataId",
											// Wrong and conflicting wikidataId.
											Val: "Q110739",
										},
									},
								},
							},
						},
					},
				},
				"result.json",
			},
		} {
			resp, err := mixer.ResolveEntities(ctx, c.req)
			if err != nil {
				t.Errorf("could not ResolveEntities: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.ResolveEntitiesResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			cmpOpts := cmp.Options{
				protocmp.Transform(),
				protocmp.SortRepeated(func(a, b *pb.ResolveEntitiesResponse_ResolvedEntity) bool {
					return a.GetSourceId() > b.GetSourceId()
				}),
			}
			if diff := cmp.Diff(resp, &expected, cmpOpts); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"ResolveEntities", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}

}
