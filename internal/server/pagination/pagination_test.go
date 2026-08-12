// Copyright 2022 Google LLC
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

package pagination

import (
	"testing"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestDecode(t *testing.T) {
	for _, c := range []struct {
		info  *pbv1.PaginationInfo
		token string
	}{
		{
			// One entity scenario.
			&pbv1.PaginationInfo{
				CursorGroups: []*pbv1.CursorGroup{
					{
						Cursors: []*pbv1.Cursor{
							{
								ImportGroup: 0,
								Page:        0,
								Item:        20,
							},
							{
								ImportGroup: 1,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 2,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 3,
								Page:        2,
								Item:        50,
							},
							{
								ImportGroup: 4,
								Page:        1,
								Item:        10,
							},
						},
					},
				},
			},
			"H4sIAAAAAAAA/+JSEWKSEBFi42AUYJTgEmLjYILSzAJMEkZCbBwsID4AAAD//wEAAP//htWoVyYAAAA=",
		},
		{
			// Multiple entity scenario.
			&pbv1.PaginationInfo{
				CursorGroups: []*pbv1.CursorGroup{
					{
						Keys: []string{"geoId/05"},
						Cursors: []*pbv1.Cursor{
							{
								ImportGroup: 0,
								Page:        0,
								Item:        20,
							},
							{
								ImportGroup: 1,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 2,
								Page:        1,
								Item:        10,
							},
							{
								ImportGroup: 3,
								Page:        2,
								Item:        50,
							},
							{
								ImportGroup: 4,
								Page:        1,
								Item:        10,
							},
						},
					},
					{
						Keys: []string{"geoId/06"},
						Cursors: []*pbv1.Cursor{
							{
								ImportGroup: 0,
								Page:        5,
								Item:        200,
							},
							{
								ImportGroup: 1,
								Page:        8,
								Item:        100,
							},
							{
								ImportGroup: 2,
								Page:        7,
								Item:        150,
							},
							{
								ImportGroup: 3,
								Page:        15,
								Item:        60,
							},
							{
								ImportGroup: 4,
								Page:        4,
								Item:        40,
							},
						},
					},
				},
			},
			"H4sIAAAAAAAA/+LS4+JIT833TNE3MBVikhARYuNgFGCU4BJi42CC0swCTBJGQmwcLCA+lxFcvZkQqwCrxAlGsBYOiRQhdg4mAXaJaYxgPfwSNmA9LBIaAAAAAP//AQAA//82b3t4ZAAAAA==",
		},
	} {
		info, err := Decode(c.token)
		if err != nil {
			t.Errorf("Decode() got err %v", err)
			continue
		}
		if diff := cmp.Diff(info, c.info, protocmp.Transform()); diff != "" {
			t.Errorf("getScorePb() got diff score %v", diff)
		}
	}
}

func TestDecodeNextToken(t *testing.T) {
	for _, c := range []struct {
		info  *pbv2.Pagination
		token string
	}{
		{
			&pbv2.Pagination{
				Info: []*pbv2.Pagination_DataSourceInfo{
					{
						Id: "spanner",
						DataSourceInfo: &pbv2.Pagination_DataSourceInfo_SpannerInfo{
							SpannerInfo: &pbv2.SpannerInfo{
								Offset: 5000,
							},
						},
					},
					{
						Id: "bigtable",
						DataSourceInfo: &pbv2.Pagination_DataSourceInfo_BigtableInfo{
							BigtableInfo: &pbv1.PaginationInfo{
								CursorGroups: []*pbv1.CursorGroup{
									{
										Keys: []string{"geoId/05"},
										Cursors: []*pbv1.Cursor{
											{
												ImportGroup: 0,
												Page:        0,
												Item:        20,
											},
											{
												ImportGroup: 1,
												Page:        1,
												Item:        10,
											},
											{
												ImportGroup: 2,
												Page:        1,
												Item:        10,
											},
											{
												ImportGroup: 3,
												Page:        2,
												Item:        50,
											},
											{
												ImportGroup: 4,
												Page:        1,
												Item:        10,
											},
										},
									},
									{
										Keys: []string{"geoId/06"},
										Cursors: []*pbv1.Cursor{
											{
												ImportGroup: 0,
												Page:        5,
												Item:        200,
											},
											{
												ImportGroup: 1,
												Page:        8,
												Item:        100,
											},
											{
												ImportGroup: 2,
												Page:        7,
												Item:        150,
											},
											{
												ImportGroup: 3,
												Page:        15,
												Item:        60,
											},
											{
												ImportGroup: 4,
												Page:        4,
												Item:        40,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			"H4sIAAAAAAAA/+Li42IvLkjMy0stkmLm6FDnKuDiSMpML0lMykkVSuHS4+JIT833TNE3MBVikhARYuNgFGCU4BJi42CC0swCTBJGQmwcLCA+lxFcvZkQqwCrxAlGsBYOiRQhdg4mAXaJaYxgPfwSNmA9LBIaAAAAAP//AQAA///R+sYdggAAAA=="},
	} {
		info, err := DecodeNextToken(c.token)
		if err != nil {
			t.Errorf("DecodeNextToken(%v) got err %v", c.token, err)
			continue
		}
		if diff := cmp.Diff(info, c.info, protocmp.Transform()); diff != "" {
			t.Errorf("DecodeNextToken(%v) got diff: %s", c.token, diff)
		}
	}
}

func TestDecode_InvalidInput(t *testing.T) {
	for _, token := range []string{"", "invalid-base64!!!", "aW52YWxpZC1nemlw"} {
		_, err := Decode(token)
		if err == nil {
			t.Errorf("Decode(%q) expected error, got nil", token)
			continue
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("Decode(%q) code = %v, want %v", token, st.Code(), codes.InvalidArgument)
		}
	}
}

func TestDecodeNextToken_InvalidInput(t *testing.T) {
	for _, token := range []string{"", "invalid-base64!!!", "aW52YWxpZC1nemlw", "SoME_veRy_L0ng_STrIng"} {
		_, err := DecodeNextToken(token)
		if err == nil {
			t.Errorf("DecodeNextToken(%q) expected error, got nil", token)
			continue
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("DecodeNextToken(%q) code = %v, want %v", token, st.Code(), codes.InvalidArgument)
		}
	}
}
