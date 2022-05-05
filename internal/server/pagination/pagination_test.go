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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestDencode(t *testing.T) {
	for _, c := range []struct {
		info  *pb.PaginationInfo
		token string
	}{
		{
			// One entity scenario.
			&pb.PaginationInfo{
				CursorGroups: []*pb.CursorGroup{
					{
						Cursors: []*pb.Cursor{
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
			&pb.PaginationInfo{
				CursorGroups: []*pb.CursorGroup{
					{
						Key: "geoId/05",
						Cursors: []*pb.Cursor{
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
						Key: "geoId/06",
						Cursors: []*pb.Cursor{
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
