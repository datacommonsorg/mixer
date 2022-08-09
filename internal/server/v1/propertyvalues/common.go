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

package propertyvalues

import (
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/protobuf/proto"
)

// Cache builder generates page size of 500.
// This (approximately) allows one request to be fulfilled by reading one page
// from all import groups after merging.
var defaultLimit = 1000

func buildDefaultCursorGroups(
	properties []string,
	entities []string,
	propType map[string]map[string][]string,
	n int,
) []*pb.CursorGroup {
	result := []*pb.CursorGroup{}
	for _, p := range properties {
		for _, e := range entities {
			for _, t := range propType[p][e] {
				cg := &pb.CursorGroup{
					Keys: []string{e, p, t},
				}
				for i := 0; i < n; i++ {
					cg.Cursors = append(cg.Cursors, &pb.Cursor{ImportGroup: int32(i)})
				}
				result = append(result, cg)
			}
		}
	}
	return result
}

var unmarshalFunc = func(jsonRaw []byte) (interface{}, error) {
	var p pb.PagedEntities
	err := proto.Unmarshal(jsonRaw, &p)
	return &p, err
}
