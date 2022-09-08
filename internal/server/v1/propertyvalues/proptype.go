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
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

func getNodePropType(
	ctx context.Context,
	btGroup *bigtable.Group,
	nodes []string,
	properites []string,
	direction string,
) (map[string]map[string][]string, error) {
	btDataList, err := bigtable.Read(
		ctx,
		btGroup,
		bigtable.BtPropType,
		[][]string{nodes, properites},
		func(jsonRaw []byte) (interface{}, error) {
			var propertyTypes pb.PropertyTypes
			if err := proto.Unmarshal(jsonRaw, &propertyTypes); err != nil {
				return nil, err
			}
			return &propertyTypes, nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string][]string{}

	for _, btData := range btDataList {
		for _, row := range btData {
			propTypes := row.Data.(*pb.PropertyTypes)
			var types []string
			if direction == util.DirectionOut {
				types = propTypes.OutTypes
			} else {
				types = propTypes.InTypes
			}
			if result[row.Parts[0]] == nil {
				result[row.Parts[0]] = map[string][]string{}
			}
			if result[row.Parts[0]][row.Parts[1]] == nil {
				result[row.Parts[0]][row.Parts[1]] = types
			} else {
				result[row.Parts[0]][row.Parts[1]] = util.MergeDedupe(
					result[row.Parts[0]][row.Parts[1]],
					types,
				)
			}
		}
	}
	return result, nil
}
