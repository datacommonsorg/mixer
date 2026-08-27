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

package propertyvalue

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
)

// GetPropertyValuesHelper get property values.
func GetPropertyValuesHelper(
	ctx context.Context,
	store *store.Store,
	nodes []string,
	prop string,
	arcOut bool,
) (map[string][]*pb.EntityInfo, error) {
	var direction string
	if arcOut {
		direction = util.DirectionOut
	} else {
		direction = util.DirectionIn
	}
	resp, err := propertyvalues.BulkPropertyValues(
		ctx,
		&pbv1.BulkPropertyValuesRequest{
			Property:  prop,
			Nodes:     nodes,
			Direction: direction,
		},
		store,
	)
	if err != nil {
		return nil, err
	}
	result := map[string][]*pb.EntityInfo{}
	for _, item := range resp.Data {
		result[item.GetNode()] = item.GetValues()
	}
	return result, nil
}
