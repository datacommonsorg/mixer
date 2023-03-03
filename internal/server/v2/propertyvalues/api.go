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

package propertyvalues

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	v1pv "github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"

	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
)

// PropertyValues implements mixer.PropertyValues handler.
func PropertyValuesV2(
	ctx context.Context,
	in *pb.PropertyValuesV2Request,
	store *store.Store,
) (*pb.PropertyValuesV2Response, error) {
	nodes := in.GetNodes()
	property := in.GetProperty()

	data, _, err := v1pv.Fetch(
		ctx,
		store,
		nodes,
		[]string{property},
		0,
		"",
		util.DirectionOut,
	)
	if err != nil {
		return nil, err
	}
	res := &pb.PropertyValuesV2Response{Data: []*pb.NodePropertyValues{}}
	for node := range data {
		res.Data = append(
			res.Data,
			&pb.NodePropertyValues{
				Node:   node,
				Values: v1pv.MergeTypedNodes(data[node][property]),
			},
		)
	}
	return res, nil
}
