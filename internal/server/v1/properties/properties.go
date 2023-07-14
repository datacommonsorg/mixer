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

package properties

import (
	"context"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	nodewrapper "github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datacommonsorg/mixer/internal/store"
)

// Properties implements API for Mixer.Properties.
func Properties(
	ctx context.Context,
	in *pbv1.PropertiesRequest,
	store *store.Store,
) (*pbv1.PropertiesResponse, error) {
	node := in.GetNode()
	direction := in.GetDirection()
	if direction != util.DirectionIn && direction != util.DirectionOut {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid direction: should be 'in' or 'out'")
	}
	data, err := nodewrapper.GetPropertiesHelper(ctx, []string{node}, store, direction)
	if err != nil {
		return nil, err
	}
	result := &pbv1.PropertiesResponse{
		Node: node,
	}
	result.Properties = data[node]
	return result, nil
}

// BulkProperties implements API for Mixer.BulkProperties.
func BulkProperties(
	ctx context.Context,
	in *pbv1.BulkPropertiesRequest,
	store *store.Store,
) (*pbv1.BulkPropertiesResponse, error) {
	nodes := in.GetNodes()
	direction := in.GetDirection()
	if direction != util.DirectionIn && direction != util.DirectionOut {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid direction: should be 'in' or 'out'")
	}
	resp, err := nodewrapper.GetPropertiesHelper(ctx, nodes, store, direction)
	if err != nil {
		return nil, err
	}
	result := &pbv1.BulkPropertiesResponse{
		Data: []*pbv1.PropertiesResponse{},
	}
	for _, node := range nodes {
		if _, ok := resp[node]; !ok {
			result.Data = append(result.Data, &pbv1.PropertiesResponse{
				Node:       node,
				Properties: []string{},
			})
			continue
		}
		result.Data = append(result.Data, &pbv1.PropertiesResponse{
			Node:       node,
			Properties: resp[node],
		})
	}
	return result, nil
}
