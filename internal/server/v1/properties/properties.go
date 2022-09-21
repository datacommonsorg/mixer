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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	nodewrapper "github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datacommonsorg/mixer/internal/store"
)

// Properties implements API for Mixer.Properties.
func Properties(
	ctx context.Context,
	in *pb.PropertiesRequest,
	store *store.Store,
) (*pb.PropertiesResponse, error) {
	node := in.GetNode()
	direction := in.GetDirection()
	if direction != util.DirectionIn && direction != util.DirectionOut {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid direction: should be 'in' or 'out'")
	}
	data, err := nodewrapper.GetPropertiesHelper(ctx, []string{node}, store)
	if err != nil {
		return nil, err
	}
	result := &pb.PropertiesResponse{
		Node: node,
	}
	if direction == util.DirectionIn {
		result.Properties = data[node].InLabels
	} else if direction == util.DirectionOut {
		result.Properties = data[node].OutLabels
	}
	if result.Properties == nil {
		result.Properties = []string{}
	}
	return result, nil
}

// BulkProperties implements API for Mixer.BulkProperties.
func BulkProperties(
	ctx context.Context,
	in *pb.BulkPropertiesRequest,
	store *store.Store,
) (*pb.BulkPropertiesResponse, error) {
	nodes := in.GetNodes()
	direction := in.GetDirection()
	if direction != util.DirectionIn && direction != util.DirectionOut {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid direction: should be 'in' or 'out'")
	}
	resp, err := nodewrapper.GetPropertiesHelper(ctx, nodes, store)
	if err != nil {
		return nil, err
	}
	result := &pb.BulkPropertiesResponse{
		Data: []*pb.PropertiesResponse{},
	}
	for _, node := range nodes {
		if _, ok := resp[node]; !ok {
			result.Data = append(result.Data, &pb.PropertiesResponse{
				Node:       node,
				Properties: []string{},
			})
			continue
		}
		if direction == util.DirectionIn {
			result.Data = append(result.Data, &pb.PropertiesResponse{
				Node:       node,
				Properties: resp[node].InLabels,
			})
		} else if direction == util.DirectionOut {
			result.Data = append(result.Data, &pb.PropertiesResponse{
				Node:       node,
				Properties: resp[node].OutLabels,
			})
		}
	}
	return result, nil
}
