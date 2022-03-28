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
	"github.com/datacommonsorg/mixer/internal/server/node"
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
	entity := in.GetEntity()
	direction := in.GetDirection()
	if direction != "in" && direction != "out" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid direction: should be 'in' or 'out'")
	}
	data, err := node.GetPropertiesHelper(ctx, []string{entity}, store)
	if err != nil {
		return nil, err
	}
	result := &pb.PropertiesResponse{
		Entity: entity,
	}
	if direction == "in" {
		result.Properties = data[entity].InLabels
	} else if direction == "out" {
		result.Properties = data[entity].OutLabels
	}
	if result.Properties == nil {
		result.Properties = []string{}
	}
	return result, nil
}

// Properties implements API for Mixer.Properties.
func BulkProperties(
	ctx context.Context,
	in *pb.BulkPropertiesRequest,
	store *store.Store,
) (*pb.BulkPropertiesResponse, error) {
	entities := in.GetEntities()
	direction := in.GetDirection()
	if direction != "in" && direction != "out" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid direction: should be 'in' or 'out'")
	}
	resp, err := node.GetPropertiesHelper(ctx, entities, store)
	if err != nil {
		return nil, err
	}
	result := &pb.BulkPropertiesResponse{
		Data: []*pb.PropertiesResponse{},
	}
	for _, entity := range entities {
		if _, ok := resp[entity]; !ok {
			result.Data = append(result.Data, &pb.PropertiesResponse{
				Entity:     entity,
				Properties: []string{},
			})
			continue
		}
		if direction == "in" {
			result.Data = append(result.Data, &pb.PropertiesResponse{
				Entity:     entity,
				Properties: resp[entity].InLabels,
			})
		} else if direction == "out" {
			result.Data = append(result.Data, &pb.PropertiesResponse{
				Entity:     entity,
				Properties: resp[entity].OutLabels,
			})
		}
	}
	return result, nil
}
