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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// GetPropertyValues implements API for Mixer.GetPropertyValues.
func GetPropertyValues(
	ctx context.Context,
	in *pb.GetPropertyValuesRequest,
	store *store.Store,
) (*pb.GetPropertyValuesResponse, error) {
	dcids := in.GetDcids()
	prop := in.GetProperty()
	typ := in.GetValueType()
	direction := in.GetDirection()
	limit := int(in.GetLimit())

	// Check arguments
	if prop == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument: property")
	}
	if len(dcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "missing required arguments: dcids")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid DCIDs %s", dcids)
	}

	// Get in, out or both direction
	var (
		inArc  = true
		outArc = true
		inRes  = map[string][]*pb.EntityInfo{}
		outRes = map[string][]*pb.EntityInfo{}
	)
	var err error
	if direction == util.DirectionIn {
		outArc = false
	} else if direction == util.DirectionOut {
		inArc = false
	}

	if inArc {
		inRes, err = GetPropertyValuesHelper(ctx, store, dcids, prop, false)
		if err != nil {
			return nil, err
		}
	}
	if outArc {
		outRes, err = GetPropertyValuesHelper(ctx, store, dcids, prop, true)
		if err != nil {
			return nil, err
		}
	}

	result := &pb.GetPropertyValuesResponse{Data: map[string]*pb.ArcNodes{}}
	for _, dcid := range dcids {
		result.Data[dcid] = &pb.ArcNodes{}
	}
	for dcid, entities := range inRes {
		entities = filterEntities(entities, typ, limit)
		if len(entities) > 0 {
			result.Data[dcid].In = entities

		}
	}
	for dcid, entities := range outRes {
		entities = filterEntities(entities, typ, limit)
		if len(entities) > 0 {
			result.Data[dcid].Out = entities
		}
	}
	return result, nil
}

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
		&pb.BulkPropertyValuesRequest{
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

func filterEntities(in []*pb.EntityInfo, typ string, limit int) []*pb.EntityInfo {
	if limit == 0 && typ == "" {
		return in
	}
	result := []*pb.EntityInfo{}
	for _, entity := range in {
		if typ == "" {
			result = append(result, entity)
		} else {
			for _, t := range entity.Types {
				if t == typ {
					result = append(result, entity)
					break
				}
			}
		}
		if limit > 0 && len(result) == limit {
			break
		}
	}
	return result
}
