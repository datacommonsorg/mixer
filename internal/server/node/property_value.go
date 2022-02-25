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

package node

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

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
	if direction == "in" {
		outArc = false
	} else if direction == "out" {
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
	dcids []string,
	prop string,
	arcOut bool,
) (map[string][]*pb.EntityInfo, error) {
	rowList := bigtable.BuildPropertyValuesKey(dcids, prop, arcOut)
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			var propVals pb.EntityInfoCollection
			var err error
			if isProto {
				err = proto.Unmarshal(jsonRaw, &propVals)
			} else {
				err = protojson.Unmarshal(jsonRaw, &propVals)
			}
			return propVals.Entities, err
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	result := map[string][]*pb.EntityInfo{}
	visited := map[string]map[string]struct{}{}
	// Loop over the import groups. They are ordered by the preferences in
	// /deploy/storage/bigtable_import_groups.json. So only add a node if it is
	// not seen yet.
	for _, btData := range btDataList {
		for dcid, data := range btData {
			_, ok := result[dcid]
			if ok {
				// For out arcs, only get data from one cache. Do not merge across cache.
				if arcOut {
					continue
				}
			} else {
				result[dcid] = []*pb.EntityInfo{}
			}
			if data != nil {
				entities, ok := data.([]*pb.EntityInfo)
				if !ok {
					return nil, status.Error(codes.Internal, "Failed to convert data into []*pb.EntityInfo")
				}
				if arcOut {
					// Only pick one cache for out arc.
					result[dcid] = entities
				} else {
					// Need to merge nodes of in-arc from different cache.
					if _, ok := visited[dcid]; !ok {
						visited[dcid] = map[string]struct{}{}
					}
					for _, e := range entities {
						// Check if a duplicate node has been added to the result.
						// Duplication is based on either the DCID or the value.
						if e.Dcid != "" {
							if _, ok := visited[dcid][e.Dcid]; !ok {
								result[dcid] = append(result[dcid], e)
								visited[dcid][e.Dcid] = struct{}{}
							}
						} else if e.Value != "" {
							if _, ok := visited[dcid][e.Value]; !ok {
								result[dcid] = append(result[dcid], e)
								visited[dcid][e.Value] = struct{}{}
							}
						}
					}
				}
			}
		}
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
