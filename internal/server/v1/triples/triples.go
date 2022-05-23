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

package triples

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/v1/properties"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Triples implements mixer.Triples handler.
func Triples(
	ctx context.Context,
	in *pb.TriplesRequest,
	store *store.Store,
) (*pb.TriplesResponse, error) {
	entity := in.GetEntity()
	token := in.GetNextToken()
	if !util.CheckValidDCIDs([]string{entity}) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entity %s", entity)
	}
	// Assemble out and in arc entities. Do this sequentially, if needed can
	// parallel the fetch.
	res := &pb.TriplesResponse{
		OutEntities: map[string]*pb.EntityInfoCollection{},
		InEntities:  map[string]*pb.EntityInfoCollection{},
	}
	paginationInfo := &pb.PaginationInfo{}
	for _, direction := range []string{util.DirectionOut, util.DirectionIn} {
		propsResp, err := properties.Properties(
			ctx, &pb.PropertiesRequest{
				Entity:    entity,
				Direction: direction,
			},
			store,
		)
		if err != nil {
			return nil, err
		}
		properties := propsResp.GetProperties()
		data, pi, err := propertyvalues.PropertyValuesHelper(
			ctx,
			store,
			properties,
			[]string{entity},
			0,
			token,
			direction,
		)
		if err != nil {
			return nil, err
		}
		if pi != nil {
			if direction == util.DirectionOut {
				paginationInfo.OutCursorGroups = pi.OutCursorGroups
			} else {
				paginationInfo.InCursorGroups = pi.InCursorGroups
			}
		}
		var entityMap map[string]*pb.EntityInfoCollection
		if direction == util.DirectionOut {
			entityMap = res.OutEntities
		} else {
			entityMap = res.InEntities
		}
		for property := range data {
			entityMap[property] = &pb.EntityInfoCollection{
				Entities: data[property][entity],
			}
		}
	}
	nextToken := ""
	if paginationInfo.OutCursorGroups != nil || paginationInfo.InCursorGroups != nil {
		var err error
		nextToken, err = util.EncodeProto(paginationInfo)
		if err != nil {
			return nil, err
		}
	}
	res.NextToken = nextToken
	return res, nil
}
