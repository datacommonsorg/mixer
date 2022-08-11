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
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PropertyValues implements mixer.PropertyValues handler.
func PropertyValues(
	ctx context.Context,
	in *pb.PropertyValuesRequest,
	store *store.Store,
) (*pb.PropertyValuesResponse, error) {
	entityProperty := in.GetEntityProperty()
	limit := int(in.GetLimit())
	token := in.GetNextToken()
	direction := in.GetDirection()

	parts := strings.Split(entityProperty, "/")
	if len(parts) < 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid request URI")
	}
	property := parts[len(parts)-1]
	entity := strings.Join(parts[0:len(parts)-1], "/")

	// Check arguments
	if property == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing required argument: property")
	}
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/property/out/ or /v1/property/in/")
	}
	if !util.CheckValidDCIDs([]string{entity}) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entity %s", entity)
	}
	data, pi, err := Fetch(
		ctx,
		store,
		[]string{entity},
		[]string{property},
		limit,
		token,
		direction,
	)
	if err != nil {
		return nil, err
	}
	nextToken := ""
	if pi != nil {
		nextToken, err = util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
	}
	res := &pb.PropertyValuesResponse{
		NextToken: nextToken,
		Values:    MergeTypedEntities(data[entity][property]),
	}
	return res, nil
}

// BulkPropertyValues implements mixer.BulkPropertyValues handler.
func BulkPropertyValues(
	ctx context.Context,
	in *pb.BulkPropertyValuesRequest,
	store *store.Store,
) (*pb.BulkPropertyValuesResponse, error) {
	property := in.GetProperty()
	entities := in.GetEntities()
	limit := int(in.GetLimit())
	token := in.GetNextToken()
	direction := in.GetDirection()

	// Check arguments
	if property == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing required argument: property")
	}
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/bulk/property/out/** or /v1/bulk/property/in/**")
	}
	if !util.CheckValidDCIDs(entities) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entities %s", entities)
	}
	data, pi, err := Fetch(
		ctx,
		store,
		entities,
		[]string{property},
		limit,
		token,
		direction,
	)
	if err != nil {
		return nil, err
	}
	nextToken := ""
	if pi != nil {
		nextToken, err = util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
	}
	res := &pb.BulkPropertyValuesResponse{
		NextToken: nextToken,
	}
	for _, e := range entities {
		res.Data = append(
			res.Data,
			&pb.BulkPropertyValuesResponse_EntityPropertyValues{
				Entity: e,
				Values: MergeTypedEntities(data[e][property]),
			},
		)
	}
	return res, nil
}
