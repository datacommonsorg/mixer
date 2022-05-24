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
	direction := in.GetDirection()
	token := in.GetNextToken()
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/triples/out/ or /v1/triples/in/")
	}
	if !util.CheckValidDCIDs([]string{entity}) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entity %s", entity)
	}
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
	data, pi, err := propertyvalues.Fetch(
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
	res := &pb.TriplesResponse{
		Triples: map[string]*pb.EntityInfoCollection{},
	}
	for property := range data {
		res.Triples[property] = &pb.EntityInfoCollection{
			Entities: data[property][entity],
		}
	}
	if pi != nil {
		nextToken, err := util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
		res.NextToken = nextToken
	}
	return res, nil
}

// BulkTriples implements mixer.BulkTriples handler.
func BulkTriples(
	ctx context.Context,
	in *pb.BulkTriplesRequest,
	store *store.Store,
) (*pb.BulkTriplesResponse, error) {
	entities := in.GetEntities()
	direction := in.GetDirection()
	token := in.GetNextToken()
	if direction != util.DirectionOut && direction != util.DirectionIn {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/triples/out/ or /v1/triples/in/")
	}
	if !util.CheckValidDCIDs(entities) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entities %s", entities)
	}
	bulkPropsResp, err := properties.BulkProperties(
		ctx, &pb.BulkPropertiesRequest{
			Entities:  entities,
			Direction: direction,
		},
		store,
	)
	if err != nil {
		return nil, err
	}
	bulkProps := bulkPropsResp.GetData()
	entityProps := map[string]map[string]struct{}{}
	for _, e := range entities {
		entityProps[e] = map[string]struct{}{}
	}
	properties := []string{}
	for _, resp := range bulkProps {
		for _, p := range resp.GetProperties() {
			entityProps[resp.GetEntity()][p] = struct{}{}
		}
		properties = util.MergeDedupe(properties, resp.GetProperties())
	}
	data, pi, err := propertyvalues.Fetch(
		ctx,
		store,
		properties,
		entities,
		0,
		token,
		direction,
	)
	if err != nil {
		return nil, err
	}
	res := &pb.BulkTriplesResponse{
		Data: []*pb.BulkTriplesResponse_EntityTriples{},
	}
	triplesByEntity := map[string]map[string][]*pb.EntityInfo{}
	for _, e := range entities {
		triplesByEntity[e] = map[string][]*pb.EntityInfo{}
	}
	for p := range data {
		for e := range data[p] {
			if _, ok := entityProps[e][p]; ok {
				triplesByEntity[e][p] = data[p][e]
			}
		}
	}
	for e := range triplesByEntity {
		entityTriples := &pb.BulkTriplesResponse_EntityTriples{
			Entity:  e,
			Triples: map[string]*pb.EntityInfoCollection{},
		}
		for p := range triplesByEntity[e] {
			entityTriples.Triples[p] = &pb.EntityInfoCollection{
				Entities: triplesByEntity[e][p],
			}
		}
		res.Data = append(res.Data, entityTriples)
	}

	if pi != nil {
		nextToken, err := util.EncodeProto(pi)
		if err != nil {
			return nil, err
		}
		res.NextToken = nextToken
	}
	return res, nil
}
