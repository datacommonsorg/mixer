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

// Package resolve is for V2 resolve API.
package resolve

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"googlemaps.github.io/maps"
)

// ID resolves ID to ID.
func ID(
	ctx context.Context,
	store *store.Store,
	nodes []string,
	inProp string,
	outProp string,
) (*pbv2.ResolveResponse, error) {
	data, err := recon.ResolveIds(ctx,
		&pb.ResolveIdsRequest{
			Ids:     nodes,
			InProp:  inProp,
			OutProp: outProp,
		},
		store)
	if err != nil {
		return nil, err
	}
	resp := &pbv2.ResolveResponse{}
	for _, e := range data.GetEntities() {
		resp.Entities = append(resp.Entities,
			&pbv2.ResolveResponse_Entity{
				Node:        e.GetInId(),
				ResolvedIds: e.GetOutIds(),
			})
	}
	return resp, nil
}

// Coordinate resolves geoCoordinate to DCID.
func Coordinate(
	ctx context.Context,
	store *store.Store,
	nodes []string,
) (*pbv2.ResolveResponse, error) {
	coordinates := []*pb.ResolveCoordinatesRequest_Coordinate{}
	for _, node := range nodes {
		lat, lng, err := parseCoordinate(node)
		if err != nil {
			return nil, err
		}
		coordinates = append(coordinates,
			&pb.ResolveCoordinatesRequest_Coordinate{
				Latitude:  lat,
				Longitude: lng,
			})
	}
	data, err := recon.ResolveCoordinates(ctx,
		&pb.ResolveCoordinatesRequest{Coordinates: coordinates},
		store)
	if err != nil {
		return nil, err
	}
	resp := &pbv2.ResolveResponse{}
	for _, e := range data.GetPlaceCoordinates() {
		resp.Entities = append(resp.Entities,
			&pbv2.ResolveResponse_Entity{
				Node:        fmt.Sprintf("%f#%f", e.GetLatitude(), e.GetLongitude()),
				ResolvedIds: e.GetPlaceDcids(),
			})
	}
	return resp, nil
}

// Description resolves description to DCID.
func Description(
	ctx context.Context,
	store *store.Store,
	mapsClient *maps.Client,
	nodes []string,
	typeOfs []string,
) (*pbv2.ResolveResponse, error) {
	entities := []*pb.ResolveDescriptionRequest_Entity{}
	for _, node := range nodes {
		if len(typeOfs) == 0 {
			entities = append(entities, &pb.ResolveDescriptionRequest_Entity{
				Description: node,
			})
		} else {
			for _, typeOf := range typeOfs {
				entities = append(entities, &pb.ResolveDescriptionRequest_Entity{
					Description: node,
					Type:        typeOf,
				})
			}
		}
	}
	data, err := recon.ResolveDescription(ctx, &pb.ResolveDescriptionRequest{
		Entities: entities,
	}, store, mapsClient)
	if err != nil {
		return nil, err
	}
	resp := &pbv2.ResolveResponse{}
	for _, e := range data.GetEntities() {
		resp.Entities = append(resp.Entities, &pbv2.ResolveResponse_Entity{
			Node:        e.GetDescription(),
			ResolvedIds: e.GetDcids(),
		})
	}
	return resp, nil
}

func parseCoordinate(coordinateExpr string) (float64, float64, error) {
	parts := strings.Split(coordinateExpr, "#")
	if len(parts) != 2 {
		return 0, 0, status.Errorf(codes.InvalidArgument,
			"invalid coordinate expression: %s", coordinateExpr)
	}

	lat, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, 0, status.Errorf(codes.InvalidArgument,
			"invalid coordinate expression: %s", coordinateExpr)
	}

	lng, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, 0, status.Errorf(codes.InvalidArgument,
			"invalid coordinate expression: %s", coordinateExpr)
	}

	return lat, lng, nil
}
