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
	"sort"
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

var resolvedPlaceTypePriorityList = []string{
	"AdministrativeArea5",
	"AdministrativeArea4",
	"AdministrativeArea3",
	"AdministrativeArea2",
	"AdministrativeArea1",
	"EurostatNUTS3",
	"EurostatNUTS2",
	"EurostatNUTS1",
	"Town",
	"City",
	"County",
	"State",
	"Country",
}

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
		candidates := []*pbv2.ResolveResponse_Entity_Candidate{}
		for _, dcid := range e.GetOutIds() {
			candidates = append(candidates, &pbv2.ResolveResponse_Entity_Candidate{
				Dcid: dcid,
			})
		}
		resp.Entities = append(resp.Entities,
			&pbv2.ResolveResponse_Entity{
				Node:        e.GetInId(),
				ResolvedIds: e.GetOutIds(),
				Candidates:  candidates,
			})
	}
	return resp, nil
}

// Coordinate resolves geoCoordinate to DCID.
func Coordinate(
	ctx context.Context,
	store *store.Store,
	nodes []string,
	typeOfs []string,
) (*pbv2.ResolveResponse, error) {
	type latLng struct {
		lat float64
		lng float64
	}

	coordinates := []*pb.ResolveCoordinatesRequest_Coordinate{}
	latLngToNode := map[latLng]string{}
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
		latLngToNode[latLng{lat: lat, lng: lng}] = node
	}
	data, err := recon.ResolveCoordinates(ctx,
		&pb.ResolveCoordinatesRequest{Coordinates: coordinates,
			PlaceTypes: typeOfs},
		store)
	if err != nil {
		return nil, err
	}
	resp := &pbv2.ResolveResponse{}
	for _, e := range data.GetPlaceCoordinates() {
		node, ok := latLngToNode[latLng{lat: e.GetLatitude(), lng: e.GetLongitude()}]
		if !ok {
			continue
		}
		resp.Entities = append(resp.Entities,
			&pbv2.ResolveResponse_Entity{
				Node:        node,
				ResolvedIds: e.GetPlaceDcids(),
				Candidates:  getSortedResolvedPlaceCandidates(e.GetPlaces()),
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
	entities := []*pb.BulkFindEntitiesRequest_Entity{}
	for _, node := range nodes {
		if len(typeOfs) == 0 {
			entities = append(entities, &pb.BulkFindEntitiesRequest_Entity{
				Description: node,
			})
		} else {
			for _, typeOf := range typeOfs {
				entities = append(entities, &pb.BulkFindEntitiesRequest_Entity{
					Description: node,
					Type:        typeOf,
				})
			}
		}
	}
	data, err := recon.BulkFindEntities(ctx, &pb.BulkFindEntitiesRequest{
		Entities: entities,
	}, store, mapsClient)
	if err != nil {
		return nil, err
	}
	resp := &pbv2.ResolveResponse{}
	for _, e := range data.GetEntities() {
		candidates := []*pbv2.ResolveResponse_Entity_Candidate{}
		for _, dcid := range e.GetDcids() {
			candidates = append(candidates, &pbv2.ResolveResponse_Entity_Candidate{
				Dcid: dcid,
			})
		}
		resp.Entities = append(resp.Entities, &pbv2.ResolveResponse_Entity{
			Node:        e.GetDescription(),
			ResolvedIds: e.GetDcids(),
			Candidates:  candidates,
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

// Sort resolved place candidates by a priority list of place types.
// If a candidate's type is not in the priority list, then sort by DCID alphabetically.
func getSortedResolvedPlaceCandidates(
	places []*pb.ResolveCoordinatesResponse_Place) []*pbv2.ResolveResponse_Entity_Candidate {
	typeToCandidate := map[string]*pbv2.ResolveResponse_Entity_Candidate{}
	for _, place := range places {
		// Two candidates do not likely to have the same type. In the rare case they do, we can
		// randomly pick one.
		typeToCandidate[place.GetDominantType()] = &pbv2.ResolveResponse_Entity_Candidate{
			Dcid:         place.GetDcid(),
			DominantType: place.GetDominantType(),
		}
	}

	// Add candidates whose type is in the priority list.
	candidates := []*pbv2.ResolveResponse_Entity_Candidate{}
	selectedPriorityTypeSet := map[string]struct{}{}
	for _, priorityType := range resolvedPlaceTypePriorityList {
		if candidate, ok := typeToCandidate[priorityType]; ok {
			candidates = append(candidates, candidate)
			selectedPriorityTypeSet[priorityType] = struct{}{}
		}
	}

	// Sort leftover candidates.
	leftoverCandidates := []*pbv2.ResolveResponse_Entity_Candidate{}
	for t, candidate := range typeToCandidate {
		if _, ok := selectedPriorityTypeSet[t]; !ok {
			leftoverCandidates = append(leftoverCandidates, candidate)
		}
	}
	sort.Slice(leftoverCandidates, func(i, j int) bool {
		return leftoverCandidates[i].GetDcid() < leftoverCandidates[j].GetDcid()
	})

	// Assemeble final result.
	candidates = append(candidates, leftoverCandidates...)

	return candidates
}
