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
	"sort"
	"strconv"
	"strings"

	"github.com/datacommonsorg/mixer/internal/maps"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// --- Constants related to the resolve endpoint ---
	// ResolveTargetBaseOnly indicates resolution should only use the base mixer.
	ResolveTargetBaseOnly = "base_only"
	// ResolveTargetCustomOnly indicates resolution should only use the custom mixer.
	ResolveTargetCustomOnly = "custom_only"
	// ResolveTargetBaseAndCustom indicates resolution should use both custom and base mixers.
	ResolveTargetBaseAndCustom = "base_and_custom"

	// ResolveResolverPlace is the default resolver type.
	ResolveResolverPlace = "place"
	// ResolveResolverIndicator is the resolver name for indicator/embeddings resolution.
	ResolveResolverIndicator = "indicator"
	// ResolveResolverEmbeddings is the resolver name for Spanner embeddings resolution.
	ResolveResolverEmbeddings = "embeddings"

	// ResolveDefaultPropertyExpression is the property name for description.
	ResolveDefaultPropertyExpression = "<-description->dcid"
	// DcidProperty is the property name for dcid.
	DcidProperty = "dcid"
	// DescriptionProperty is the property name for description.
	DescriptionProperty = "description"
	// GeoCoordinateProperty is the property name for geoCoordinate.
	GeoCoordinateProperty = "geoCoordinate"
	// TypeOfProperty is the property name for typeOf.
	TypeOfProperty = "typeOf"
)

type NormalizedResolveRequest struct {
	Request      *pbv2.ResolveRequest
	InProp       string
	OutProp      string
	TypeOfValues []string
}

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
				Node:       e.GetInId(),
				Candidates: candidates,
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
		lat, lng, err := ParseCoordinate(node)
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
				Node:       node,
				Candidates: GetSortedResolvedPlaceCandidates(e.GetPlaces()),
			})
	}
	return resp, nil
}

// Description resolves description to DCID.
func Description(
	ctx context.Context,
	store *store.Store,
	mapsClient maps.MapsClient,
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
			Node:       e.GetDescription(),
			Candidates: candidates,
		})
	}
	return resp, nil
}

// ParseCoordinate parses a `lat#lng` coordinate expression used by resolve.
func ParseCoordinate(coordinateExpr string) (float64, float64, error) {
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

// GetSortedResolvedPlaceCandidates sorts resolved place candidates by place-type priority.
// If a candidate's type is not in the priority list, then sort by DCID alphabetically.
func GetSortedResolvedPlaceCandidates(
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

// validateAndParseResolveInputs validates and parses the inputs for the resolve request.
//
// Validation logic:
// - `target`: Must be one of "base_only", "custom_only", "base_and_custom". Defaults to "base_and_custom".
// - `resolver`: Must be one of "place", "indicator". Defaults to "place".
// - `property`:
//   - Must match the format "<-inProp->outProp" (optionally with filters).
//   - "inProp" and "outProp" validation depends on the resolver:
//   - "place": if "inProp" is "description" or "geoCoordinate", "outProp" must be "dcid".
//   - "indicator": "inProp" must be "description" and "outProp" must be "dcid".
//
// Returns:
// - normalized request (*NormalizedResolveRequest)
// - error if validation fails
func ValidateAndParseResolveInputs(in *pbv2.ResolveRequest) (*NormalizedResolveRequest, error) {
	normalizedRequest := proto.Clone(in).(*pbv2.ResolveRequest)
	var validationErrors []string

	// Parse and validate `target`
	targetError := parseAndValidateResolveTarget(normalizedRequest)
	if targetError != "" {
		validationErrors = append(validationErrors, targetError)
	}

	// Parse and validate `resolver`
	resolverError := parseAndValidateResolveResolver(normalizedRequest)
	if resolverError != "" {
		validationErrors = append(validationErrors, resolverError)
	}

	// Parse `property` expression into its in arc property, out arc property and typeOf filter values
	if normalizedRequest.GetProperty() == "" {
		normalizedRequest.Property = ResolveDefaultPropertyExpression
	}

	inProp, outProp, typeOfValues, err := parseResolvePropertyExpression(normalizedRequest.GetProperty())
	if err != nil {
		validationErrors = append(validationErrors, fmt.Sprintf("Invalid 'property' expression: %v", err))
	}

	// Validate property expression based on resolver
	if err == nil {
		switch normalizedRequest.GetResolver() {
		case ResolveResolverPlace:
			// geoCoordinate and description only support dcid as outArc
			if (inProp == DescriptionProperty || inProp == GeoCoordinateProperty) && outProp != DcidProperty {
				validationErrors = append(validationErrors, fmt.Sprintf(
					"Invalid 'property' expression: given input property '%s', output property can only be '%s'",
					inProp, DcidProperty))
			}
		case ResolveResolverIndicator:
			// Indicator resolution only supports description as inArc.
			if inProp != DescriptionProperty {
				validationErrors = append(validationErrors, fmt.Sprintf(
					"Invalid 'property' expression: indicator resolution only supports '%s' as input property",
					DescriptionProperty))
			}
			// Indicator resolution only supports dcid as outArc.
			if outProp != DcidProperty {
				validationErrors = append(validationErrors, fmt.Sprintf(
					"Invalid 'property' expression: indicator resolution only supports '%s' as output property",
					DcidProperty))
			}
		}
	}

	if len(validationErrors) > 0 {
		return &NormalizedResolveRequest{
			Request:      normalizedRequest,
			InProp:       "",
			OutProp:      "",
			TypeOfValues: nil,
		}, status.Errorf(codes.InvalidArgument, "Invalid inputs in request. %s", strings.Join(validationErrors, ". "))
	}

	return &NormalizedResolveRequest{
		Request:      normalizedRequest,
		InProp:       inProp,
		OutProp:      outProp,
		TypeOfValues: typeOfValues,
	}, nil
}

// parseAndValidateResolveTarget parses and validates ResolveRequest.Target.
// Returns an optional error string.
func parseAndValidateResolveTarget(req *pbv2.ResolveRequest) string {
	switch req.GetTarget() {
	case ResolveTargetBaseOnly, ResolveTargetCustomOnly, ResolveTargetBaseAndCustom:
		return ""
	case "":
		// Set default value; ignored if current call is to base dc
		req.Target = ResolveTargetBaseAndCustom
		return ""
	default:
		return fmt.Sprintf("Invalid 'target': valid values are '%s', '%s', '%s'",
			ResolveTargetCustomOnly, ResolveTargetBaseOnly, ResolveTargetBaseAndCustom)
	}
}

// parseAndValidateResolveResolver parses and validates ResolveRequest.Resolver.
// Returns an optional error string.
func parseAndValidateResolveResolver(req *pbv2.ResolveRequest) string {
	switch req.GetResolver() {
	case ResolveResolverPlace, ResolveResolverIndicator, ResolveResolverEmbeddings:
		return ""
	case "":
		// Set default value
		req.Resolver = ResolveResolverPlace
		return ""
	default:
		return fmt.Sprintf("Invalid 'resolver': valid values are '%s', '%s', '%s'",
			ResolveResolverIndicator, ResolveResolverEmbeddings, ResolveResolverPlace)
	}
}

// parseResolvePropertyExpression parses and validates a property expression string.
//
// The expression generally takes the form "<-inProp->outProp", optionally with filters
// like "<-inProp{typeOf:Type}->outProp".
//
// Returns:
// - input property (string)
// - output property (string)
// - typeOf filter values ([]string, from the input property filter)
// - error if validation fails
func parseResolvePropertyExpression(prop string) (string, string, []string, error) {
	// Parse property expression into Arcs.
	arcs, err := v2.ParseProperty(prop)
	if err != nil {
		return "", "", nil, err
	}

	if len(arcs) != 2 {
		return "", "", nil, fmt.Errorf("must define exactly two parts (incoming and outgoing arcs). Found %d parts", len(arcs))
	}

	inArc := arcs[0]
	outArc := arcs[1]
	if inArc.Out || !outArc.Out {
		return "", "", nil, fmt.Errorf("must start with an incoming arc and end with an outgoing arc")
	}

	if inArc.SingleProp == "" {
		return "", "", nil, fmt.Errorf("input property must be provided")
	}
	if outArc.SingleProp == "" {
		return "", "", nil, fmt.Errorf("output property must be provided")
	}

	var typeOfValues []string
	// Validate filters
	if len(inArc.Filter) > 0 {
		if len(inArc.Filter) > 1 {
			return "", "", nil, fmt.Errorf("only '%s' filter is supported", TypeOfProperty)
		}
		if filter, ok := inArc.Filter[TypeOfProperty]; !ok {
			for k := range inArc.Filter {
				return "", "", nil, fmt.Errorf("invalid filter key '%s'. Only '%s' filter is supported", k, TypeOfProperty)
			}
		} else {
			typeOfValues = filter
		}
	}

	return inArc.SingleProp, outArc.SingleProp, typeOfValues, nil
}

// resolveRouting determines whether to route to local and/or remote instances
// based on the target parameter and the presence of a remote mixer domain.
// Returns (shouldCallLocal, shouldCallRemote).
//
// Assumes that `target` has been validated.
//
// logic:
//   - If remoteMixerDomain is empty, we are the base instance (or standalone).
//     Always process locally, ignore target.
//   - If remoteMixerDomain is set, we are a custom instance.
//     Route based on target:
//   - "base_only": Call remote only.
//   - "custom_only": Call local only.
//   - "base_and_custom": Call both.
//   - Any other value defaults to calling both.
func ResolveRouting(target string, hasRemoteMixerDomain bool) (bool, bool) {
	if !hasRemoteMixerDomain {
		return true, false
	}
	switch target {
	case ResolveTargetBaseOnly:
		return false, true
	case ResolveTargetCustomOnly:
		return true, false
	default:
		return true, true
	}
}
