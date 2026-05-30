// Copyright 2026 Google LLC
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

package agent

import (
	"context"
	"slices"
	"strings"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)



// SearchIndicators resolves explicit topics and variables matching a query,
// filtering out indicators that lack observation data for target places.
func (s *Service) SearchIndicators(
	ctx context.Context,
	req *pbv2.SearchIndicatorsRequest,
) (*pbv2.SearchIndicatorsResponse, error) {
	// Validate request parameters
	if err := validateRequest(req); err != nil {
		return nil, err
	}

	// Set default World query place if query is empty and no places are specified
	places := req.GetPlaces()
	if req.GetQuery() == "" && len(places) == 0 && req.GetParentPlace() == "" {
		places = []string{DefaultPlaceWorld}
	}

	// Resolve parent and query place names to DCIDs
	resolvedPlaces, parentPlaceDcid, err := s.resolvePlaces(ctx, places, req.GetParentPlace())
	if err != nil {
		return nil, err
	}

	// Determine final limits: if empty, default to 10
	limit := req.GetPerSearchLimit()
	if limit <= 0 {
		limit = DefaultSearchLimit
	}

	// Execute oversampled embeddings search (fetching twice the limit to guarantee high-quality matches after place filtering)
	oversampledLimit := limit * 2
	candidates, err := s.fetchCandidates(ctx, req.GetQuery(), oversampledLimit, req.GetIncludeTopics())
	if err != nil {
		return nil, err
	}

	// Perform place existence filtering if places are specified
	var placeDcids []string
	for _, info := range resolvedPlaces {
		placeDcids = append(placeDcids, info.Dcid)
	}
	if len(placeDcids) > 0 {
		candidates, err = s.filterByPlaceExistence(ctx, candidates, placeDcids, req.GetIncludeTopics())
		if err != nil {
			return nil, err
		}
	}

	// Assemble and format unified response payload with late truncation limits
	return s.translateToResponse(candidates, resolvedPlaces, parentPlaceDcid, limit)
}

// validateRequest checks the validity of the incoming request constraints.
func validateRequest(req *pbv2.SearchIndicatorsRequest) error {
	limit := req.GetPerSearchLimit()
	if limit < MinSearchLimit || limit > MaxSearchLimit {
		return status.Errorf(codes.InvalidArgument, "per_search_limit must be between %d and %d, got: %d", MinSearchLimit, MaxSearchLimit, limit)
	}
	if req.GetParentPlace() != "" && len(req.GetPlaces()) == 0 {
		return status.Errorf(codes.InvalidArgument, "places must be specified when parent_place is provided")
	}
	return nil
}

// resolvedPlaceInfo holds resolved place metadata.
type resolvedPlaceInfo struct {
	Name   string
	Dcid   string
	TypeOf []string
}

// resolvePlaces maps common place name strings to their resolved KG DCIDs.
// Using named returns explicitly documents the returned map and parents DCID in the signature itself.
func (s *Service) resolvePlaces(
	ctx context.Context,
	places []string,
	parentPlaceName string,
) (resolvedMap map[string]*resolvedPlaceInfo, parentPlaceDcid string, err error) {
	var placesToResolve []string
	placesToResolve = append(placesToResolve, places...)
	if parentPlaceName != "" {
		placesToResolve = append(placesToResolve, parentPlaceName)
	}

	if len(placesToResolve) == 0 {
		return nil, "", nil
	}

	resolveReq := &pbv2.ResolveRequest{
		Nodes:    placesToResolve,
		Property: PropDescription,
	}

	resp, err := s.mixer.V2Resolve(ctx, resolveReq)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "failed to resolve place names: %v", err)
	}

	resolvedMap = make(map[string]*resolvedPlaceInfo)

	for _, entity := range resp.GetEntities() {
		name := entity.GetNode()
		candidates := entity.GetCandidates()
		if len(candidates) == 0 {
			return nil, "", status.Errorf(codes.NotFound, "no place found matching name: %s", name)
		}

		topCand := candidates[0]
		info := &resolvedPlaceInfo{
			Name:   topCand.GetName(),
			Dcid:   topCand.GetDcid(),
			TypeOf: topCand.GetTypeOf(),
		}
		resolvedMap[name] = info

		if name == parentPlaceName {
			parentPlaceDcid = info.Dcid
		}
	}

	return resolvedMap, parentPlaceDcid, nil
}

// fetchCandidates calls V2Resolve to execute embeddings similarity searches.
func (s *Service) fetchCandidates(
	ctx context.Context,
	query string,
	limit int32,
	includeTopics bool,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	// Map empty queries to browse root topics
	var nodes []string
	if query != "" {
		nodes = []string{query}
	}

	resolveReq := &pbv2.ResolveRequest{
		Nodes:        nodes,
		Resolver:     ResolverIndicator,
		ExpandTopics: includeTopics,
	}

	resp, err := s.mixer.V2Resolve(ctx, resolveReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to execute embeddings search: %v", err)
	}

	var candidates []*pbv2.ResolveResponse_Entity_Candidate
	for _, entity := range resp.GetEntities() {
		candidates = append(candidates, entity.GetCandidates()...)
	}

	// Enforce search limit
	if limit > 0 && int(limit) < len(candidates) {
		candidates = candidates[:limit]
	}

	return candidates, nil
}

// filterByPlaceExistence uses the Cache layer to filter out candidates lacking data.
func (s *Service) filterByPlaceExistence(
	ctx context.Context,
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	placeDcids []string,
	includeTopics bool,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	// Collect all candidate variables to check.
	// We pre-compute and memoize candidate topic child variables in a local map
	// to avoid redundant recursive walks inside the nested place loop below.
	var varsToCheck []string
	candidateVars := make(map[*pbv2.ResolveResponse_Entity_Candidate][]string)

	for _, c := range candidates {
		if isTopic(c) {
			vars := collectTopicVariables(c)
			candidateVars[c] = vars
			varsToCheck = append(varsToCheck, vars...)
		} else {
			varsToCheck = append(varsToCheck, c.GetDcid())
		}
	}

	if len(varsToCheck) == 0 {
		return candidates, nil
	}

	// Batch fetch availability mappings from read-through Cache
	availabilityMap, err := s.cache.CheckAvailability(ctx, placeDcids, varsToCheck)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check variables place availability: %v", err)
	}

	// Filter candidates list based on availability
	var filtered []*pbv2.ResolveResponse_Entity_Candidate
	for _, c := range candidates {
		var placesWithData []string
		for _, p := range placeDcids {
			hasData := false
			if isTopic(c) {
				// Topic has data if any child variable has data (instantly lookup memoized list)
				for _, v := range candidateVars[c] {
					if availabilityMap[p][v] {
						hasData = true
						break
					}
				}
			} else {
				hasData = availabilityMap[p][c.GetDcid()]
			}

			if hasData {
				placesWithData = append(placesWithData, p)
			}
		}

		if len(placesWithData) > 0 || len(placeDcids) == 0 {
			// Candidate remains active! Cache the places with data metadata
			c.Metadata = map[string]string{
				MetadataPlacesWithData: strings.Join(placesWithData, DcidSeparator),
			}
			filtered = append(filtered, c)
		}
	}

	return filtered, nil
}

// collectTopicVariables recursively walks candidate children to aggregate all member variables.
func collectTopicVariables(cand *pbv2.ResolveResponse_Entity_Candidate) []string {
	var vars []string
	for _, child := range cand.GetChildren() {
		if isTopic(child) {
			vars = append(vars, collectTopicVariables(child)...)
		} else {
			vars = append(vars, child.GetDcid())
		}
	}
	return vars
}

// translateToResponse formats and translates candidate lists into the final RPC payload.
func (s *Service) translateToResponse(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
	limit int32,
) (*pbv2.SearchIndicatorsResponse, error) {
	resp := &pbv2.SearchIndicatorsResponse{
		Status:           StatusSuccess,
		DcidNameMappings: make(map[string]string),
		DcidPlaceTypeMappings: make(map[string]string),
	}

	// Populate place metadata mappings
	populatePlaceMetadata(resp, resolvedPlaces, parentPlaceDcid)

	// Populate topics and variables lists up to requested limit
	topicCount := int32(0)
	varCount := int32(0)

	for _, c := range candidates {
		resp.DcidNameMappings[c.GetDcid()] = c.GetName()

		var placesWithData []string
		if pStr, ok := c.Metadata[MetadataPlacesWithData]; ok && pStr != "" {
			placesWithData = strings.Split(pStr, DcidSeparator)
		}

		if isTopic(c) {
			if topicCount >= limit {
				continue
			}
			translateTopicCandidate(c, placesWithData, resp)
			topicCount++
		} else {
			if varCount >= limit {
				continue
			}
			translateVariableCandidate(c, placesWithData, resp)
			varCount++
		}
	}

	// Keep the slices sorted consistently to ensure deterministic test runs
	slices.SortFunc(resp.Topics, func(a, b *pbv2.SearchIndicatorsResponse_Topic) int {
		return strings.Compare(a.Dcid, b.Dcid)
	})
	slices.SortFunc(resp.Variables, func(a, b *pbv2.SearchIndicatorsResponse_Variable) int {
		return strings.Compare(a.Dcid, b.Dcid)
	})

	return resp, nil
}

// populatePlaceMetadata aggregates place information and registers parent place structures.
func populatePlaceMetadata(
	resp *pbv2.SearchIndicatorsResponse,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
) {
	for _, info := range resolvedPlaces {
		resp.DcidNameMappings[info.Dcid] = info.Name
		resp.DcidPlaceTypeMappings[info.Dcid] = strings.Join(info.TypeOf, DcidSeparator)

		if info.Dcid == parentPlaceDcid {
			resp.ResolvedParentPlace = &pbv2.SearchIndicatorsResponse_ResolvedPlace{
				Dcid:   info.Dcid,
				Name:   info.Name,
				TypeOf: info.TypeOf,
			}
		}
	}
}

// translateTopicCandidate converts a topic candidate and registers all member variable/topic names.
func translateTopicCandidate(
	c *pbv2.ResolveResponse_Entity_Candidate,
	placesWithData []string,
	resp *pbv2.SearchIndicatorsResponse,
) {
	var memberTopics []string
	var memberVariables []string

	for _, child := range c.GetChildren() {
		resp.DcidNameMappings[child.GetDcid()] = child.GetName()
		if isTopic(child) {
			memberTopics = append(memberTopics, child.GetDcid())
		} else {
			memberVariables = append(memberVariables, child.GetDcid())
		}
	}

	resp.Topics = append(resp.Topics, &pbv2.SearchIndicatorsResponse_Topic{
		Dcid:                 c.GetDcid(),
		MemberTopics:         memberTopics,
		MemberVariables:      memberVariables,
		PlacesWithData:       placesWithData,
		Description:          c.GetName(),
		AlternateDescriptions: []string{c.GetName()}, // Fallback alt description
	})
}

// translateVariableCandidate converts a variable candidate and maps its name.
func translateVariableCandidate(
	c *pbv2.ResolveResponse_Entity_Candidate,
	placesWithData []string,
	resp *pbv2.SearchIndicatorsResponse,
) {
	resp.Variables = append(resp.Variables, &pbv2.SearchIndicatorsResponse_Variable{
		Dcid:           c.GetDcid(),
		PlacesWithData: placesWithData,
		Description:    c.GetName(),
	})
}

// isTopic returns true if the candidate is classified as a Topic in TypeOf or DominantType.
func isTopic(c *pbv2.ResolveResponse_Entity_Candidate) bool {
	if c.GetDominantType() == DcidTypeTopic {
		return true
	}
	return slices.Contains(c.GetTypeOf(), DcidTypeTopic)
}
