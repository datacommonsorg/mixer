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
	"log/slog"
	"slices"
	"strings"
	"time"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resolvedPlaceInfo holds resolved place metadata.
type resolvedPlaceInfo struct {
	Name   string
	Dcid   string
	TypeOf []string
}

// SearchIndicators resolves explicit topics and variables matching a query,
// filtering out indicators that lack observation data for target places.
func (s *Service) SearchIndicators(
	ctx context.Context,
	req *SearchIndicatorsRequest,
) (*SearchIndicatorsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	defer util.TimeTrack(time.Now(), "Agent: SearchIndicators")
	slog.Info("SearchIndicators started", "query", req.Query, "places", req.Places, "placeDcids", req.PlaceDcids, "parentPlace", req.ParentPlace)

	// Validate request parameters
	if err := validateRequest(req); err != nil {
		return nil, err
	}

	// Determine final limits: if empty, default to 10
	limit := req.PerSearchLimit
	if limit <= 0 {
		limit = DefaultSearchLimit
	}

	// Phase 1: Resolve place names and fetch similarity candidates in parallel
	var placeDcids []string
	var resolvedPlaces map[string]*resolvedPlaceInfo
	var parentPlaceDcid string
	var candidates []*pbv2.ResolveResponse_Entity_Candidate

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		placeDcids, resolvedPlaces, parentPlaceDcid, err = s.acquirePlaceDcids(gCtx, req)
		return err
	})

	g.Go(func() error {
		var err error
		oversampledLimit := limit * 2
		// Fetch candidates using embeddings search. Leaf expansion is enabled depending on s.defaultExpandTopics.
		candidates, err = s.fetchCandidates(gCtx, req.Query, oversampledLimit, s.defaultExpandTopics, req.Target)
		if err != nil {
			return err
		}
		// If IncludeTopics is false, expand and deduplicate topic candidates into standard variables
		if !req.IncludeTopics {
			candidates = expandTopicCandidates(candidates)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Phase 2: Run place existence filtering and place metadata enrichment in parallel
	g2, g2Ctx := errgroup.WithContext(ctx)

	// Goroutine A: Filter candidate indicators by place existence checking
	g2.Go(func() error {
		if len(placeDcids) > 0 {
			var err error
			candidates, err = s.filterByPlaceExistence(g2Ctx, candidates, placeDcids, req.Target)
			return err
		}
		return nil
	})

	// Goroutine B: Fetch and enrich names and types of resolved places in parallel
	g2.Go(func() error {
		if resolvedPlaces != nil {
			s.enrichPlaceNamesAndTypes(g2Ctx, resolvedPlaces)
		}
		return nil
	})

	if err := g2.Wait(); err != nil {
		return nil, err
	}

	// If place_dcids is explicitly specified, or if no legacy place fields are passed, return tabular payload.
	if len(req.PlaceDcids) > 0 || (len(req.Places) == 0 && req.ParentPlace == "") {
		return s.translateToTabularResponse(candidates, resolvedPlaces, parentPlaceDcid, limit), nil
	}

	// Assemble and format legacy response payload for backward compatibility
	return s.translateToResponse(candidates, resolvedPlaces, parentPlaceDcid, limit), nil
}

// validateRequest checks the validity of the incoming request constraints.
func validateRequest(req *SearchIndicatorsRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	limit := req.PerSearchLimit
	if limit < MinSearchLimit || limit > MaxSearchLimit {
		return status.Errorf(codes.InvalidArgument, "per_search_limit must be between %d and %d, got: %d", MinSearchLimit, MaxSearchLimit, limit)
	}
	if req.ParentPlace != "" {
		if len(req.Places) == 0 {
			return status.Errorf(codes.InvalidArgument, "places must be specified when parent_place is provided")
		}
		if len(req.PlaceDcids) > 0 {
			return status.Errorf(codes.InvalidArgument, "parent_place cannot be specified alongside place_dcids")
		}
	}
	if target := req.Target; target != "" && !slices.Contains(validTargets, target) {
		return status.Errorf(codes.InvalidArgument, "invalid target: %s, valid values are '%s'", target, strings.Join(validTargets, "', '"))
	}
	return nil
}

// acquirePlaceDcids returns direct place_dcids if provided, or resolves legacy places/parentPlace strings.
func (s *Service) acquirePlaceDcids(
	ctx context.Context,
	req *SearchIndicatorsRequest,
) ([]string, map[string]*resolvedPlaceInfo, string, error) {
	if len(req.PlaceDcids) > 0 {
		return req.PlaceDcids, nil, "", nil
	}

	places := req.Places
	isImplicitWorld := false

	if len(places) == 0 && req.ParentPlace == "" {
		if req.Query == "" {
			places = []string{DefaultPlaceWorld}
			isImplicitWorld = true
		} else {
			return nil, nil, "", nil
		}
	}

	resolvedMap, parentDcid, err := s.resolvePlaces(ctx, places, req.ParentPlace, req.Target)
	if err != nil {
		return nil, nil, "", err
	}
	var dcids []string
	for _, name := range places {
		if info, ok := resolvedMap[name]; ok && info.Dcid != "" {
			if !slices.Contains(dcids, info.Dcid) {
				dcids = append(dcids, info.Dcid)
			}
		}
	}

	if isImplicitWorld {
		return dcids, nil, "", nil
	}
	return dcids, resolvedMap, parentDcid, nil
}

// resolvePlaces maps common place name strings to their resolved KG DCIDs.
// Using named returns explicitly documents the returned map and parents DCID in the signature itself.
func (s *Service) resolvePlaces(
	ctx context.Context,
	places []string,
	parentPlaceName string,
	target string,
) (resolvedMap map[string]*resolvedPlaceInfo, parentPlaceDcid string, err error) {
	defer util.TimeTrack(time.Now(), "Agent: resolvePlaces")
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
		Target:   target,
	}

	resp, err := s.mixer.V2Resolve(ctx, resolveReq)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "failed to resolve place names: %v", err)
	}
	if resp == nil {
		return nil, "", status.Error(codes.Internal, "received nil response from V2Resolve")
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

// filterByPlaceExistence uses the Cache layer to filter out candidates lacking data.
func (s *Service) filterByPlaceExistence(
	ctx context.Context,
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	placeDcids []string,
	target string,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	defer util.TimeTrack(time.Now(), "Agent: filterByPlaceExistence")
	slog.Info("Filtering indicators by place existence", "candidatesCount", len(candidates), "placesCount", len(placeDcids))

	if len(placeDcids) == 0 {
		return candidates, nil
	}

	// Collect all topic DCIDs (parents & child subtopics) and direct variables across all candidates
	topicDcids, directVarDcids := collectSubtopicsAndDirectVars(candidates)

	// Fetch descendant variables of all topics concurrently in a single batch
	topicDescendants, err := s.fetchDescendantVariables(ctx, topicDcids, target)
	if err != nil {
		return nil, err
	}

	// Gather all variables to check for place availability
	varsToCheck := gatherVarsToCheck(directVarDcids, topicDescendants)
	if len(varsToCheck) == 0 {
		return candidates, nil
	}

	// Batch fetch availability mappings from read-through Cache
	availabilityMap, err := s.cache.CheckAvailability(ctx, placeDcids, varsToCheck)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check variables place availability: %v", err)
	}

	// Filter candidates list and prune child subtopics and variables in-place
	var filtered []*pbv2.ResolveResponse_Entity_Candidate
	for _, c := range candidates {
		if isTopic(c) {
			if prunedTopic, ok := pruneSingleTopic(c, placeDcids, topicDescendants, availabilityMap); ok {
				filtered = append(filtered, prunedTopic)
			}
		} else {
			if prunedVar, ok := pruneSingleVariable(c, placeDcids, availabilityMap); ok {
				filtered = append(filtered, prunedVar)
			}
		}
	}

	return filtered, nil
}

// expandTopicCandidates unpacks all nested children variables of topic candidates,
// returning a flat slice of standard statistical variable candidates deduplicated by DCID.
func expandTopicCandidates(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
) []*pbv2.ResolveResponse_Entity_Candidate {
	var flat []*pbv2.ResolveResponse_Entity_Candidate
	seen := make(map[string]bool)

	for _, c := range candidates {
		if isTopic(c) {
			for _, child := range c.GetChildren() {
				if isTopic(child) {
					continue // Skip nested sub-topic candidates since include_topics is false
				}
				if !seen[child.GetDcid()] {
					seen[child.GetDcid()] = true
					flat = append(flat, child)
				}
			}
		} else {
			if !seen[c.GetDcid()] {
				seen[c.GetDcid()] = true
				flat = append(flat, c)
			}
		}
	}
	return flat
}

// pruneSingleTopic prunes empty subtopics/variables from a topic candidate c, returning true if the topic itself has data.
func pruneSingleTopic(
	c *pbv2.ResolveResponse_Entity_Candidate,
	placeDcids []string,
	topicDescendants map[string][]string,
	availabilityMap map[string]map[string]bool,
) (*pbv2.ResolveResponse_Entity_Candidate, bool) {
	parentPlaces := hasPlacesWithData(topicDescendants[c.GetDcid()], placeDcids, availabilityMap)
	if len(parentPlaces) == 0 {
		return nil, false
	}

	var keptChildren []*pbv2.ResolveResponse_Entity_Candidate
	for _, child := range c.GetChildren() {
		var childPlaces []string
		if isTopic(child) {
			childPlaces = hasPlacesWithData(topicDescendants[child.GetDcid()], placeDcids, availabilityMap)
		} else {
			childPlaces = placesWithDataForVar(child.GetDcid(), placeDcids, availabilityMap)
		}

		if len(childPlaces) > 0 {
			setPlacesWithDataMetadata(child, childPlaces)
			keptChildren = append(keptChildren, child)
		}
	}

	c.Children = keptChildren

	if len(parentPlaces) > 0 {
		setPlacesWithDataMetadata(c, parentPlaces)
	}

	return c, true
}

// pruneSingleVariable filters a variable candidate by place availability.
func pruneSingleVariable(
	c *pbv2.ResolveResponse_Entity_Candidate,
	placeDcids []string,
	availabilityMap map[string]map[string]bool,
) (*pbv2.ResolveResponse_Entity_Candidate, bool) {
	placesWithData := placesWithDataForVar(c.GetDcid(), placeDcids, availabilityMap)
	if len(placesWithData) == 0 {
		return nil, false
	}
	setPlacesWithDataMetadata(c, placesWithData)
	return c, true
}

// setPlacesWithDataMetadata sets the metadata field for places with data.
func setPlacesWithDataMetadata(c *pbv2.ResolveResponse_Entity_Candidate, places []string) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[MetadataPlacesWithData] = strings.Join(places, DcidSeparator)
}

// buildTopicsTable constructs a tabular representation of matched topic candidates.
func buildTopicsTable(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	limit int32,
) *Table {
	table := &Table{
		Columns: []string{colDcid, colName, colPlacesWithData, colMemberTopics, colMemberVariables},
	}
	topicCount := int32(0)
	for _, c := range candidates {
		if !isTopic(c) {
			continue
		}
		if topicCount >= limit {
			break
		}
		var placesWithData []string
		if pStr, ok := c.GetMetadata()[MetadataPlacesWithData]; ok && pStr != "" {
			placesWithData = strings.Split(pStr, DcidSeparator)
		}
		var memberTopics []string
		var memberVariables []string
		for _, child := range c.GetChildren() {
			if isTopic(child) {
				memberTopics = append(memberTopics, child.GetDcid())
			} else {
				memberVariables = append(memberVariables, child.GetDcid())
			}
		}
		table.Rows = append(table.Rows, []any{
			c.GetDcid(),
			c.GetName(),
			util.NonNilStringSlice(placesWithData),
			util.NonNilStringSlice(memberTopics),
			util.NonNilStringSlice(memberVariables),
		})
		topicCount++
	}
	return table
}

// buildVariablesTable constructs a tabular representation of matched statistical variable candidates.
func buildVariablesTable(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	limit int32,
) *Table {
	table := &Table{
		Columns: []string{colDcid, colName, colPlacesWithData, colObservationProperties},
	}
	varCount := int32(0)
	for _, c := range candidates {
		if isTopic(c) {
			continue
		}
		if varCount >= limit {
			break
		}
		var placesWithData []string
		if pStr, ok := c.GetMetadata()[MetadataPlacesWithData]; ok && pStr != "" {
			placesWithData = strings.Split(pStr, DcidSeparator)
		}
		table.Rows = append(table.Rows, []any{
			c.GetDcid(),
			c.GetName(),
			util.NonNilStringSlice(placesWithData),
			util.NonNilStringSlice(c.GetObservationProperties()),
		})
		varCount++
	}
	return table
}

// translateToTabularResponse formats matched topics and variables into tabular SearchIndicatorsResponse fields.
func (s *Service) translateToTabularResponse(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
	limit int32,
) *SearchIndicatorsResponse {
	resp := &SearchIndicatorsResponse{
		Status:             StatusSuccess,
		TopicCandidates:    buildTopicsTable(candidates, limit),
		VariableCandidates: buildVariablesTable(candidates, limit),
	}

	if resolvedPlaces != nil {
		populatePlaceMetadata(resp, resolvedPlaces, parentPlaceDcid)
	}

	return resp
}

// translateToResponse formats and translates candidate lists into the legacy SearchIndicatorsResponse payload.
func (s *Service) translateToResponse(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
	limit int32,
) *SearchIndicatorsResponse {
	resp := &SearchIndicatorsResponse{
		Status:                StatusSuccess,
		DcidNameMappings:      make(map[string]string),
		DcidPlaceTypeMappings: make(map[string][]string),
	}

	// Populate place metadata mappings
	populatePlaceMetadata(resp, resolvedPlaces, parentPlaceDcid)

	// Populate topics and variables lists up to requested limit
	topicCount := int32(0)
	varCount := int32(0)

	for _, c := range candidates {
		var placesWithData []string
		if pStr, ok := c.Metadata[MetadataPlacesWithData]; ok && pStr != "" {
			placesWithData = strings.Split(pStr, DcidSeparator)
		}

		if isTopic(c) {
			if topicCount >= limit {
				continue
			}
			resp.DcidNameMappings[c.GetDcid()] = c.GetName()
			translateTopicCandidate(c, placesWithData, resp)
			topicCount++
		} else {
			if varCount >= limit {
				continue
			}
			resp.DcidNameMappings[c.GetDcid()] = c.GetName()
			translateVariableCandidate(c, placesWithData, resp)
			varCount++
		}
	}

	// Keep the slices sorted consistently to ensure deterministic test runs
	slices.SortFunc(resp.Topics, func(a, b *TopicCandidate) int {
		return strings.Compare(a.Dcid, b.Dcid)
	})
	slices.SortFunc(resp.Variables, func(a, b *VariableCandidate) int {
		return strings.Compare(a.Dcid, b.Dcid)
	})

	return resp
}

// populatePlaceMetadata aggregates place information and registers parent place structures.
func populatePlaceMetadata(
	resp *SearchIndicatorsResponse,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
) {
	if resp.DcidNameMappings == nil {
		resp.DcidNameMappings = make(map[string]string)
	}
	if resp.DcidPlaceTypeMappings == nil {
		resp.DcidPlaceTypeMappings = make(map[string][]string)
	}

	for _, info := range resolvedPlaces {
		resp.DcidNameMappings[info.Dcid] = info.Name
		resp.DcidPlaceTypeMappings[info.Dcid] = util.NonNilStringSlice(info.TypeOf)

		if info.Dcid == parentPlaceDcid {
			resp.ResolvedParentPlace = &ResolvedPlace{
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
	resp *SearchIndicatorsResponse,
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

	resp.Topics = append(resp.Topics, &TopicCandidate{
		Dcid:                  c.GetDcid(),
		MemberTopics:          memberTopics,
		MemberVariables:       memberVariables,
		PlacesWithData:        placesWithData,
		Description:           c.GetName(),
		AlternateDescriptions: []string{c.GetName()}, // Fallback alt description
	})
}

// translateVariableCandidate converts a variable candidate and maps its name.
func translateVariableCandidate(
	c *pbv2.ResolveResponse_Entity_Candidate,
	placesWithData []string,
	resp *SearchIndicatorsResponse,
) {
	resp.Variables = append(resp.Variables, &VariableCandidate{
		Dcid:                  c.GetDcid(),
		PlacesWithData:        placesWithData,
		ObservationProperties: c.GetObservationProperties(),
	})
}

// enrichPlaceNamesAndTypes retrieves and populates the canonical names and types of resolved places.
func (s *Service) enrichPlaceNamesAndTypes(
	ctx context.Context,
	resolvedMap map[string]*resolvedPlaceInfo,
) {
	defer util.TimeTrack(time.Now(), "Agent: enrichPlaceNamesAndTypes")
	var dcids []string
	for _, info := range resolvedMap {
		dcids = append(dcids, info.Dcid)
	}
	slog.Info("Enriching resolved place names and types via V2Node", "placesCount", len(dcids))

	if len(dcids) == 0 {
		return
	}

	nodeReq := &pbv2.NodeRequest{
		Nodes:    dcids,
		Property: "->[name, typeOf]",
	}
	if nodeResp, err := s.fetchAllNodes(ctx, nodeReq); err == nil && nodeResp != nil && nodeResp.GetData() != nil {
		for _, info := range resolvedMap {
			if nodeData, ok := nodeResp.GetData()[info.Dcid]; ok {
				if name := getPropValue(nodeData, "name"); name != "" {
					info.Name = name
				}
				if types := getPropDcids(nodeData, "typeOf"); len(types) > 0 {
					info.TypeOf = types
				}
			}
		}
	}
}
