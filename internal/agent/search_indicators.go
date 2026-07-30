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
	"google.golang.org/protobuf/types/known/structpb"
)

// SearchIndicators resolves explicit topics and variables matching a query,
// filtering out indicators that lack observation data for target places.
func (s *Service) SearchIndicators(
	ctx context.Context,
	req *pbv2.SearchIndicatorsRequest,
) (*pbv2.SearchIndicatorsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	defer util.TimeTrack(time.Now(), "Agent: SearchIndicators")
	slog.Info("SearchIndicators started", "query", req.GetQuery(), "places", req.GetPlaces(), "placeDcids", req.GetPlaceDcids(), "parentPlace", req.GetParentPlace())

	// Validate request parameters
	if err := validateRequest(req); err != nil {
		return nil, err
	}

	// Determine final limits: if empty, default to 10
	limit := req.GetPerSearchLimit()
	if limit <= 0 {
		limit = DefaultSearchLimit
	}

	// Determine include_topics default: true unless explicitly set to false
	includeTopics := true
	if req.IncludeTopics != nil {
		includeTopics = req.GetIncludeTopics()
	}

	// Determine expand_topics default: true unless explicitly set to false
	expandTopics := true
	if req.ExpandTopics != nil {
		expandTopics = req.GetExpandTopics()
	}

	var placeDcids []string
	var resolvedPlaces map[string]*resolvedPlaceInfo
	var parentPlaceDcid string
	var candidates []*pbv2.ResolveResponse_Entity_Candidate

	// Concurrent Place Acquisition & Candidate Search
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		placeDcids, resolvedPlaces, parentPlaceDcid, err = s.acquirePlaceDcids(gCtx, req)
		return err
	})

	g.Go(func() error {
		var err error
		candidates, err = s.fetchAndProcessCandidates(gCtx, req.GetQuery(), limit, includeTopics, expandTopics, req.GetTarget())
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Concurrent Filtering & Legacy Place Enrichment
	candidates, err := s.filterAndEnrichCandidates(ctx, candidates, placeDcids, resolvedPlaces, req.GetTarget())
	if err != nil {
		return nil, err
	}

	// Tabular Response Assembly
	return s.translateToTabularResponse(candidates, resolvedPlaces, parentPlaceDcid, limit)
}

// acquirePlaceDcids resolves place names or returns explicit place DCIDs.
func (s *Service) acquirePlaceDcids(
	ctx context.Context,
	req *pbv2.SearchIndicatorsRequest,
) ([]string, map[string]*resolvedPlaceInfo, string, error) {
	if len(req.GetPlaceDcids()) > 0 {
		return req.GetPlaceDcids(), nil, "", nil
	}
	if len(req.GetPlaces()) > 0 || req.GetParentPlace() != "" {
		resolvedMap, parentDcid, err := s.resolvePlaces(ctx, req.GetPlaces(), req.GetParentPlace(), req.GetTarget())
		if err != nil {
			return nil, nil, "", err
		}
		var dcids []string
		for _, info := range resolvedMap {
			dcids = append(dcids, info.Dcid)
		}
		return dcids, resolvedMap, parentDcid, nil
	}
	if req.GetQuery() == "" {
		return []string{DefaultPlaceDcidWorld}, nil, "", nil
	}
	return nil, nil, "", nil
}

// fetchAndProcessCandidates executes vector search and applies topic expansion options.
func (s *Service) fetchAndProcessCandidates(
	ctx context.Context,
	query string,
	limit int32,
	includeTopics bool,
	expandTopics bool,
	target string,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	oversampledLimit := limit * 2
	candidates, err := s.fetchCandidates(ctx, query, oversampledLimit, expandTopics, target)
	if err != nil {
		return nil, err
	}
	if !includeTopics {
		candidates = expandTopicCandidates(candidates)
	}
	return candidates, nil
}

// filterAndEnrichCandidates concurrently filters candidates by place existence and enriches resolved place metadata.
func (s *Service) filterAndEnrichCandidates(
	ctx context.Context,
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	placeDcids []string,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	target string,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	g, gCtx := errgroup.WithContext(ctx)

	var filteredCandidates []*pbv2.ResolveResponse_Entity_Candidate
	g.Go(func() error {
		if len(placeDcids) > 0 {
			var err error
			filteredCandidates, err = s.filterByPlaceExistence(gCtx, candidates, placeDcids, target)
			return err
		}
		filteredCandidates = candidates
		return nil
	})

	g.Go(func() error {
		if resolvedPlaces != nil {
			s.enrichPlaceNamesAndTypes(gCtx, resolvedPlaces)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return filteredCandidates, nil
}

// validateRequest checks the validity of the incoming request constraints.
func validateRequest(req *pbv2.SearchIndicatorsRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	limit := req.GetPerSearchLimit()
	if limit < MinSearchLimit || limit > MaxSearchLimit {
		return status.Errorf(codes.InvalidArgument, "per_search_limit must be between %d and %d, got: %d", MinSearchLimit, MaxSearchLimit, limit)
	}
	if req.GetParentPlace() != "" {
		if len(req.GetPlaces()) == 0 {
			return status.Errorf(codes.InvalidArgument, "places must be specified when parent_place is provided")
		}
		if len(req.GetPlaceDcids()) > 0 {
			return status.Errorf(codes.InvalidArgument, "parent_place cannot be specified alongside place_dcids")
		}
	}
	if target := req.GetTarget(); target != "" && !slices.Contains(validTargets, target) {
		return status.Errorf(codes.InvalidArgument, "invalid target: %s, valid values are '%s'", target, strings.Join(validTargets, "', '"))
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

// fetchCandidates calls V2Resolve to execute embeddings similarity searches.
func (s *Service) fetchCandidates(
	ctx context.Context,
	query string,
	limit int32,
	expandTopics bool,
	target string,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	defer util.TimeTrack(time.Now(), "Agent: fetchCandidates")
	// Map empty queries to browse root topics
	var nodes []string
	if query != "" {
		nodes = []string{query}
	}

	resolver := ResolverIndicator
	if query == "" {
		resolver = ResolverTopic
	}

	resolveReq := &pbv2.ResolveRequest{
		Nodes:        nodes,
		Resolver:     resolver,
		ExpandTopics: expandTopics,
		Target:       target,
	}

	resp, err := s.mixer.V2Resolve(ctx, resolveReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to execute embeddings search: %v", err)
	}
	if resp == nil {
		return nil, status.Error(codes.Internal, "received nil response from V2Resolve")
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

// collectSubtopicsAndDirectVars separates subtopic DCIDs and direct variable DCIDs across candidates.
func collectSubtopicsAndDirectVars(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
) ([]string, []string) {
	var topicDcids []string
	var directVarDcids []string
	seenTopics := make(map[string]bool)
	seenVars := make(map[string]bool)

	for _, c := range candidates {
		if isTopic(c) {
			if !seenTopics[c.GetDcid()] {
				seenTopics[c.GetDcid()] = true
				topicDcids = append(topicDcids, c.GetDcid())
			}
			for _, child := range c.GetChildren() {
				if isTopic(child) {
					if !seenTopics[child.GetDcid()] {
						seenTopics[child.GetDcid()] = true
						topicDcids = append(topicDcids, child.GetDcid())
					}
				} else {
					if !seenVars[child.GetDcid()] {
						seenVars[child.GetDcid()] = true
						directVarDcids = append(directVarDcids, child.GetDcid())
					}
				}
			}
		} else {
			if !seenVars[c.GetDcid()] {
				seenVars[c.GetDcid()] = true
				directVarDcids = append(directVarDcids, c.GetDcid())
			}
		}
	}
	return topicDcids, directVarDcids
}

// gatherVarsToCheck aggregates direct variables and all subtopic descendant variables.
func gatherVarsToCheck(
	directVarDcids []string,
	subtopicDescendants map[string][]string,
) []string {
	var vars []string
	seen := make(map[string]bool)
	for _, v := range directVarDcids {
		if !seen[v] {
			seen[v] = true
			vars = append(vars, v)
		}
	}
	for _, descendantVars := range subtopicDescendants {
		for _, v := range descendantVars {
			if !seen[v] {
				seen[v] = true
				vars = append(vars, v)
			}
		}
	}
	return vars
}

// hasPlacesWithData checks if any descendant variable has data for any target place.
func hasPlacesWithData(
	descendants []string,
	placeDcids []string,
	availabilityMap map[string]map[string]bool,
) []string {
	var places []string
	for _, p := range placeDcids {
		placeMap, ok := availabilityMap[p]
		if !ok || placeMap == nil {
			continue
		}
		for _, v := range descendants {
			if placeMap[v] {
				places = append(places, p)
				break
			}
		}
	}
	slices.Sort(places)
	return places
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

// placesWithDataForVar returns a sorted list of places where the variable has data.
func placesWithDataForVar(
	varDcid string,
	placeDcids []string,
	availabilityMap map[string]map[string]bool,
) []string {
	var places []string
	for _, p := range placeDcids {
		placeMap, ok := availabilityMap[p]
		if ok && placeMap != nil && placeMap[varDcid] {
			places = append(places, p)
		}
	}
	slices.Sort(places)
	return places
}

// setPlacesWithDataMetadata sets the metadata field for places with data.
func setPlacesWithDataMetadata(c *pbv2.ResolveResponse_Entity_Candidate, places []string) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[MetadataPlacesWithData] = strings.Join(places, DcidSeparator)
}

func (s *Service) fetchDescendantVariables(
	ctx context.Context,
	topicDcids []string,
	target string,
) (map[string][]string, error) {
	defer util.TimeTrack(time.Now(), "Agent: fetchDescendantVariables")
	if len(topicDcids) == 0 {
		return nil, nil
	}

	resolveReq := &pbv2.ResolveRequest{
		Nodes:        topicDcids,
		Resolver:     ResolverTopic,
		ExpandTopics: true,
		Target:       target,
	}

	resp, err := s.mixer.V2Resolve(ctx, resolveReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch descendant variables for topics: %v", err)
	}
	if resp == nil {
		return nil, status.Error(codes.Internal, "received nil response from V2Resolve")
	}

	res := make(map[string][]string)
	for _, entity := range resp.GetEntities() {
		node := entity.GetNode()
		if node == "" {
			// Root topics expansion
			for _, cand := range entity.GetCandidates() {
				var vars []string
				for _, child := range cand.GetChildren() {
					vars = append(vars, child.GetDcid())
				}
				res[cand.GetDcid()] = vars
			}
		} else {
			// Specific topic expansion
			var vars []string
			for _, cand := range entity.GetCandidates() {
				for _, child := range cand.GetChildren() {
					vars = append(vars, child.GetDcid())
				}
			}
			res[node] = vars
		}
	}
	return res, nil
}

// buildTopicsTable formats topic candidates into a tabular Table proto payload.
func buildTopicsTable(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	limit int32,
) (*pbv2.Table, error) {
	table := &pbv2.Table{
		Columns: []string{colDcid, colName, colPlacesWithData, colMemberTopics, colMemberVariables},
		Rows:    []*structpb.ListValue{},
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
		if pStr, ok := c.Metadata[MetadataPlacesWithData]; ok && pStr != "" {
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
		placesList := util.ToStringListValue(placesWithData)
		topicsList := util.ToStringListValue(memberTopics)
		varsList := util.ToStringListValue(memberVariables)

		row, err := structpb.NewList([]any{
			c.GetDcid(),
			c.GetName(),
			placesList.AsSlice(),
			topicsList.AsSlice(),
			varsList.AsSlice(),
		})
		if err != nil {
			return nil, err
		}
		table.Rows = append(table.Rows, row)
		topicCount++
	}
	return table, nil
}

// buildVariablesTable formats variable candidates into a tabular Table proto payload.
func buildVariablesTable(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	limit int32,
) (*pbv2.Table, error) {
	table := &pbv2.Table{
		Columns: []string{colDcid, colName, colPlacesWithData, colObservationProperties},
		Rows:    []*structpb.ListValue{},
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
		if pStr, ok := c.Metadata[MetadataPlacesWithData]; ok && pStr != "" {
			placesWithData = strings.Split(pStr, DcidSeparator)
		}
		placesList := util.ToStringListValue(placesWithData)
		obsPropsList := util.ToStringListValue(c.GetObservationProperties())

		row, err := structpb.NewList([]any{
			c.GetDcid(),
			c.GetName(),
			placesList.AsSlice(),
			obsPropsList.AsSlice(),
		})
		if err != nil {
			return nil, err
		}
		table.Rows = append(table.Rows, row)
		varCount++
	}
	return table, nil
}

// translateToTabularResponse assembles the tabular SearchIndicatorsResponse payload.
func (s *Service) translateToTabularResponse(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
	limit int32,
) (*pbv2.SearchIndicatorsResponse, error) {
	topicsTable, err := buildTopicsTable(candidates, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build topics table: %v", err)
	}
	variablesTable, err := buildVariablesTable(candidates, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build variables table: %v", err)
	}

	resp := &pbv2.SearchIndicatorsResponse{
		Status:    StatusSuccess,
		Topics:    topicsTable,
		Variables: variablesTable,
	}

	if resolvedPlaces != nil {
		populatePlaceMetadata(resp, resolvedPlaces, parentPlaceDcid)
	}

	return resp, nil
}

// populatePlaceMetadata aggregates place information and registers parent place structures.
func populatePlaceMetadata(
	resp *pbv2.SearchIndicatorsResponse,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
) {
	if resp.DcidNameMappings == nil {
		resp.DcidNameMappings = make(map[string]string)
	}
	if resp.DcidPlaceTypeMappings == nil {
		resp.DcidPlaceTypeMappings = make(map[string]*structpb.ListValue)
	}

	for _, info := range resolvedPlaces {
		resp.DcidNameMappings[info.Dcid] = info.Name

		resp.DcidPlaceTypeMappings[info.Dcid] = util.ToStringListValue(info.TypeOf)

		if info.Dcid == parentPlaceDcid {
			resp.ResolvedParentPlace = &pbv2.SearchIndicatorsResponse_ResolvedPlace{
				Dcid:   info.Dcid,
				Name:   info.Name,
				TypeOf: info.TypeOf,
			}
		}
	}
}

// isTopic returns true if the candidate is classified as a Topic in TypeOf or DominantType.
func isTopic(c *pbv2.ResolveResponse_Entity_Candidate) bool {
	if c.GetDominantType() == DcidTypeTopic {
		return true
	}
	return slices.Contains(c.GetTypeOf(), DcidTypeTopic)
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
	if nodeResp, err := s.mixer.V2Node(ctx, nodeReq); err == nil && nodeResp != nil && nodeResp.GetData() != nil {
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
