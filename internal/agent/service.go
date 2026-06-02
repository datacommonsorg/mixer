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
	slog.Info("SearchIndicators started", "query", req.GetQuery(), "places", req.GetPlaces(), "parentPlace", req.GetParentPlace())

	// Validate request parameters
	if err := validateRequest(req); err != nil {
		return nil, err
	}

	// Set default World query place if query is empty and no places are specified
	places := req.GetPlaces()
	if req.GetQuery() == "" && len(places) == 0 && req.GetParentPlace() == "" {
		places = []string{DefaultPlaceWorld}
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

	// Phase 1: Resolve place names and fetch similarity candidates in parallel
	var resolvedPlaces map[string]*resolvedPlaceInfo
	var parentPlaceDcid string
	var candidates []*pbv2.ResolveResponse_Entity_Candidate

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		resolvedPlaces, parentPlaceDcid, err = s.resolvePlaces(gCtx, places, req.GetParentPlace())
		return err
	})

	g.Go(func() error {
		var err error
		oversampledLimit := limit * 2
		// Fetch candidates using embeddings search. Leaf expansion is enabled depending on expandTopics.
		candidates, err = s.fetchCandidates(gCtx, req.GetQuery(), oversampledLimit, expandTopics)
		if err != nil {
			return err
		}
		// If includeTopics is false, expand and deduplicate topic candidates into standard variables
		if !includeTopics {
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
		var placeDcids []string
		for _, info := range resolvedPlaces {
			placeDcids = append(placeDcids, info.Dcid)
		}
		if len(placeDcids) > 0 {
			var err error
			candidates, err = s.filterByPlaceExistence(g2Ctx, candidates, placeDcids)
			return err
		}
		return nil
	})

	// Goroutine B: Fetch and enrich names and types of resolved places in parallel
	g2.Go(func() error {
		s.enrichPlaceNamesAndTypes(g2Ctx, resolvedPlaces)
		return nil
	})

	if err := g2.Wait(); err != nil {
		return nil, err
	}

	// Assemble and format unified response payload with late truncation limits
	return s.translateToResponse(candidates, resolvedPlaces, parentPlaceDcid, limit)
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
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	defer util.TimeTrack(time.Now(), "Agent: filterByPlaceExistence")
	slog.Info("Filtering indicators by place existence", "candidatesCount", len(candidates), "placesCount", len(placeDcids))

	if len(placeDcids) == 0 {
		return candidates, nil
	}

	// Collect all topic DCIDs (parents & child subtopics) and direct variables across all candidates
	topicDcids, directVarDcids := collectSubtopicsAndDirectVars(candidates)

	// Fetch descendant variables of all topics concurrently in a single batch
	topicDescendants, err := s.fetchDescendantVariables(ctx, topicDcids)
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
	for _, c := range candidates {
		if isTopic(c) {
			topicDcids = append(topicDcids, c.GetDcid())
			for _, child := range c.GetChildren() {
				if isTopic(child) {
					topicDcids = append(topicDcids, child.GetDcid())
				} else {
					directVarDcids = append(directVarDcids, child.GetDcid())
				}
			}
		} else {
			directVarDcids = append(directVarDcids, c.GetDcid())
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
	vars = append(vars, directVarDcids...)
	for _, descendantVars := range subtopicDescendants {
		vars = append(vars, descendantVars...)
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
) (map[string][]string, error) {
	defer util.TimeTrack(time.Now(), "Agent: fetchDescendantVariables")
	if len(topicDcids) == 0 {
		return nil, nil
	}

	resolveReq := &pbv2.ResolveRequest{
		Nodes:        topicDcids,
		Resolver:     ResolverTopic,
		ExpandTopics: true,
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

// translateToResponse formats and translates candidate lists into the final RPC payload.
func (s *Service) translateToResponse(
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	resolvedPlaces map[string]*resolvedPlaceInfo,
	parentPlaceDcid string,
	limit int32,
) (*pbv2.SearchIndicatorsResponse, error) {
	resp := &pbv2.SearchIndicatorsResponse{
		Status:                StatusSuccess,
		DcidNameMappings:      make(map[string]string),
		DcidPlaceTypeMappings: make(map[string]*structpb.ListValue),
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
	resp.DcidPlaceTypeMappings = make(map[string]*structpb.ListValue)

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

// getPropValue extracts the first string value of a property from a LinkedGraph.
func getPropValue(graph *pbv2.LinkedGraph, prop string) string {
	if graph == nil || graph.Arcs == nil {
		return ""
	}
	if nodes, ok := graph.Arcs[prop]; ok && nodes != nil && len(nodes.GetNodes()) > 0 {
		return nodes.GetNodes()[0].GetValue()
	}
	return ""
}

// getPropDcids extracts all node DCIDs of a property from a LinkedGraph.
func getPropDcids(graph *pbv2.LinkedGraph, prop string) []string {
	if graph == nil || graph.Arcs == nil {
		return nil
	}
	var res []string
	if nodes, ok := graph.Arcs[prop]; ok && nodes != nil {
		for _, node := range nodes.GetNodes() {
			if dcid := node.GetDcid(); dcid != "" {
				res = append(res, dcid)
			}
		}
	}
	return res
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
