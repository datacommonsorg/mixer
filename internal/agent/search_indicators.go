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

// This file holds search_indicators helpers shared by the v1 and v2 tools.

package agent

import (
	"context"
	"slices"
	"time"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fetchCandidates calls V2Resolve to execute embeddings similarity searches.
//
// V2Resolve errors are returned unchanged so each tool version can word them for its callers: v1 wraps them
// as Internal, while v2 maps the gRPC code to caller-facing guidance (see wrapV2BackendError).
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
		return nil, err
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

// hasPlacesWithData checks if any descendant variable has data for any target entity.
func hasPlacesWithData(
	descendants []string,
	entityDcids []string,
	availabilityMap map[string]map[string]bool,
) []string {
	var entities []string
	for _, e := range entityDcids {
		entityMap, ok := availabilityMap[e]
		if !ok || entityMap == nil {
			continue
		}
		for _, v := range descendants {
			if entityMap[v] {
				entities = append(entities, e)
				break
			}
		}
	}
	slices.Sort(entities)
	return entities
}

// placesWithDataForVar returns a sorted list of entities where the variable has data.
func placesWithDataForVar(
	varDcid string,
	entityDcids []string,
	availabilityMap map[string]map[string]bool,
) []string {
	var entities []string
	for _, e := range entityDcids {
		entityMap, ok := availabilityMap[e]
		if ok && entityMap != nil && entityMap[varDcid] {
			entities = append(entities, e)
		}
	}
	slices.Sort(entities)
	return entities
}

// fetchDescendantVariables resolves all descendant statistical variables for the given topic DCIDs.
// V2Resolve errors are returned unchanged, as in fetchCandidates.
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
		return nil, err
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

// isTopic returns true if the candidate is classified as a Topic in TypeOf or DominantType.
func isTopic(c *pbv2.ResolveResponse_Entity_Candidate) bool {
	if c.GetDominantType() == DcidTypeTopic {
		return true
	}
	return slices.Contains(c.GetTypeOf(), DcidTypeTopic)
}
