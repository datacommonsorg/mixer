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
	topics, vars := newDcidSet(), newDcidSet()
	for _, c := range candidates {
		if !isTopic(c) {
			vars.add(c.GetDcid())
			continue
		}
		topics.add(c.GetDcid())
		for _, child := range c.GetChildren() {
			if isTopic(child) {
				topics.add(child.GetDcid())
			} else {
				vars.add(child.GetDcid())
			}
		}
	}
	return topics.dcids, vars.dcids
}

// gatherVarsToCheck aggregates direct variables and all subtopic descendant variables.
func gatherVarsToCheck(
	directVarDcids []string,
	subtopicDescendants map[string][]string,
) []string {
	vars := newDcidSet()
	for _, v := range directVarDcids {
		vars.add(v)
	}
	for _, descendantVars := range subtopicDescendants {
		for _, v := range descendantVars {
			vars.add(v)
		}
	}
	return vars.dcids
}

// entitiesWithData returns, sorted, the entities with data for at least one of the variables.
func entitiesWithData(
	varDcids []string,
	entityDcids []string,
	availabilityMap map[string]map[string]bool,
) []string {
	var entities []string
	for _, e := range entityDcids {
		if slices.ContainsFunc(varDcids, func(v string) bool { return availabilityMap[e][v] }) {
			entities = append(entities, e)
		}
	}
	slices.Sort(entities)
	return entities
}

// fetchDescendantVariables resolves all descendant statistical variables for the given topic DCIDs, keyed by
// topic DCID (see fetchDescendantCandidates). V2Resolve errors are returned unchanged, as in fetchCandidates.
func (s *Service) fetchDescendantVariables(
	ctx context.Context,
	topicDcids []string,
	target string,
) (map[string][]string, error) {
	candidates, err := s.fetchDescendantCandidates(ctx, topicDcids, target)
	if err != nil {
		return nil, err
	}
	res := make(map[string][]string, len(candidates))
	for topicDcid, children := range candidates {
		res[topicDcid] = childDcids(children)
	}
	return res, nil
}

// fetchDescendantCandidates resolves the descendant statistical variable candidates (with their names) for
// the given topic DCIDs, keyed by topic DCID, in hierarchy order. The topic resolver returns one candidate per
// requested topic, with the topic's descendant variables as children, and none for unknown topics or topics
// without variables, which get no key. A variable listed under several sub-topics appears once per listing.
// V2Resolve errors are returned unchanged, as in fetchCandidates.
func (s *Service) fetchDescendantCandidates(
	ctx context.Context,
	topicDcids []string,
	target string,
) (map[string][]*pbv2.ResolveResponse_Entity_Candidate, error) {
	defer util.TimeTrack(time.Now(), "Agent: fetchDescendantCandidates")
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

	res := make(map[string][]*pbv2.ResolveResponse_Entity_Candidate)
	for _, entity := range resp.GetEntities() {
		for _, cand := range entity.GetCandidates() {
			res[cand.GetDcid()] = append(res[cand.GetDcid()], cand.GetChildren()...)
		}
	}
	return res, nil
}

// childDcids returns the DCIDs of the given candidates, in order.
func childDcids(candidates []*pbv2.ResolveResponse_Entity_Candidate) []string {
	var dcids []string
	for _, c := range candidates {
		dcids = append(dcids, c.GetDcid())
	}
	return dcids
}

// isTopic returns true if the candidate is classified as a Topic in TypeOf or DominantType.
func isTopic(c *pbv2.ResolveResponse_Entity_Candidate) bool {
	if c.GetDominantType() == DcidTypeTopic {
		return true
	}
	return slices.Contains(c.GetTypeOf(), DcidTypeTopic)
}
