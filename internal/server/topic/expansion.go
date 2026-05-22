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

// Package topic manages the in-memory and Redis caching of Knowledge Graph topic hierarchies.
// This file (expansion.go) implements topic tree navigation and candidate expansion algorithms.
package topic

import (
	"context"
	"fmt"
	"log/slog"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// newCandidate creates a base candidate with standard type identification.
func newCandidate(dcid, entityType string) *pbv2.ResolveResponse_Entity_Candidate {
	return &pbv2.ResolveResponse_Entity_Candidate{
		Dcid:   dcid,
		TypeOf: []string{entityType},
	}
}

// newTopicCandidate constructs a ResolveResponse_Entity_Candidate for a Topic.
func newTopicCandidate(dcid, name string) *pbv2.ResolveResponse_Entity_Candidate {
	cand := newCandidate(dcid, "Topic")
	cand.Name = name
	return cand
}

// newSVCandidate constructs a ResolveResponse_Entity_Candidate for a Statistical Variable.
func newSVCandidate(dcid string, info *StatVarInfo) *pbv2.ResolveResponse_Entity_Candidate {
	cand := newCandidate(dcid, "StatisticalVariable")
	if info != nil {
		cand.Name = info.Name
		cand.ObservationProperties = info.ObservationProperties
	}
	return cand
}

// ExpandRoots returns top-level root topic candidates.
// If expandTopics is true, it fully expands descendant Statistical Variables under each root topic.
// If expandTopics is false, it populates each root topic with its immediate children (sub-topics and SVs).
func (m *TopicCacheManager) ExpandRoots(ctx context.Context, expandTopics bool) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	h, _ := m.GetHierarchy(ctx)
	if h == nil || len(h.GetRootTopicDcids()) == 0 {
		return nil, nil
	}

	var candidates []*pbv2.ResolveResponse_Entity_Candidate
	for _, rootDcid := range h.GetRootTopicDcids() {
		var name string
		if node, ok := h.GetTopics()[rootDcid]; ok && node != nil {
			name = node.GetName()
		}
		cand := newTopicCandidate(rootDcid, name)
		children, err := m.ExpandTopic(ctx, rootDcid, expandTopics)
		if err != nil {
			slog.Error("Failed to expand root topic during resolve", "root", rootDcid, "error", err)
		} else {
			cand.Children = children
		}
		candidates = append(candidates, cand)
	}
	return candidates, nil
}

// ExpandTopic resolves children for a given topic DCID.
func (m *TopicCacheManager) ExpandTopic(ctx context.Context, topicDcid string, expandTopics bool) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	h, _ := m.GetHierarchy(ctx)
	if h == nil || h.GetTopics() == nil {
		return nil, nil
	}

	allSvDcids := collectTopicSVs(h, topicDcid, expandTopics)

	// Batch load metadata for all SVs required in this view
	infos, err := m.GetStatVarInfos(ctx, allSvDcids)
	if err != nil {
		return nil, fmt.Errorf("failed to get statistical variable infos: %w", err)
	}

	var candidates []*pbv2.ResolveResponse_Entity_Candidate
	if expandTopics {
		// Flattened descendant SV list
		for _, svDcid := range allSvDcids {
			candidates = append(candidates, newSVCandidate(svDcid, infos[svDcid]))
		}
	} else {
		// Immediate members: Sub-Topics and SVs
		if node, ok := h.GetTopics()[topicDcid]; ok && node != nil {
			for _, childDcid := range node.GetRelevantVariables() {
				if isTopicDcid(childDcid) {
					var name string
					if childNode, childOk := h.GetTopics()[childDcid]; childOk && childNode != nil {
						name = childNode.GetName()
					}
					candidates = append(candidates, newTopicCandidate(childDcid, name))
				} else {
					candidates = append(candidates, newSVCandidate(childDcid, infos[childDcid]))
				}
			}
		}
	}

	return candidates, nil
}

// collectTopicSVs initializes seenTopics cycle tracking map and initiates traversal.
func collectTopicSVs(h *pb.TopicHierarchy, topicDcid string, expandTopics bool) []string {
	var res []string
	collectTopicSVsInternal(h, topicDcid, expandTopics, make(map[string]struct{}), &res)
	return res
}

// collectTopicSVsInternal performs traversal of a topic's hierarchy under seenTopics protection,
// collecting all resolved SV DCIDs inside the provided result slice in-place to prevent intermediate allocations.
func collectTopicSVsInternal(h *pb.TopicHierarchy, topicDcid string, expandTopics bool, seenTopics map[string]struct{}, res *[]string) {
	if _, seen := seenTopics[topicDcid]; seen {
		return
	}
	seenTopics[topicDcid] = struct{}{}

	if node, ok := h.GetTopics()[topicDcid]; ok && node != nil {
		for _, childDcid := range node.GetRelevantVariables() {
			if isTopicDcid(childDcid) {
				if expandTopics {
					collectTopicSVsInternal(h, childDcid, expandTopics, seenTopics, res)
				}
			} else {
				*res = append(*res, childDcid)
			}
		}
	}
}
