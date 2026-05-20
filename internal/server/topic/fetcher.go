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
// This file (fetcher.go) isolates database fetching, inline SVPG expansion, parsing helpers,
// and graph processing algorithms.
package topic

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
)

// FetchTopicsFromKG fetches all topics and their relevant variables from the KG, builds the hierarchy, detects roots, and returns a TopicHierarchy.
func (m *TopicCacheManager) FetchTopicsFromKG(ctx context.Context) (*pb.TopicHierarchy, error) {
	defer util.TimeTrack(time.Now(), "topic: fetchTopicsFromKG")
	// Fetch core topic nodes, populating their DCID and Name.
	topics, err := m.fetchTopicNodes(ctx)
	if err != nil {
		return nil, err
	}

	if len(topics) == 0 {
		return &pb.TopicHierarchy{
			Topics: make(map[string]*pb.TopicNode),
		}, nil
	}

	// Collect topic DCIDs
	topicDcids := make([]string, 0, len(topics))
	for dcid := range topics {
		topicDcids = append(topicDcids, dcid)
	}

	// Batch-fetch variables and sub-topics for all topics in one call.
	relevantVarsMap, err := m.fetchRelevantVariables(ctx, topicDcids)
	if err != nil {
		return nil, err
	}

	// Link variables/sub-topics to their parent topics
	for dcid, relevantVars := range relevantVarsMap {
		if t, ok := topics[dcid]; ok {
			t.RelevantVariables = relevantVars
		}
	}

	// Build the hierarchy and automatically discover root topics
	hierarchy := m.buildHierarchy(topics)

	return hierarchy, nil
}

// parseStatVarInfo extracts metadata properties from a LinkedGraph and populates a StatVarInfo struct.
func parseStatVarInfo(dcid string, graph *pbv2.LinkedGraph) *StatVarInfo {
	info := &StatVarInfo{Dcid: dcid}
	if graph == nil || graph.GetArcs() == nil {
		return info
	}
	if arc, exists := graph.GetArcs()["name"]; exists && len(arc.GetNodes()) > 0 {
		info.Name = extractName(arc.GetNodes()[0])
	}
	info.ObservationProperties = extractValues(graph.GetArcs()["observationProperty"])
	info.EntityMappings = extractValues(graph.GetArcs()["entityMapping"])
	return info
}

// parseStatVarInfos populates metadata for all requested DCIDs from a NodeResponse.
func parseStatVarInfos(dcids []string, resp *pbv2.NodeResponse) map[string]*StatVarInfo {
	res := make(map[string]*StatVarInfo, len(dcids))
	for _, dcid := range dcids {
		res[dcid] = &StatVarInfo{Dcid: dcid}
	}
	if resp == nil || resp.GetData() == nil {
		return res
	}
	for dcid, graph := range resp.GetData() {
		res[dcid] = parseStatVarInfo(dcid, graph)
	}
	return res
}

// fetchStatVarInfos queries the Knowledge Graph for name, observationProperty, and entityMapping.
func (m *TopicCacheManager) fetchStatVarInfos(ctx context.Context, dcids []string) (map[string]*StatVarInfo, error) {
	defer util.TimeTrack(time.Now(), "topic: fetchStatVarInfos")
	req := &pbv2.NodeRequest{
		Nodes:    dcids,
		Property: "->[name, observationProperty, entityMapping]",
	}
	resp, err := m.fetcher.NodeFetchAll(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch stat var infos: %w", err)
	}

	return parseStatVarInfos(dcids, resp), nil
}

// extractName attempts to extract a name for the entity.
// It prioritizes Name, then Value, and falls back to Dcid.
func extractName(entity *pb.EntityInfo) string {
	if entity == nil {
		return ""
	}
	if name := entity.GetName(); name != "" {
		return name
	}
	if value := entity.GetValue(); value != "" {
		return value
	}
	return entity.GetDcid()
}

// extractValues iterates through nodes in an arc, extracting values or DCIDs as a string slice.
func extractValues(arc *pbv2.Nodes) []string {
	if arc == nil {
		return nil
	}
	var values []string
	for _, n := range arc.GetNodes() {
		if val := n.GetValue(); val != "" {
			values = append(values, val)
		} else if d := n.GetDcid(); d != "" {
			values = append(values, d)
		}
	}
	return values
}

// fetchTopicNodes fetches all topic nodes from the KG.
// It returns a map of DCID to newly instantiated TopicNode with Dcid and Name populated.
func (m *TopicCacheManager) fetchTopicNodes(ctx context.Context) (map[string]*pb.TopicNode, error) {
	defer util.TimeTrack(time.Now(), "topic: fetchTopicNodes")
	req := &pbv2.NodeRequest{
		Nodes:    []string{"Topic"},
		Property: "<-typeOf",
	}
	resp, err := m.fetcher.NodeFetchAll(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic nodes: %w", err)
	}

	topics := make(map[string]*pb.TopicNode)
	graph, ok := resp.GetData()["Topic"]
	if !ok {
		return topics, nil
	}

	nodes, ok := graph.GetArcs()["typeOf"]
	if !ok {
		return topics, nil
	}

	for _, n := range nodes.GetNodes() {
		dcid := n.GetDcid()
		if dcid == "" {
			continue
		}
		topics[dcid] = &pb.TopicNode{
			Dcid: dcid,
			Name: extractName(n),
		}
	}

	slog.Info("fetchTopicNodes query completed", "topicCount", len(topics))
	return topics, nil
}

// parseCommaSeparatedList splits a comma-separated string value, trimming whitespace and filtering empty elements.
func parseCommaSeparatedList(val string) []string {
	var list []string
	for _, item := range strings.Split(val, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			list = append(list, item)
		}
	}
	return list
}

// TODO: (nick-nlb) combine with checks in internal/util/util.go

// isSvpgDcid checks if the DCID belongs to a Stat Var Peer Group (SVPG).
func isSvpgDcid(dcid string) bool {
	return strings.Contains(dcid, "/svpg/")
}

// isTopicDcid checks if the DCID belongs to a Topic.
func isTopicDcid(dcid string) bool {
	return strings.Contains(dcid, "/topic/")
}

// parseTopicMembers parses the relevantVariableList arcs, extracting children and collecting referenced SVPGs.
func parseTopicMembers(nodes *pbv2.Nodes, svpgSet map[string]struct{}) []string {
	seen := make(map[string]struct{})
	var children []string
	for _, n := range nodes.GetNodes() {
		if val := n.GetValue(); val != "" {
			for _, childDcid := range parseCommaSeparatedList(val) {
				if _, exists := seen[childDcid]; !exists {
					seen[childDcid] = struct{}{}
					children = append(children, childDcid)
					if isSvpgDcid(childDcid) {
						svpgSet[childDcid] = struct{}{}
					}
				}
			}
		}
	}
	return children
}

// parseSvpgMembers parses the memberList arcs, extracting the direct member variables.
func parseSvpgMembers(nodes *pbv2.Nodes) []string {
	seen := make(map[string]struct{})
	var members []string
	for _, n := range nodes.GetNodes() {
		if val := n.GetValue(); val != "" {
			for _, memberDcid := range parseCommaSeparatedList(val) {
				if _, exists := seen[memberDcid]; !exists {
					seen[memberDcid] = struct{}{}
					members = append(members, memberDcid)
				}
			}
		}
	}
	return members
}

// expandTopicMembers expands referenced SVPG member variables in-memory under each topic, maintaining curated order.
func expandTopicMembers(topicToChildren map[string][]string, svpgToMembers map[string][]string) map[string][]string {
	relevantVarsMap := make(map[string][]string)
	for topicDcid, children := range topicToChildren {
		var expanded []string
		seen := make(map[string]struct{})
		for _, childDcid := range children {
			if isSvpgDcid(childDcid) {
				// Expand SVPG members directly inline
				if members, ok := svpgToMembers[childDcid]; ok {
					for _, m := range members {
						if _, exists := seen[m]; !exists {
							seen[m] = struct{}{}
							expanded = append(expanded, m)
						}
					}
				} else {
					slog.Warn("Referenced SVPG not found in datasource; dropping from topic", "svpgDcid", childDcid, "topicDcid", topicDcid)
				}
			} else {
				if _, exists := seen[childDcid]; !exists {
					seen[childDcid] = struct{}{}
					expanded = append(expanded, childDcid)
				}
			}
		}
		relevantVarsMap[topicDcid] = expanded
	}
	return relevantVarsMap
}

// fetchTopicMembers batches the datasource requests to fetch Topic children (relevantVariableList).
// It populates topicToChildren mapping and registers linked SVPGs in svpgSet.
func (m *TopicCacheManager) fetchTopicMembers(ctx context.Context, topicDcids []string, topicToChildren map[string][]string, svpgSet map[string]struct{}) error {
	req := &pbv2.NodeRequest{
		Nodes:    topicDcids,
		Property: "->relevantVariableList",
	}
	resp, err := m.fetcher.NodeFetchAll(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to fetch topic properties: %w", err)
	}

	for dcid, graph := range resp.GetData() {
		if nodes, ok := graph.GetArcs()["relevantVariableList"]; ok {
			topicToChildren[dcid] = parseTopicMembers(nodes, svpgSet)
		}
	}
	return nil
}

// fetchSvpgMembers batches the datasource requests to fetch SVPG direct member variables (memberList).
// It populates svpgToMembers mapping.
func (m *TopicCacheManager) fetchSvpgMembers(ctx context.Context, svpgDcids []string, svpgToMembers map[string][]string) error {
	req := &pbv2.NodeRequest{
		Nodes:    svpgDcids,
		Property: "->memberList",
	}
	resp, err := m.fetcher.NodeFetchAll(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to fetch svpg properties: %w", err)
	}

	for dcid, graph := range resp.GetData() {
		if nodes, ok := graph.GetArcs()["memberList"]; ok {
			svpgToMembers[dcid] = parseSvpgMembers(nodes)
		}
	}
	return nil
}

// fetchRelevantVariables fetches and expands all relevant variables for the specified topic DCIDs.
// It makes two separate, explicit batch calls: first for Topics, then for referenced SVPGs.
func (m *TopicCacheManager) fetchRelevantVariables(ctx context.Context, topicDcids []string) (map[string][]string, error) {
	defer util.TimeTrack(time.Now(), "topic: fetchRelevantVariables")
	if len(topicDcids) == 0 {
		return nil, nil
	}

	topicToChildren := make(map[string][]string)
	svpgSet := make(map[string]struct{})

	// Fetch Topic relations
	if err := m.fetchTopicMembers(ctx, topicDcids, topicToChildren, svpgSet); err != nil {
		return nil, err
	}

	if len(svpgSet) == 0 {
		// No SVPGs to expand, return basic expansion directly
		return expandTopicMembers(topicToChildren, nil), nil
	}

	// Fetch SVPG member lists
	svpgDcids := util.StringSetToSlice(svpgSet)

	svpgToMembers := make(map[string][]string)
	if err := m.fetchSvpgMembers(ctx, svpgDcids, svpgToMembers); err != nil {
		return nil, err
	}

	// Perform in-memory inline expansion of SVPGs
	return expandTopicMembers(topicToChildren, svpgToMembers), nil
}

// buildHierarchy processes the raw topics map, tracks parent-child relationships,
// identifies root topics, and returns a populated TopicHierarchy.
func (m *TopicCacheManager) buildHierarchy(topics map[string]*pb.TopicNode) *pb.TopicHierarchy {
	defer util.TimeTrack(time.Now(), "topic: buildHierarchy")

	// Set of all topics that are referenced as a child in any topic
	childTopicsSet := make(map[string]struct{})
	for _, t := range topics {
		for _, childDcid := range t.RelevantVariables {
			if isTopicDcid(childDcid) {
				childTopicsSet[childDcid] = struct{}{}
			}
		}
	}

	// A topic is a root topic if it is never referenced as a child
	var roots []string
	rootsMap := make(map[string]struct{})
	for dcid := range topics {
		if _, isChild := childTopicsSet[dcid]; !isChild {
			roots = append(roots, dcid)
			rootsMap[dcid] = struct{}{}
		}
	}

	// Fallback Hack: If there are too many roots, filter the detected roots down to only the curated default roots
	// that actually exist in the detected roots list. Log a warning instructing KG curation to be trimmed.
	if len(roots) > rootTopicThreshold {
		var fallbackRoots []string
		for _, defaultRoot := range defaultRootTopicDcids {
			if _, exists := rootsMap[defaultRoot]; exists {
				fallbackRoots = append(fallbackRoots, defaultRoot)
			}
		}

		if len(fallbackRoots) > 0 {
			slog.Warn(
				"Detected excessive number of root topics, falling back to curated default roots. Please trim the number of roots in the KG.",
				"detectedRootCount", len(roots),
				"fallbackRoots", fallbackRoots,
			)
			roots = fallbackRoots
		}
	}

	// Sort root DCIDs alphabetically for determinism
	sort.Strings(roots)

	slog.Info("Topic hierarchy built", "totalTopics", len(topics), "rootCount", len(roots))
	return &pb.TopicHierarchy{
		Topics:         topics,
		RootTopicDcids: roots,
	}
}
