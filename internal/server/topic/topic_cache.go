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

package topic

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/redis"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	defaultPageSize      = 1000
	rootTopicThreshold   = 10
	defaultRootTopicDcid = "dc/topic/Root"
)

var (
	redisCacheKeyProto = &wrapperspb.StringValue{Value: "topic/topic_cache"}
)

// TopicVariableCache is an in-memory composite struct caching server-wide topic hierarchy
// and variable metadata.
type TopicVariableCache struct {
	TopicHierarchy *pbv2.TopicHierarchy
	// SVs map[string]*SVInfo // Placeholder for follow-up task
}

// TopicCacheManager manages the loading, building, and caching of topics.
type TopicCacheManager struct {
	ds          datasource.DataSource
	redisClient redis.CacheClient

	mu    sync.RWMutex
	cache *TopicVariableCache
}

// NewTopicCacheManager creates a new TopicCacheManager.
func NewTopicCacheManager(ds datasource.DataSource, redisClient redis.CacheClient) *TopicCacheManager {
	return &TopicCacheManager{
		ds:          ds,
		redisClient: redisClient,
	}
}

// GetHierarchy returns the currently cached TopicHierarchy in a thread-safe manner.
func (m *TopicCacheManager) GetHierarchy() *pbv2.TopicHierarchy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cache == nil {
		return nil
	}
	return m.cache.TopicHierarchy
}

// Update thread-safely updates the internal in-memory TopicVariableCache.
// Note: Updating the hierarchy replaces the entire composite cache object. Any other cached metadata
// (such as Statistical Variables) will be refreshed/repopulated alongside the hierarchy.
func (m *TopicCacheManager) Update(hierarchy *pbv2.TopicHierarchy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = &TopicVariableCache{
		TopicHierarchy: hierarchy,
	}
}

// LoadHierarchy retrieves the cached TopicHierarchy.
// It first checks the local L1 in-memory cache.
// If L1 is empty/cold, it checks the L2 Redis cache.
// If both L1 and L2 are misses, it synchronously loads from the KG, populates both L1 and L2 caches, and returns it.
func (m *TopicCacheManager) LoadHierarchy(ctx context.Context) (*pbv2.TopicHierarchy, error) {
	// Check local L1 in-memory cache
	if h := m.GetHierarchy(); h != nil {
		return h, nil
	}

	// Try loading from L2 Redis cache
	if m.redisClient != nil {
		var cachedHierarchy pbv2.TopicHierarchy
		if found, err := m.redisClient.GetCachedResponse(ctx, redisCacheKeyProto, &cachedHierarchy); found && err == nil {
			slog.Info("Topic cache hit in Redis")
			m.Update(&cachedHierarchy)
			return &cachedHierarchy, nil
		} else if err != nil {
			slog.Error("Failed to read topic cache from Redis", "error", err)
		}
	}

	// L1 & L2 Miss: synchronous load from KG
	slog.Info("Topic cache miss: loading synchronously from KG")
	hierarchy, err := m.FetchTopicsFromKG(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load topic cache from KG during miss: %w", err)
	}

	m.Update(hierarchy)

	// Populate Redis warm L2 cache
	if m.redisClient != nil {
		if err := m.redisClient.CacheResponse(ctx, redisCacheKeyProto, hierarchy); err != nil {
			slog.Error("Failed to write topic cache to Redis", "error", err)
		} else {
			slog.Info("Saved topic cache in Redis successfully")
		}
	}

	return hierarchy, nil
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

// fetchTopicNodes fetches all topic nodes from the KG.
// It returns a map of DCID to newly instantiated TopicNode with Dcid and Name populated.
func (m *TopicCacheManager) fetchTopicNodes(ctx context.Context) (map[string]*pbv2.TopicNode, error) {
	defer util.TimeTrack(time.Now(), "topic: fetchTopicNodes")
	req := &pbv2.NodeRequest{
		Nodes:    []string{"Topic"},
		Property: "<-typeOf",
	}
	resp, err := m.ds.Node(ctx, req, defaultPageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic nodes: %w", err)
	}
	slog.Debug("fetchTopicNodes response data", "data", resp.GetData())

	topics := make(map[string]*pbv2.TopicNode)
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
		topics[dcid] = &pbv2.TopicNode{
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

// isSvpgDcid checks if the DCID belongs to a Stat Var Peer Group (SVPG).
func isSvpgDcid(dcid string) bool {
	return strings.Contains(dcid, "/svpg/")
}

// isTopicDcid checks if the DCID belongs to a Topic.
func isTopicDcid(dcid string) bool {
	return strings.Contains(dcid, "/topic/")
}

// buildHierarchy processes the raw topics map, tracks parent-child relationships,
// identifies root topics, and returns a populated TopicHierarchy.
func (m *TopicCacheManager) buildHierarchy(topics map[string]*pbv2.TopicNode) *pbv2.TopicHierarchy {
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
	hasDefaultRoot := false
	for dcid := range topics {
		if _, isChild := childTopicsSet[dcid]; !isChild {
			roots = append(roots, dcid)
			if dcid == defaultRootTopicDcid {
				hasDefaultRoot = true
			}
		}
	}

	// Fallback Hack: If there are too many roots, and the default root "dc/topic/Root" exists,
	// set it as the only root topic and log a warning instructing KG curation to be trimmed.
	if len(roots) > rootTopicThreshold && hasDefaultRoot {
		slog.Warn(
			"Detected excessive number of root topics, falling back to single default root. Please trim the number of roots in the KG.",
			"detectedRootCount", len(roots),
			"fallbackRootDcid", defaultRootTopicDcid,
		)
		roots = []string{defaultRootTopicDcid}
	} else {
		// Sort root DCIDs alphabetically for determinism
		sort.Strings(roots)
	}

	slog.Info("Topic hierarchy built", "totalTopics", len(topics), "rootCount", len(roots))
	return &pbv2.TopicHierarchy{
		Topics:         topics,
		RootTopicDcids: roots,
	}
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
	resp, err := m.ds.Node(ctx, req, defaultPageSize)
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
	resp, err := m.ds.Node(ctx, req, defaultPageSize)
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

// FetchTopicsFromKG fetches all topics and their relevant variables from the KG, builds the hierarchy, detects roots, and returns a TopicHierarchy.
func (m *TopicCacheManager) FetchTopicsFromKG(ctx context.Context) (*pbv2.TopicHierarchy, error) {
	defer util.TimeTrack(time.Now(), "topic: fetchTopicsFromKG")
	// Fetch core topic nodes, populating their DCID and Name.
	topics, err := m.fetchTopicNodes(ctx)
	if err != nil {
		return nil, err
	}

	if len(topics) == 0 {
		return &pbv2.TopicHierarchy{
			Topics: make(map[string]*pbv2.TopicNode),
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




