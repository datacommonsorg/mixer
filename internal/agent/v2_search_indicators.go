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
)

const (
	// v2MaxSearchTopics and v2MaxSearchVariables cap the topic and variable rows of a search. They are
	// filled separately from all resolver candidates, so topics don't crowd out variables. Keep in sync with
	// internal/server/mcp/instructions/v2/tools/search_indicators.md.
	v2MaxSearchTopics    = 10
	v2MaxSearchVariables = 10
	// v2MaxEntityDcids caps entity_dcids per call; each entity's available variables are loaded once and
	// cached. Keep in sync with the 'entity_dcids' field description in internal/server/mcp/v2_types.go.
	v2MaxEntityDcids = 20
	// v2MaxTopicDcids caps topic_dcids per call, since each requested topic returns all its direct members.
	// Keep in sync with the 'topic_dcids' field description in internal/server/mcp/v2_types.go.
	v2MaxTopicDcids = 10
)

// v2SearchMode is what a search_indicators call does, decided by which of query and topic_dcids is set.
type v2SearchMode int

const (
	v2ModeSearch         v2SearchMode = iota // query set: search topics and variables
	v2ModeBrowseTopics                       // topic_dcids set: return those topics
	v2ModeBrowseTopLevel                     // neither set: return the top-level topics
)

// String returns the mode name for logs.
func (m v2SearchMode) String() string {
	switch m {
	case v2ModeSearch:
		return "search"
	case v2ModeBrowseTopics:
		return "browse_topics"
	default:
		return "browse_top_level"
	}
}

// V2SearchIndicators searches for topics and variables matching a query, or browses topics, keeping only
// indicators with data for the given entities (see V2SearchIndicatorsResponse).
func (s *Service) V2SearchIndicators(
	ctx context.Context,
	req *V2SearchIndicatorsRequest,
) (*V2SearchIndicatorsResponse, error) {
	req = normalizeV2SearchIndicatorsRequest(req)
	if err := validateV2SearchIndicatorsRequest(req); err != nil {
		return nil, err
	}
	defer util.TimeTrack(time.Now(), "Agent: V2SearchIndicators")
	mode := v2SearchModeOf(req)
	slog.Info("V2SearchIndicators started", "mode", mode, "topicDcidsCount", len(req.TopicDcids), "entitiesCount", len(req.EntityDcids), "target", req.Target)

	candidates, entitiesWithoutData, err := s.fetchV2CandidatesAndEntityData(ctx, req, mode)
	if err != nil {
		return nil, err
	}
	data, err := s.fetchV2IndicatorData(ctx, candidates, req.EntityDcids, entitiesWithoutData, req.Target)
	if err != nil {
		return nil, err
	}
	resp := buildV2SearchIndicatorsResponse(mode, candidates, data)

	slog.Info("V2SearchIndicators completed", "mode", mode, "candidatesCount", len(candidates), "topicsCount", len(resp.TopicCandidates.Rows))
	return resp, nil
}

// normalizeV2SearchIndicatorsRequest returns a copy of req with the query trimmed and blank and duplicate
// DCIDs dropped (order kept). A nil request becomes an empty one, which browses the top-level topics.
func normalizeV2SearchIndicatorsRequest(req *V2SearchIndicatorsRequest) *V2SearchIndicatorsRequest {
	if req == nil {
		return &V2SearchIndicatorsRequest{}
	}
	return &V2SearchIndicatorsRequest{
		Query:       strings.TrimSpace(req.Query),
		TopicDcids:  uniqueNonBlank(req.TopicDcids),
		EntityDcids: uniqueNonBlank(req.EntityDcids),
		Target:      req.Target,
	}
}

// validateV2SearchIndicatorsRequest checks a normalized request and returns actionable errors (see
// v2_tool_errors.go).
func validateV2SearchIndicatorsRequest(req *V2SearchIndicatorsRequest) error {
	if req.Query != "" && len(req.TopicDcids) > 0 {
		return errV2QueryAndTopicDcids()
	}
	if len(req.EntityDcids) > v2MaxEntityDcids {
		return errV2TooManyEntityDcids(len(req.EntityDcids))
	}
	if len(req.TopicDcids) > v2MaxTopicDcids {
		return errV2TooManyTopicDcids(len(req.TopicDcids))
	}
	if req.Target != "" && !slices.Contains(validTargets, req.Target) {
		return errV2InvalidTarget(req.Target)
	}
	return nil
}

// v2SearchModeOf returns the mode of a normalized, valid request.
func v2SearchModeOf(req *V2SearchIndicatorsRequest) v2SearchMode {
	switch {
	case req.Query != "":
		return v2ModeSearch
	case len(req.TopicDcids) > 0:
		return v2ModeBrowseTopics
	default:
		return v2ModeBrowseTopLevel
	}
}

// fetchV2CandidatesAndEntityData fetches the candidates for the request and, in parallel, loads which
// entities have no data at all, then rejects unknown entity DCIDs.
//
// Entity variable sets are loaded in parallel with the candidate fetch because the availability filter
// needs them anyway (they are cached, so its later check is served from memory), and loading them early
// overlaps their cold latency with the search.
func (s *Service) fetchV2CandidatesAndEntityData(
	ctx context.Context,
	req *V2SearchIndicatorsRequest,
	mode v2SearchMode,
) ([]*pbv2.ResolveResponse_Entity_Candidate, []string, error) {
	var candidates []*pbv2.ResolveResponse_Entity_Candidate
	var entitiesWithoutData []string
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		candidates, err = s.fetchV2Candidates(gCtx, req, mode)
		return err
	})
	if len(req.EntityDcids) > 0 {
		g.Go(func() error {
			var err error
			entitiesWithoutData, err = s.loadEntitiesWithoutData(gCtx, req.EntityDcids)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	if err := s.checkEntitiesExist(ctx, entitiesWithoutData); err != nil {
		return nil, nil, err
	}
	return candidates, entitiesWithoutData, nil
}

// fetchV2Candidates fetches the topic and variable candidates for the mode. Topics come with their direct
// members (not their recursive descendants) and search candidates are not truncated, so the tables can be
// filled from all of them after filtering.
func (s *Service) fetchV2Candidates(
	ctx context.Context,
	req *V2SearchIndicatorsRequest,
	mode v2SearchMode,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	if mode == v2ModeBrowseTopics {
		return s.fetchTopicCandidates(ctx, req.TopicDcids, req.Target)
	}
	// An empty query makes fetchCandidates browse the top-level topics.
	candidates, err := s.fetchCandidates(ctx, req.Query, 0 /* no limit */, false /* direct members */, req.Target)
	if err != nil {
		slog.Warn("Failed to fetch indicator candidates", "error", err, "mode", mode)
		if mode == v2ModeSearch {
			return nil, errV2SearchFailed(err)
		}
		return nil, errV2TopicLookupFailed(err)
	}
	return candidates, nil
}

// fetchTopicCandidates returns the requested topics with their direct members, in request order, or an
// error listing the DCIDs that are not topics with members (the topic resolver returns no candidate for
// them).
func (s *Service) fetchTopicCandidates(
	ctx context.Context,
	topicDcids []string,
	target string,
) ([]*pbv2.ResolveResponse_Entity_Candidate, error) {
	resp, err := s.mixer.V2Resolve(ctx, &pbv2.ResolveRequest{
		Nodes:        topicDcids,
		Resolver:     ResolverTopic,
		ExpandTopics: false,
		Target:       target,
	})
	if err != nil {
		slog.Warn("Failed to look up topics", "error", err, "topicsCount", len(topicDcids))
		return nil, errV2TopicLookupFailed(err)
	}
	candidates, unknown := topicCandidatesInRequestOrder(resp, topicDcids)
	if len(unknown) > 0 {
		return nil, errV2UnknownTopicDcids(unknown)
	}
	return candidates, nil
}

// topicCandidatesInRequestOrder returns the topic resolver's candidates for the requested topics in request
// order (the resolver may return them in any order), and the requested DCIDs it returned no topic for.
func topicCandidatesInRequestOrder(
	resp *pbv2.ResolveResponse,
	topicDcids []string,
) ([]*pbv2.ResolveResponse_Entity_Candidate, []string) {
	byDcid := map[string]*pbv2.ResolveResponse_Entity_Candidate{}
	for _, entity := range resp.GetEntities() {
		for _, candidate := range entity.GetCandidates() {
			if candidate.GetDcid() == entity.GetNode() && isTopic(candidate) {
				byDcid[candidate.GetDcid()] = candidate
			}
		}
	}
	var candidates []*pbv2.ResolveResponse_Entity_Candidate
	var unknown []string
	for _, dcid := range topicDcids {
		if candidate, ok := byDcid[dcid]; ok {
			candidates = append(candidates, candidate)
		} else {
			unknown = append(unknown, dcid)
		}
	}
	return candidates, unknown
}

// loadEntitiesWithoutData loads (and caches) the entities' variable sets and returns the entities without any
// data, mapping a failed load to a caller-facing error.
func (s *Service) loadEntitiesWithoutData(ctx context.Context, entities []string) ([]string, error) {
	withoutData, err := s.cache.EntitiesWithoutData(ctx, entities)
	if err != nil {
		slog.Warn("Failed to load entity variables", "error", err, "entitiesCount", len(entities))
		return nil, errV2AvailabilityFailed(err)
	}
	return withoutData, nil
}

// checkEntitiesExist returns an error listing the entities that don't exist in the knowledge graph (no
// typeOf). It only gets the entities without any data: an unknown DCID never has data, so checking just
// those catches every unknown DCID while sparing the V2Node call whenever all entities have data.
func (s *Service) checkEntitiesExist(ctx context.Context, entitiesWithoutData []string) error {
	if len(entitiesWithoutData) == 0 {
		return nil
	}
	nodes, err := s.fetchNodeGraphs(ctx, entitiesWithoutData, "->typeOf")
	if err != nil {
		slog.Warn("Failed to look up entities without data", "error", err, "entitiesCount", len(entitiesWithoutData))
		return errV2EntityLookupFailed(err)
	}
	var unknown []string
	for _, dcid := range entitiesWithoutData {
		if len(getPropDcids(nodes[dcid], "typeOf")) == 0 {
			unknown = append(unknown, dcid)
		}
	}
	if len(unknown) > 0 {
		return errV2UnknownEntityDcids(unknown)
	}
	return nil
}

// v2IndicatorData holds what the response builders need beyond the candidates: each topic's descendant
// variables and, when entities are given, which entities have data for each variable.
type v2IndicatorData struct {
	entities            []string
	entitiesWithoutData []string
	descendants         map[string][]string
	availability        map[string]map[string]bool
}

// fetchV2IndicatorData fetches the descendant variables of every topic among the candidates and their
// member sub-topics, and, when entities are given, which of the entities have data for each variable.
//
// Descendants are fetched even without entities, since descendant_variable_count needs them.
func (s *Service) fetchV2IndicatorData(
	ctx context.Context,
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	entities []string,
	entitiesWithoutData []string,
	target string,
) (*v2IndicatorData, error) {
	topicDcids, directVarDcids := collectSubtopicsAndDirectVars(candidates)
	descendants, err := s.fetchDescendantVariables(ctx, topicDcids, target)
	if err != nil {
		slog.Warn("Failed to fetch topic descendants", "error", err, "topicsCount", len(topicDcids))
		return nil, errV2TopicLookupFailed(err)
	}
	data := &v2IndicatorData{entities: entities, entitiesWithoutData: entitiesWithoutData, descendants: descendants}
	if !data.filtered() {
		return data, nil
	}
	data.availability, err = s.cache.CheckAvailability(ctx, entities, gatherVarsToCheck(directVarDcids, descendants))
	if err != nil {
		slog.Warn("Failed to check indicator availability", "error", err, "entitiesCount", len(entities))
		return nil, errV2AvailabilityFailed(err)
	}
	return data, nil
}

// filtered reports whether results are filtered by data availability (entities were given).
func (d *v2IndicatorData) filtered() bool {
	return len(d.entities) > 0
}

// topicEntities returns the sorted entities with data for any variable under the topic, or an empty list
// when results are not filtered.
func (d *v2IndicatorData) topicEntities(topicDcid string) []string {
	if !d.filtered() {
		return []string{}
	}
	return util.NonNilStringSlice(entitiesWithData(d.descendants[topicDcid], d.entities, d.availability))
}

// variableEntities returns the sorted entities with data for the variable, or an empty list when results
// are not filtered.
func (d *v2IndicatorData) variableEntities(varDcid string) []string {
	if !d.filtered() {
		return []string{}
	}
	return util.NonNilStringSlice(entitiesWithData([]string{varDcid}, d.entities, d.availability))
}

// descendantCount returns the number of distinct variables at any depth under the topic (a variable can be
// listed under several sub-topics), counting only those with data for some entity when filtered. A topic is
// kept only when this is positive, which when filtered means some entity has data under it.
func (d *v2IndicatorData) descendantCount(topicDcid string) int {
	counted := map[string]struct{}{}
	for _, varDcid := range d.descendants[topicDcid] {
		if _, ok := counted[varDcid]; !ok && d.hasDataForVariable(varDcid) {
			counted[varDcid] = struct{}{}
		}
	}
	return len(counted)
}

// hasDataForVariable reports whether a variable row or member is kept: always when results are not filtered,
// otherwise when some entity has data for it. Unlike variableEntities, it doesn't build a sorted list, since
// it runs for every descendant of every topic.
func (d *v2IndicatorData) hasDataForVariable(varDcid string) bool {
	if !d.filtered() {
		return true
	}
	for _, entity := range d.entities {
		if d.availability[entity][varDcid] {
			return true
		}
	}
	return false
}

// noEntityHasData reports whether results are filtered and none of the entities has any data.
func (d *v2IndicatorData) noEntityHasData() bool {
	return d.filtered() && len(d.entitiesWithoutData) == len(d.entities)
}

// buildV2SearchIndicatorsResponse builds the tables and note for the mode. Search returns up to
// v2MaxSearchTopics topics and v2MaxSearchVariables variables; browse returns every requested or
// top-level topic and no variables table.
//
// Topics without data for the entities are dropped in browse too, like search rows, so an empty table
// always means nothing has data rather than a mix of empty and non-empty rows.
func buildV2SearchIndicatorsResponse(
	mode v2SearchMode,
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	data *v2IndicatorData,
) *V2SearchIndicatorsResponse {
	resp := &V2SearchIndicatorsResponse{}
	if mode == v2ModeSearch {
		resp.TopicCandidates = buildV2TopicsTable(candidates, data, v2MaxSearchTopics)
		resp.VariableCandidates = buildV2VariablesTable(candidates, data, v2MaxSearchVariables)
	} else {
		resp.TopicCandidates = buildV2TopicsTable(candidates, data, 0)
	}
	resp.Note = v2ResponseNote(mode, candidates, data, resp)
	return resp
}

// v2ResponseNote returns the note for a response: why results are empty when they are, or that results
// are unfiltered when no entities were given.
func v2ResponseNote(
	mode v2SearchMode,
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	data *v2IndicatorData,
	resp *V2SearchIndicatorsResponse,
) string {
	empty := len(resp.TopicCandidates.Rows) == 0 && (resp.VariableCandidates == nil || len(resp.VariableCandidates.Rows) == 0)
	switch {
	// Checked first: when no entity has any data, rephrasing the query can't help, so the agent is told that
	// rather than that nothing matched.
	case data.noEntityHasData():
		return v2NoEntityDataNote
	case mode == v2ModeSearch && len(candidates) == 0:
		return v2NoMatchNote
	case !data.filtered():
		return v2NoEntitiesNote
	case !empty:
		return ""
	case mode == v2ModeSearch:
		return v2NoQueryDataNote
	case mode == v2ModeBrowseTopics:
		return v2NoRequestedTopicDataNote
	default:
		return v2NoTopLevelTopicDataNote
	}
}

// buildV2TopicsTable returns a row per topic candidate with variables below it (with data, when filtered),
// in candidate order and without duplicates, up to limit rows (0 means no limit). Topics without any
// variables are dropped even when unfiltered: some topics (e.g. SDG goals) group variables the topic
// hierarchy doesn't list, and an empty row gives the agent nothing to use or drill into.
func buildV2TopicsTable(candidates []*pbv2.ResolveResponse_Entity_Candidate, data *v2IndicatorData, limit int) *Table {
	columns := []string{colDcid, colName, colEntitiesWithData, colV2MemberTopics, colV2MemberVariables, colDescendantVariableCount}
	return buildV2Table(columns, candidates, true /* topics */, limit, data.topicRow)
}

// buildV2VariablesTable returns a row per variable candidate with data, in candidate order and without
// duplicates, up to limit rows (0 means no limit).
func buildV2VariablesTable(candidates []*pbv2.ResolveResponse_Entity_Candidate, data *v2IndicatorData, limit int) *Table {
	columns := []string{colDcid, colName, colEntitiesWithData, colV2ObservationProperties}
	return buildV2Table(columns, candidates, false /* variables */, limit, data.variableRow)
}

// buildV2Table returns a table with a row per unique candidate of one kind (topics when wantTopics is set,
// variables otherwise), in candidate order, skipping candidates the row function rejects, up to limit rows
// (0 means no limit).
func buildV2Table(
	columns []string,
	candidates []*pbv2.ResolveResponse_Entity_Candidate,
	wantTopics bool,
	limit int,
	row func(*pbv2.ResolveResponse_Entity_Candidate) ([]any, bool),
) *Table {
	table := &Table{Columns: columns}
	seen := newDcidSet()
	for _, c := range candidates {
		if limit > 0 && len(table.Rows) >= limit {
			break
		}
		if isTopic(c) != wantTopics || !seen.add(c.GetDcid()) {
			continue
		}
		if r, ok := row(c); ok {
			table.Rows = append(table.Rows, r)
		}
	}
	return table
}

// topicRow returns the row for a topic candidate, or false when no variables (with data, when filtered) are
// below it.
func (d *v2IndicatorData) topicRow(c *pbv2.ResolveResponse_Entity_Candidate) ([]any, bool) {
	count := d.descendantCount(c.GetDcid())
	if count == 0 {
		return nil, false
	}
	memberTopics, memberVariables := v2TopicMembers(c, d)
	return []any{c.GetDcid(), c.GetName(), d.topicEntities(c.GetDcid()), memberTopics, memberVariables, count}, true
}

// variableRow returns the row for a variable candidate, or false when results are filtered and no entity has
// data for it.
func (d *v2IndicatorData) variableRow(c *pbv2.ResolveResponse_Entity_Candidate) ([]any, bool) {
	if !d.hasDataForVariable(c.GetDcid()) {
		return nil, false
	}
	return []any{c.GetDcid(), c.GetName(), d.variableEntities(c.GetDcid()), util.NonNilStringSlice(c.GetObservationProperties())}, true
}

// v2TopicMembers returns the topic's direct sub-topics and variables with data, in the order the topic
// defines them; sub-topics are kept by the same rule as topic rows (see buildV2TopicsTable). Both lists are
// non-nil so they serialize as [].
func v2TopicMembers(c *pbv2.ResolveResponse_Entity_Candidate, data *v2IndicatorData) ([]v2IndicatorRef, []v2IndicatorRef) {
	topics := []v2IndicatorRef{}
	variables := []v2IndicatorRef{}
	for _, child := range c.GetChildren() {
		ref := v2IndicatorRef{Dcid: child.GetDcid(), Name: child.GetName()}
		if isTopic(child) {
			if data.descendantCount(child.GetDcid()) > 0 {
				topics = append(topics, ref)
			}
		} else if data.hasDataForVariable(child.GetDcid()) {
			variables = append(variables, ref)
		}
	}
	return topics, variables
}

// uniqueNonBlank returns the trimmed, non-blank values without duplicates, in their original order.
// util.MergeDedupe is not used because it sorts, and browse returns topics in request order.
func uniqueNonBlank(values []string) []string {
	unique := newDcidSet()
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			unique.add(value)
		}
	}
	return unique.dcids
}
