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
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

const (
	// v2MaxEntitiesPerQuery caps the candidates returned per queried name. The non_place resolver can
	// return up to 50 candidates above its similarity threshold; a few are enough for an LLM to pick
	// from and keep responses small.
	v2MaxEntitiesPerQuery = 3
	// v2MaxEntitiesPerRequest caps the names per call. Names are resolved in parallel and each can try
	// several resolvers (Maps-backed place resolution and embeddings search), so this bounds backend fan-out.
	// Keep in sync with the 'entities' field description in internal/server/mcp/v2_types.go.
	v2MaxEntitiesPerRequest = 20
	// v2MaxChildHintsPerEntity caps child_entity_type_hints per entity. Without it, agents tend to send
	// many loosely related hints (e.g. "subcounty", "sub-county", "ward", "town", ...) instead of the one
	// or two types they want; the error steers them back. Keep in sync with the
	// 'child_entity_type_hints' field description in internal/server/mcp/v2_types.go.
	v2MaxChildHintsPerEntity = 3
	// v2MaxTypesPerHint caps the Class DCIDs used per entity type hint. The type resolver can return many
	// loosely related classes; the top few cover close variants (e.g. School, PublicSchool) without diluting
	// filters.
	v2MaxTypesPerHint = 3
	// v2MaxTypeCandidatesPerHint caps the Class DCIDs kept per hint from the type resolver. It is larger
	// than v2MaxTypesPerHint because child type hints filter candidates by existence under the parent
	// (see matchChildTypes), so the wanted type can rank below generic or absent classes.
	v2MaxTypeCandidatesPerHint = 10
)

// v2NonPlaceExcludedTypes mirrors excluded_types for the non_place resolver in
// internal/server/spanner/spanner_config/default.yaml. The resolver rejects requests whose typeOf filter
// contains any of these, so they must be stripped from filters before calling it. Keep in sync with that config.
var v2NonPlaceExcludedTypes = []string{
	DcidTypeVariable,
	DcidTypeTopic,
}

// V2ResolveEntities resolves entity names (places and non-place entities) to DCIDs, using optional
// type hints to route each name to the right resolver and to filter candidates by type.
//
// The "type" and "non_place" resolvers are only implemented by the Spanner data source, so this
// requires a Spanner-backed mixer.
//
// If every resolver attempt fails for a name, the whole call fails rather than returning partial
// results, so backend errors are not mistaken for names that could not be resolved.
func (s *Service) V2ResolveEntities(
	ctx context.Context,
	req *V2ResolveEntitiesRequest,
) (*V2ResolveEntitiesResponse, error) {
	if err := validateV2ResolveEntitiesRequest(req); err != nil {
		return nil, err
	}
	defer util.TimeTrack(time.Now(), "Agent: V2ResolveEntities")
	slog.Info("V2ResolveEntities started", "entitiesCount", len(req.Entities), "target", req.Target)

	// All hints (entity and child entity types) across the batch are resolved in a single call before
	// entity resolution, since entity routing depends on the resolved types. A failure of this call fails
	// the request, whichever hint list the hints came from.
	typesByHint, err := s.resolveTypeHints(ctx, collectTypeHints(req.Entities), req.Target)
	if err != nil {
		return nil, err
	}

	// Place types only matter for routing hinted queries, so skip loading them when there are no hints.
	var placeTypes map[string]struct{}
	if len(typesByHint) > 0 {
		placeTypes = s.v2PlaceTypes(ctx)
	}

	results := make([]*v2EntityGroup, len(req.Entities))
	g, gCtx := errgroup.WithContext(ctx)
	for i, query := range req.Entities {
		g.Go(func() error {
			attempts := resolutionPlan(typesForHints(query.EntityTypeHints, typesByHint), placeTypes)
			entities, err := s.resolveEntityCandidates(gCtx, strings.TrimSpace(query.Name), attempts, req.Target)
			if err != nil {
				return err
			}
			results[i] = &v2EntityGroup{query: query.Name, entities: entities, hasChildHints: hasNonBlank(query.ChildEntityTypeHints)}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	s.enrichResolvedEntities(ctx, results)
	resp := &V2ResolveEntitiesResponse{ResolvedEntities: buildResolvedEntitiesTable(results)}
	if s.resolveChildEntityTypes(ctx, req.Entities, results, typesByHint) {
		resp.CandidateChildEntityTypes = buildChildEntityTypesTable(results)
	} else {
		resp.ChildEntityTypesNote = v2ChildTypesUnavailableNote
	}

	slog.Info("V2ResolveEntities completed", "entitiesCount", len(req.Entities))
	return resp, nil
}

// validateV2ResolveEntitiesRequest checks the request and returns actionable errors (see v2_tool_errors.go)
// that tell the caller (typically an LLM) how to fix the call.
func validateV2ResolveEntitiesRequest(req *V2ResolveEntitiesRequest) error {
	if req == nil || len(req.Entities) == 0 {
		return errV2NoEntities()
	}
	if len(req.Entities) > v2MaxEntitiesPerRequest {
		return errV2TooManyEntities(len(req.Entities))
	}
	for i, query := range req.Entities {
		if query == nil || strings.TrimSpace(query.Name) == "" {
			return errV2EmptyEntityName(i)
		}
		if len(query.ChildEntityTypeHints) > v2MaxChildHintsPerEntity {
			return errV2TooManyChildHints(i, len(query.ChildEntityTypeHints))
		}
	}
	if req.Target != "" && !slices.Contains(validTargets, req.Target) {
		return errV2InvalidTarget(req.Target)
	}
	return nil
}

// resolveAttempt is one resolver call in a resolution plan: a resolver and an optional typeOf filter.
type resolveAttempt struct {
	resolver  string
	typeDcids []string
}

// resolutionPlan returns the ordered resolver attempts for a query. This is the only place that decides
// which resolvers run and in what order, so the strategy can change here without affecting callers.
//
// Types only pick the primary resolver; the others remain fallbacks when it finds nothing. Attempts run
// one at a time rather than fanning out, since most names resolve on the first attempt.
//   - Place queries: place resolver (filtered to the hinted place type, if any), then a non_place search
//     for place-like groupings the place resolver does not know (e.g. "High income countries"). When a
//     place type is hinted, no fallback drops the hint: the non_place search is filtered to the hinted
//     types and the place resolver is not retried unfiltered, since returning a different type (e.g. the
//     city, or a school, for "Santa Clara" with hint "county") is worse than returning nothing. Callers
//     are instead asked to pass fully qualified names (e.g. "Santa Clara County, California, United
//     States"), which the place resolver matches directly.
//   - Non-place queries: non_place filtered to the hinted types, then unfiltered non_place (hints from an
//     LLM may not match the entity's exact class), then the place resolver last, because it is backed by
//     Google Maps and can return false positives for organization names (e.g. a street sharing the name).
func resolutionPlan(typeDcids []string, placeTypes map[string]struct{}) []resolveAttempt {
	if isPlaceQuery(typeDcids, placeTypes) {
		placeFilter := placeTypeFilter(typeDcids, placeTypes)
		var nonPlaceFilter []string
		if len(placeFilter) > 0 {
			// Generic types are dropped so the filter does not match every place (see isGenericPlaceType).
			nonPlaceFilter = slices.DeleteFunc(nonPlaceTypeFilter(typeDcids), isGenericPlaceType)
		}
		return []resolveAttempt{
			{resolver: ResolverPlace, typeDcids: placeFilter},
			{resolver: ResolverNonPlace, typeDcids: nonPlaceFilter},
		}
	}
	var attempts []resolveAttempt
	if filterTypes := nonPlaceTypeFilter(typeDcids); len(filterTypes) > 0 {
		attempts = append(attempts, resolveAttempt{resolver: ResolverNonPlace, typeDcids: filterTypes})
	}
	return append(attempts,
		resolveAttempt{resolver: ResolverNonPlace},
		resolveAttempt{resolver: ResolverPlace},
	)
}

// resolveEntityCandidates runs the attempts in order and returns the first non-empty candidates.
// A failed attempt is logged and the next one is tried (e.g. a transient Maps failure may still resolve
// via non_place); an error is returned only if every attempt failed or the context was cancelled.
func (s *Service) resolveEntityCandidates(
	ctx context.Context,
	name string,
	attempts []resolveAttempt,
	target string,
) ([]*v2ResolvedEntity, error) {
	var lastErr error
	failed := 0
	for _, attempt := range attempts {
		entities, err := s.resolveWithResolver(ctx, name, attempt.resolver, attempt.typeDcids, target)
		if err != nil {
			// A cancelled context (e.g. a sibling query failed) dooms the remaining attempts too.
			if ctx.Err() != nil {
				return nil, err
			}
			slog.Warn("Entity resolution attempt failed; trying next resolver", "name", name, "resolver", attempt.resolver, "error", err)
			lastErr = err
			failed++
			continue
		}
		if len(entities) > 0 {
			return entities, nil
		}
	}
	if failed == len(attempts) {
		return nil, lastErr
	}
	return []*v2ResolvedEntity{}, nil
}

// resolveWithResolver calls V2Resolve for one name with the given resolver and optional typeOf filter,
// returning up to v2MaxEntitiesPerQuery deduplicated candidates in resolver order.
func (s *Service) resolveWithResolver(
	ctx context.Context,
	name string,
	resolver string,
	typeDcids []string,
	target string,
) ([]*v2ResolvedEntity, error) {
	resp, err := s.mixer.V2Resolve(ctx, &pbv2.ResolveRequest{
		Nodes:    []string{name},
		Resolver: resolver,
		Property: descriptionPropertyWithTypes(typeDcids),
		Target:   target,
	})
	if err != nil {
		return nil, errV2ResolveFailed(err, name, resolver)
	}
	return collectResolvedEntities(resp.GetEntities()), nil
}

// resolveTypeHints resolves natural-language type hints to Class DCIDs in one V2Resolve call,
// keeping the top v2MaxTypeCandidatesPerHint classes per hint (with their similarity scores) in resolver order.
func (s *Service) resolveTypeHints(ctx context.Context, hints []string, target string) (map[string][]typeCandidate, error) {
	if len(hints) == 0 {
		return nil, nil
	}
	resp, err := s.mixer.V2Resolve(ctx, &pbv2.ResolveRequest{
		Nodes:    hints,
		Resolver: ResolverType,
		Target:   target,
	})
	if err != nil {
		return nil, errV2TypeHintsFailed(err)
	}
	typesByHint := make(map[string][]typeCandidate, len(hints))
	for _, entity := range resp.GetEntities() {
		hint := entity.GetNode()
		for _, candidate := range entity.GetCandidates() {
			if len(typesByHint[hint]) >= v2MaxTypeCandidatesPerHint {
				break
			}
			dcid := candidate.GetDcid()
			if dcid == "" || slices.ContainsFunc(typesByHint[hint], func(c typeCandidate) bool { return c.dcid == dcid }) {
				continue
			}
			typesByHint[hint] = append(typesByHint[hint], typeCandidate{dcid: dcid, score: candidateScore(candidate)})
		}
	}
	return typesByHint, nil
}

// candidateScore returns the similarity score the embeddings resolver reports in candidate metadata,
// or 0 when absent or malformed.
func candidateScore(candidate *pbv2.ResolveResponse_Entity_Candidate) float64 {
	score, err := strconv.ParseFloat(candidate.GetMetadata()["score"], 64)
	if err != nil {
		return 0
	}
	return score
}

// enrichResolvedEntities fills canonical names and types for all resolved DCIDs via one V2Node call.
// The place resolver returns bare DCIDs, and non_place candidate names come from embedding sentences,
// so the knowledge graph values are preferred when available. Failures are logged and the resolver
// values are kept.
func (s *Service) enrichResolvedEntities(ctx context.Context, groups []*v2EntityGroup) {
	var dcids []string
	for _, group := range groups {
		for _, entity := range group.entities {
			if !slices.Contains(dcids, entity.dcid) {
				dcids = append(dcids, entity.dcid)
			}
		}
	}
	if len(dcids) == 0 {
		return
	}
	nodeResp, err := s.fetchAllNodes(ctx, &pbv2.NodeRequest{Nodes: dcids, Property: nodePropertiesQuery})
	if err != nil {
		slog.Warn("Failed to enrich resolved entities via V2Node", "error", err, "dcidsCount", len(dcids))
		return
	}
	for _, group := range groups {
		for _, entity := range group.entities {
			nodeData, ok := nodeResp.GetData()[entity.dcid]
			if !ok {
				continue
			}
			if name := getPropValue(nodeData, "name"); name != "" {
				entity.name = name
			}
			if types := uniqueTypes(getPropDcids(nodeData, "typeOf")); len(types) > 0 {
				entity.types = types
			}
		}
	}
}

// isPlaceQuery decides whether a query goes to the place resolver first. Queries without (resolvable)
// hints default to the place path, matching the most common use; otherwise the top-ranked class of the
// first hint decides, since it is the caller's primary and the resolver's best guess at the entity type.
func isPlaceQuery(typeDcids []string, placeTypes map[string]struct{}) bool {
	if len(typeDcids) == 0 {
		return true
	}
	_, ok := placeTypes[typeDcids[0]]
	return ok
}

// placeTypeFilter returns the place type to filter place resolution by (e.g. "Georgia" with hint
// "country" resolves to the country, not the state). At most one type is used: with multiple types the
// place resolver's recognition step keeps only one of them per name, chosen by map iteration order, so
// results become nondeterministic and the dropped types cost extra Maps calls. Generic place types are
// skipped (see isGenericPlaceType).
func placeTypeFilter(typeDcids []string, placeTypes map[string]struct{}) []string {
	for _, dcid := range typeDcids {
		if _, ok := placeTypes[dcid]; ok && !isGenericPlaceType(dcid) {
			return []string{dcid}
		}
	}
	return nil
}

// nonPlaceTypeFilter returns typeDcids without the types the non_place resolver rejects.
// See v2NonPlaceExcludedTypes.
func nonPlaceTypeFilter(typeDcids []string) []string {
	var filterTypes []string
	for _, dcid := range typeDcids {
		if !slices.Contains(v2NonPlaceExcludedTypes, dcid) {
			filterTypes = append(filterTypes, dcid)
		}
	}
	return filterTypes
}

// descriptionPropertyWithTypes builds the resolve property expression, adding a typeOf filter when
// typeDcids is non-empty (e.g. "<-description{typeOf:[School,PublicSchool]}->dcid").
func descriptionPropertyWithTypes(typeDcids []string) string {
	if len(typeDcids) == 0 {
		return PropDescription
	}
	return fmt.Sprintf("<-description{typeOf:[%s]}->dcid", strings.Join(typeDcids, ","))
}

// collectResolvedEntities flattens resolver candidates into deduplicated entities, capped at
// v2MaxEntitiesPerQuery. Candidates are merged across response entities because some backends
// (Bigtable resolve.Description) return one entity per (name, type) pair, and merged responses may
// repeat a node.
func collectResolvedEntities(resolveEntities []*pbv2.ResolveResponse_Entity) []*v2ResolvedEntity {
	entities := []*v2ResolvedEntity{}
	seen := map[string]struct{}{}
	for _, resolveEntity := range resolveEntities {
		for _, candidate := range resolveEntity.GetCandidates() {
			if len(entities) >= v2MaxEntitiesPerQuery {
				return entities
			}
			dcid := candidate.GetDcid()
			if _, ok := seen[dcid]; ok || dcid == "" {
				continue
			}
			seen[dcid] = struct{}{}
			entities = append(entities, &v2ResolvedEntity{
				dcid:  dcid,
				name:  candidate.GetName(),
				types: uniqueTypes(candidate.GetTypeOf()),
			})
		}
	}
	return entities
}

// collectTypeHints returns the unique, non-empty entity and child entity type hints across all
// queries, in first-seen order.
func collectTypeHints(queries []*V2EntityQuery) []string {
	var hints []string
	for _, query := range queries {
		for _, hint := range slices.Concat(query.EntityTypeHints, query.ChildEntityTypeHints) {
			if hint = strings.TrimSpace(hint); hint != "" && !slices.Contains(hints, hint) {
				hints = append(hints, hint)
			}
		}
	}
	return hints
}

// typesForHints returns the resolved Class DCIDs for hints, deduplicated and ordered by hint order and
// then by resolver rank within each hint, keeping the top v2MaxTypesPerHint classes per hint.
func typesForHints(hints []string, typesByHint map[string][]typeCandidate) []string {
	var typeDcids []string
	for _, hint := range hints {
		candidates := typesByHint[strings.TrimSpace(hint)]
		for _, candidate := range candidates[:min(len(candidates), v2MaxTypesPerHint)] {
			if !slices.Contains(typeDcids, candidate.dcid) {
				typeDcids = append(typeDcids, candidate.dcid)
			}
		}
	}
	return typeDcids
}

// uniqueTypes returns the sorted, deduplicated types as a non-nil slice. typeOf values can repeat when
// merged from multiple data sources (e.g. ["Country", "Country"]). The generic "Place" type is dropped
// when more specific types exist, since it adds nothing for disambiguation.
func uniqueTypes(types []string) []string {
	unique := slices.Compact(slices.Sorted(slices.Values(types)))
	if len(unique) > 1 {
		unique = slices.DeleteFunc(unique, func(t string) bool { return t == "Place" })
	}
	return util.NonNilStringSlice(unique)
}

// buildResolvedEntitiesTable formats resolved groups into a table, one row per candidate, with a
// placeholder row (empty dcid, name and types) for queries that resolved to nothing.
func buildResolvedEntitiesTable(groups []*v2EntityGroup) *Table {
	rows := [][]any{}
	for _, group := range groups {
		if len(group.entities) == 0 {
			rows = append(rows, []any{group.query, "", "", []string{}})
			continue
		}
		for _, entity := range group.entities {
			rows = append(rows, []any{group.query, entity.dcid, entity.name, entity.types})
		}
	}
	return &Table{
		Columns: []string{colQuery, colDcid, colName, colTypes},
		Rows:    rows,
	}
}

// hasNonBlank reports whether any value is non-blank after trimming whitespace.
func hasNonBlank(values []string) bool {
	return slices.ContainsFunc(values, func(v string) bool { return strings.TrimSpace(v) != "" })
}
