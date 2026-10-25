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
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"
)

const (
	// v2ChildTypesTimeout bounds the child type lookup, which is best-effort: a slow lookup must not
	// stall the entity resolution result. On timeout or failure, the child types table is omitted (see
	// resolveChildEntityTypes).
	v2ChildTypesTimeout = 5 * time.Second
	// v2ChildTypeScoreMargin is how close (in type resolver similarity) another existing class must be to
	// a hint's best existing class to also be returned. Measured on staging type embeddings: umbrella
	// terms like "school" have subtypes within ~0.02, while specific terms like "county" or "state" lead
	// the next class by 0.06 or more.
	v2ChildTypeScoreMargin = 0.02
	// v2ChildTypeRelevanceMargin is how far (in type resolver similarity) below a hint's best class a class
	// may score and still be considered for that hint. It drops loosely related classes that happen to exist
	// under the parent when the hint's own class does not (e.g. "state" -> County under Kenya, or an
	// unrecognized term like "autonomous community" -> Neighborhood). Measured on staging type embeddings:
	// genuine alternatives are within ~0.03 of the best class (AdministrativeArea1 for "administrative area
	// 1", school subtypes, PublicSchool), while such loose matches trail by 0.07 or more.
	v2ChildTypeRelevanceMargin = 0.05
	// v2MaxChildTypesPerHint caps the child types returned per hint.
	v2MaxChildTypesPerHint = 3
)

// resolveChildEntityTypes returns, for each query with child type hints (in query order), the hinted
// child types that exist within the query's best-matching entity. groups[i] holds the resolved entities
// for queries[i].
//
// The child types of all best matches are fetched in one call (served from the Spanner LinkedEdge table,
// and cached per ancestor), then filtered by the hints (see matchChildTypes). Only the best match is
// used; callers wanting another candidate's child types can re-resolve with a more specific name.
//
// It reports whether the results are usable: false if the lookup failed. Failures are logged rather than
// returned, since child types are supplementary to the entity resolution result, and the caller omits the
// child types table so a failed lookup is not mistaken for "no such child types".
func (s *Service) resolveChildEntityTypes(
	ctx context.Context,
	queries []*V2EntityQuery,
	groups []*v2EntityGroup,
	typesByHint map[string][]typeCandidate,
) ([]*v2ChildTypesResult, bool) {
	var results []*v2ChildTypesResult
	var parents []string
	for i, query := range queries {
		if !hasNonBlank(query.ChildEntityTypeHints) {
			continue
		}
		result := &v2ChildTypesResult{query: query.Name, hints: query.ChildEntityTypeHints}
		if len(groups[i].entities) > 0 {
			result.parent = groups[i].entities[0].dcid
			if !slices.Contains(parents, result.parent) {
				parents = append(parents, result.parent)
			}
		}
		results = append(results, result)
	}
	if len(parents) == 0 {
		return results, true
	}

	lookupCtx, cancel := context.WithTimeout(ctx, v2ChildTypesTimeout)
	defer cancel()
	available, err := s.childTypesOf(lookupCtx, parents)
	if err != nil {
		slog.Warn("Failed to fetch child entity types", "error", err, "parentsCount", len(parents))
		return nil, false
	}
	for _, result := range results {
		if result.parent == "" {
			continue
		}
		for _, dcid := range matchChildTypes(result.hints, typesByHint, available[result.parent]) {
			result.types = append(result.types, &v2ChildType{dcid: dcid})
		}
	}
	s.enrichChildTypeNames(ctx, results)
	return results, true
}

// childTypesOf returns the child entity types of each ancestor, via the per-ancestor cache when the
// service has one. Child types are intentionally independent of the search scope (target): they describe
// what exists under an entity, whichever data source it came from.
func (s *Service) childTypesOf(ctx context.Context, ancestors []string) (map[string][]string, error) {
	if s.cache != nil {
		return s.cache.ChildTypes(ctx, ancestors)
	}
	return fetchChildTypes(ctx, s.mixer, ancestors)
}

// matchChildTypes returns, in hint order, the child types for each hint: of the hint's resolved classes
// that are relevant (within v2ChildTypeRelevanceMargin of the hint's best class) and exist within the
// parent (generic types excluded, see isGenericPlaceType), the highest-scoring one plus any others scoring
// within v2ChildTypeScoreMargin of it, up to v2MaxChildTypesPerHint.
//
// One type per hint is the norm, since callers name the specific type they want (e.g. "county" and
// "administrative area 2"). Near-ties are kept because they usually mean an umbrella term over subtypes
// with no entities of their own, e.g. "school" -> HighSchool, ElementarySchool, MiddleSchool; picking one
// would silently narrow the request.
func matchChildTypes(hints []string, typesByHint map[string][]typeCandidate, available []string) []string {
	var matched []string
	for _, hint := range hints {
		candidates := typesByHint[strings.TrimSpace(hint)]
		var existing []typeCandidate
		for _, candidate := range candidates {
			if bestScore(candidates)-candidate.score > v2ChildTypeRelevanceMargin {
				continue
			}
			if slices.Contains(available, candidate.dcid) && !isGenericPlaceType(candidate.dcid) {
				existing = append(existing, candidate)
			}
		}
		for _, candidate := range nearTopTypes(existing) {
			if !slices.Contains(matched, candidate.dcid) {
				matched = append(matched, candidate.dcid)
			}
		}
	}
	return matched
}

// bestScore returns the highest score among candidates, or 0 for none.
func bestScore(candidates []typeCandidate) float64 {
	best := 0.0
	for _, candidate := range candidates {
		best = max(best, candidate.score)
	}
	return best
}

// nearTopTypes returns the highest-scoring candidate and those within v2ChildTypeScoreMargin of it,
// best first, capped at v2MaxChildTypesPerHint.
func nearTopTypes(candidates []typeCandidate) []typeCandidate {
	if len(candidates) == 0 {
		return nil
	}
	sorted := slices.Clone(candidates)
	slices.SortStableFunc(sorted, func(a, b typeCandidate) int { return cmp.Compare(b.score, a.score) })
	var picked []typeCandidate
	for _, candidate := range sorted {
		if len(picked) >= v2MaxChildTypesPerHint || sorted[0].score-candidate.score > v2ChildTypeScoreMargin {
			break
		}
		picked = append(picked, candidate)
	}
	return picked
}

// enrichChildTypeNames fills human-readable names for matched child types via one V2Node call.
// Failures are logged and the names are left empty.
func (s *Service) enrichChildTypeNames(ctx context.Context, results []*v2ChildTypesResult) {
	var dcids []string
	for _, result := range results {
		for _, childType := range result.types {
			dcids = append(dcids, childType.dcid)
		}
	}
	nodes, err := s.fetchNodeGraphs(ctx, dcids, "->name")
	if err != nil {
		slog.Warn("Failed to fetch child entity type names via V2Node", "error", err)
		return
	}
	for _, result := range results {
		for _, childType := range result.types {
			childType.name = getPropValue(nodes[childType.dcid], "name")
		}
	}
}

// buildChildEntityTypesTable formats child type results into a table, or returns nil when no query had
// child type hints. Queries with hints but no matching child type (or no resolved entity) get a
// placeholder row so every such query appears in the output.
func buildChildEntityTypesTable(results []*v2ChildTypesResult) *Table {
	if len(results) == 0 {
		return nil
	}
	rows := [][]any{}
	for _, result := range results {
		if len(result.types) == 0 {
			rows = append(rows, []any{result.query, result.parent, "", ""})
			continue
		}
		for _, childType := range result.types {
			rows = append(rows, []any{result.query, result.parent, childType.dcid, childType.name})
		}
	}
	return &Table{
		Columns: []string{colQuery, colEntityDcid, colChildEntityType, colChildEntityTypeName},
		Rows:    rows,
	}
}

// hasNonBlank reports whether any value is non-blank after trimming whitespace.
func hasNonBlank(values []string) bool {
	return slices.ContainsFunc(values, func(v string) bool { return strings.TrimSpace(v) != "" })
}
