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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
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
	// v2MaxChildTypesPerHint caps the child types returned per hint.
	v2MaxChildTypesPerHint = 3
)

// resolveChildEntityTypes finds, for each query with child type hints, the hinted child types that exist
// within the query's best-matching entity, and records them on its group.
//
// The child types of all best matches are fetched in one call (served from the Spanner LinkedEdge table,
// and cached per ancestor), then filtered by the hints (see matchChildTypes). Only the best match is
// used; callers wanting another candidate's child types can re-resolve with a more specific name.
// It reports whether the child types are usable: false if the lookup failed. Failures are logged rather
// than returned, since child types are supplementary to the entity resolution result, and the caller
// omits the child types table so a failed lookup is not mistaken for "no such child types".
func (s *Service) resolveChildEntityTypes(
	ctx context.Context,
	queries []*V2EntityQuery,
	groups []*v2EntityGroup,
	typesByHint map[string][]typeCandidate,
) bool {
	var parents []string
	for _, group := range groups {
		if !group.hasChildHints || len(group.entities) == 0 {
			continue
		}
		group.childTypeParent = group.entities[0].dcid
		if !slices.Contains(parents, group.childTypeParent) {
			parents = append(parents, group.childTypeParent)
		}
	}
	if len(parents) == 0 {
		return true
	}

	lookupCtx, cancel := context.WithTimeout(ctx, v2ChildTypesTimeout)
	defer cancel()
	available, err := s.childTypesOf(lookupCtx, parents)
	if err != nil {
		slog.Warn("Failed to fetch child entity types", "error", err, "parentsCount", len(parents))
		return false
	}
	for i, query := range queries {
		group := groups[i]
		if group.childTypeParent == "" {
			continue
		}
		for _, dcid := range matchChildTypes(query.ChildEntityTypeHints, typesByHint, available[group.childTypeParent]) {
			group.childTypes = append(group.childTypes, &v2ChildType{dcid: dcid})
		}
	}
	s.enrichChildTypeNames(ctx, groups)
	return true
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
// that exist within the parent (generic types excluded, see isGenericPlaceType), the highest-scoring one
// plus any others scoring within v2ChildTypeScoreMargin of it, up to v2MaxChildTypesPerHint.
//
// One type per hint is the norm, since callers name the specific type they want (e.g. "county" and
// "administrative area 2"). Near-ties are kept because they usually mean an umbrella term over subtypes
// with no entities of their own, e.g. "school" -> HighSchool, ElementarySchool, MiddleSchool; picking one
// would silently narrow the request.
func matchChildTypes(hints []string, typesByHint map[string][]typeCandidate, available []string) []string {
	var matched []string
	for _, hint := range hints {
		var existing []typeCandidate
		for _, candidate := range typesByHint[strings.TrimSpace(hint)] {
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
func (s *Service) enrichChildTypeNames(ctx context.Context, groups []*v2EntityGroup) {
	var dcids []string
	for _, group := range groups {
		for _, childType := range group.childTypes {
			if !slices.Contains(dcids, childType.dcid) {
				dcids = append(dcids, childType.dcid)
			}
		}
	}
	if len(dcids) == 0 {
		return
	}
	nodeResp, err := s.fetchAllNodes(ctx, &pbv2.NodeRequest{Nodes: dcids, Property: "->name"})
	if err != nil {
		slog.Warn("Failed to fetch child entity type names via V2Node", "error", err, "dcidsCount", len(dcids))
		return
	}
	for _, group := range groups {
		for _, childType := range group.childTypes {
			childType.name = getPropValue(nodeResp.GetData()[childType.dcid], "name")
		}
	}
}

// buildChildEntityTypesTable formats matched child types into a table, or returns nil when no query
// had child type hints. Queries with hints but no matching child type (or no resolved entity) get a
// placeholder row so every such query appears in the output.
func buildChildEntityTypesTable(groups []*v2EntityGroup) *Table {
	rows := [][]any{}
	for _, group := range groups {
		if !group.hasChildHints {
			continue
		}
		if len(group.childTypes) == 0 {
			rows = append(rows, []any{group.query, group.childTypeParent, "", ""})
			continue
		}
		for _, childType := range group.childTypes {
			rows = append(rows, []any{group.query, group.childTypeParent, childType.dcid, childType.name})
		}
	}
	if len(rows) == 0 {
		return nil
	}
	return &Table{
		Columns: []string{colQuery, colEntityDcid, colChildEntityType, colChildEntityTypeName},
		Rows:    rows,
	}
}
