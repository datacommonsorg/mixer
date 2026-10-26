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
	"strconv"
	"strings"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// Type hints are natural-language type names (e.g. "county", "food bank") that callers attach to entity
// names (entity_type_hints) and to the entities they want within them (child_entity_type_hints). They are
// resolved to Data Commons classes once per request and shared by entity routing and child type matching.

const (
	// v2MaxTypesPerHint caps the Class DCIDs used per entity type hint. The type resolver can return many
	// loosely related classes; the top few cover close variants (e.g. School, PublicSchool) without diluting
	// filters.
	v2MaxTypesPerHint = 3
	// v2MaxTypeCandidatesPerHint caps the Class DCIDs kept per hint from the type resolver. It is larger
	// than v2MaxTypesPerHint because child type hints filter candidates by existence under the parent
	// (see matchChildTypes), so the wanted type can rank below generic or absent classes.
	v2MaxTypeCandidatesPerHint = 10
)

// typeCandidate is a Class DCID resolved from a natural-language type hint, with its similarity score.
type typeCandidate struct {
	dcid  string
	score float64
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
		slog.Error("Type hint resolution failed", "hintsCount", len(hints), "error", err)
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
