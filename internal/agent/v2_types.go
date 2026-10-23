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

// V2EntityQuery is a single entity name to resolve, with optional natural-language type hints.
type V2EntityQuery struct {
	Name            string
	EntityTypeHints []string
}

// V2ResolveEntitiesRequest holds parameters for V2ResolveEntities.
type V2ResolveEntitiesRequest struct {
	Entities []*V2EntityQuery
	Target   string
}

// V2ResolveEntitiesResponse holds the resolved entities as a table with columns query, dcid, name and
// types. Each query has one row per candidate (best match first), or a single row with an empty dcid
// when it could not be resolved, so every requested name appears in the output. The tabular shape
// matches other agent tool responses and avoids repeating keys for every candidate.
type V2ResolveEntitiesResponse struct {
	ResolvedEntities *Table `json:"resolved_entities"`
}

// v2ResolvedEntity is a resolved entity DCID with its canonical name and types. Types are returned so
// callers can disambiguate same-named entities (e.g. Georgia the State vs. the Country).
type v2ResolvedEntity struct {
	dcid  string
	name  string
	types []string
}

// v2EntityGroup holds the resolved candidates for one queried name.
type v2EntityGroup struct {
	query    string
	entities []*v2ResolvedEntity
}
