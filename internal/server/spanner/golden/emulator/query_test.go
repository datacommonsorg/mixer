// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emulator

import (
	"context"
	"sort"
	"testing"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
)

// ---------------------------------------------------------------------------
// GetNodeProps
// ---------------------------------------------------------------------------

func TestGetNodeProps_OutArcs(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	props, err := client.GetNodeProps(context.Background(), []string{"Count_Person", "Count_Migration"}, true)
	if err != nil {
		t.Fatalf("GetNodeProps() error = %v", err)
	}

	// Count_Person should have a typeOf edge.
	personProps := props["Count_Person"]
	if personProps == nil {
		t.Fatal("props[Count_Person] = nil")
	}
	if len(personProps) != 1 {
		t.Fatalf("len(props[Count_Person]) = %d, want 1", len(personProps))
	}
	if personProps[0].Predicate != "typeOf" {
		t.Errorf("props[Count_Person][0].Predicate = %q, want %q", personProps[0].Predicate, "typeOf")
	}

	// Count_Migration should have typeOf + 2 observationProperties = 3 edges.
	migrationProps := props["Count_Migration"]
	if migrationProps == nil {
		t.Fatal("props[Count_Migration] = nil")
	}
	if len(migrationProps) != 3 {
		t.Fatalf("len(props[Count_Migration]) = %d, want 3", len(migrationProps))
	}
}

func TestGetNodeProps_MissingIDReturnsEmpty(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	props, err := client.GetNodeProps(context.Background(), []string{"NonExistentNode"}, true)
	if err != nil {
		t.Fatalf("GetNodeProps() error = %v", err)
	}
	if props["NonExistentNode"] == nil {
		t.Fatal("props[NonExistentNode] = nil, want empty slice")
	}
	if len(props["NonExistentNode"]) != 0 {
		t.Fatalf("len(props[NonExistentNode]) = %d, want 0", len(props["NonExistentNode"]))
	}
}

func TestGetNodeProps_EmptyInput(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	props, err := client.GetNodeProps(context.Background(), []string{}, true)
	if err != nil {
		t.Fatalf("GetNodeProps() error = %v", err)
	}
	if len(props) != 0 {
		t.Fatalf("len(props) = %d, want 0", len(props))
	}
}

// ---------------------------------------------------------------------------
// GetNodeEdgesByID
// ---------------------------------------------------------------------------

func TestGetNodeEdgesByID_OutArcsSingleProp(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	edges, err := client.GetNodeEdgesByID(
		context.Background(),
		[]string{"Count_Person"},
		&v2.Arc{Out: true, SingleProp: "typeOf"},
		500, 0,
	)
	if err != nil {
		t.Fatalf("GetNodeEdgesByID() error = %v", err)
	}

	personEdges := edges["Count_Person"]
	if personEdges == nil {
		t.Fatal("edges[Count_Person] = nil")
	}
	if len(personEdges) != 1 {
		t.Fatalf("len(edges[Count_Person]) = %d, want 1", len(personEdges))
	}
	if personEdges[0].Predicate != "typeOf" {
		t.Errorf("edges[Count_Person][0].Predicate = %q, want %q", personEdges[0].Predicate, "typeOf")
	}
	if personEdges[0].Value != "StatisticalVariable" {
		t.Errorf("edges[Count_Person][0].Value = %q, want %q", personEdges[0].Value, "StatisticalVariable")
	}
}

func TestGetNodeEdgesByID_OutArcsWildcard(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	edges, err := client.GetNodeEdgesByID(
		context.Background(),
		[]string{"Count_Migration"},
		&v2.Arc{Out: true, SingleProp: "*"},
		500, 0,
	)
	if err != nil {
		t.Fatalf("GetNodeEdgesByID() error = %v", err)
	}

	migrationEdges := edges["Count_Migration"]
	if migrationEdges == nil {
		t.Fatal("edges[Count_Migration] = nil")
	}
	// typeOf + 2 observationProperties = 3 edges.
	if len(migrationEdges) != 3 {
		t.Fatalf("len(edges[Count_Migration]) = %d, want 3", len(migrationEdges))
	}

	// Verify the node metadata is populated.
	for _, edge := range migrationEdges {
		if edge.Value == "" {
			t.Errorf("edge.Value is empty for predicate %q", edge.Predicate)
		}
	}
}

func TestGetNodeEdgesByID_InArcs(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	// Find all nodes that have a typeOf edge pointing TO StatisticalVariable.
	edges, err := client.GetNodeEdgesByID(
		context.Background(),
		[]string{"StatisticalVariable"},
		&v2.Arc{Out: false, SingleProp: "*"},
		500, 0,
	)
	if err != nil {
		t.Fatalf("GetNodeEdgesByID() error = %v", err)
	}

	svEdges := edges["StatisticalVariable"]
	if svEdges == nil {
		t.Fatal("edges[StatisticalVariable] = nil")
	}
	// Count_Migration, Count_Person, Count_Household all have typeOf -> StatisticalVariable.
	if len(svEdges) != 3 {
		t.Fatalf("len(edges[StatisticalVariable]) = %d, want 3", len(svEdges))
	}

	// Verify all edges have predicate "typeOf".
	for _, edge := range svEdges {
		if edge.Predicate != "typeOf" {
			t.Errorf("edge.Predicate = %q, want %q", edge.Predicate, "typeOf")
		}
	}
}

func TestGetNodeEdgesByID_MultipleIDs(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	edges, err := client.GetNodeEdgesByID(
		context.Background(),
		[]string{"Count_Person", "Count_Household"},
		&v2.Arc{Out: true, SingleProp: "typeOf"},
		500, 0,
	)
	if err != nil {
		t.Fatalf("GetNodeEdgesByID() error = %v", err)
	}

	if edges["Count_Person"] == nil || len(edges["Count_Person"]) != 1 {
		t.Errorf("edges[Count_Person] = %v, want 1 edge", edges["Count_Person"])
	}
	if edges["Count_Household"] == nil || len(edges["Count_Household"]) != 1 {
		t.Errorf("edges[Count_Household] = %v, want 1 edge", edges["Count_Household"])
	}
}

func TestGetNodeEdgesByID_EmptyInput(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	edges, err := client.GetNodeEdgesByID(
		context.Background(),
		[]string{},
		&v2.Arc{Out: true, SingleProp: "*"},
		500, 0,
	)
	if err != nil {
		t.Fatalf("GetNodeEdgesByID() error = %v", err)
	}
	if len(edges) != 0 {
		t.Fatalf("len(edges) = %d, want 0", len(edges))
	}
}

// ---------------------------------------------------------------------------
// ResolveByID
// ---------------------------------------------------------------------------

func TestResolveByID_DcidToDcid(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	result, err := client.ResolveByID(
		context.Background(),
		[]string{"Count_Person", "NonExistent"},
		"dcid", "dcid",
	)
	if err != nil {
		t.Fatalf("ResolveByID() error = %v", err)
	}

	// Count_Person should resolve to itself.
	candidates, ok := result["Count_Person"]
	if !ok {
		t.Fatal("result[Count_Person] not found")
	}
	if len(candidates) != 1 || candidates[0] != "Count_Person" {
		t.Errorf("result[Count_Person] = %v, want [Count_Person]", candidates)
	}

	// NonExistent should not be in the result.
	if _, ok := result["NonExistent"]; ok {
		t.Error("result[NonExistent] should not be present")
	}
}

func TestResolveByID_EmptyInput(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	result, err := client.ResolveByID(
		context.Background(),
		[]string{},
		"dcid", "dcid",
	)
	if err != nil {
		t.Fatalf("ResolveByID() error = %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("len(result) = %d, want 0", len(result))
	}
}

// ---------------------------------------------------------------------------
// FilterNodesByTypes
// ---------------------------------------------------------------------------

func TestFilterNodesByTypes(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	result, err := client.FilterNodesByTypes(
		context.Background(),
		[]string{"destinationCountry", "geoId/06", "Count_Person"},
		[]string{"ObservationProperty"},
	)
	if err != nil {
		t.Fatalf("FilterNodesByTypes() error = %v", err)
	}

	// destinationCountry has type ObservationProperty.
	matched, ok := result["destinationCountry"]
	if !ok {
		t.Fatal("result[destinationCountry] not found")
	}
	if len(matched) != 1 || matched[0] != "ObservationProperty" {
		t.Errorf("result[destinationCountry] = %v, want [ObservationProperty]", matched)
	}

	// geoId/06 has types State and AdministrativeArea1 — neither matches.
	if _, ok := result["geoId/06"]; ok {
		t.Error("result[geoId/06] should not be present (no matching types)")
	}

	// Count_Person has no types — should not match.
	if _, ok := result["Count_Person"]; ok {
		t.Error("result[Count_Person] should not be present (no types)")
	}
}

func TestFilterNodesByTypes_EmptyInput(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	result, err := client.FilterNodesByTypes(
		context.Background(),
		[]string{},
		[]string{"ObservationProperty"},
	)
	if err != nil {
		t.Fatalf("FilterNodesByTypes() error = %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("len(result) = %d, want 0", len(result))
	}
}

// ---------------------------------------------------------------------------
// GetEventCollectionDate
// ---------------------------------------------------------------------------

// Note: Event collection queries require event edges (typeOf=FireEvent, affectedPlace, startDate).
// The current seed data does not include events, so these tests verify empty/edge cases only.

func TestGetEventCollectionDate_NoData(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	// No events in seed data for this place/type combination.
	dates, err := client.GetEventCollectionDate(
		context.Background(),
		"country/USA", "FireEvent",
	)
	if err != nil {
		t.Fatalf("GetEventCollectionDate() error = %v", err)
	}
	if len(dates) != 0 {
		t.Fatalf("len(dates) = %d, want 0 (no events in seed data)", len(dates))
	}
}

// ---------------------------------------------------------------------------
// Helper: sort edges for deterministic comparison
// ---------------------------------------------------------------------------

func sortEdges(edges map[string][]*mixerspanner.Edge) {
	for _, es := range edges {
		sort.Slice(es, func(i, j int) bool {
			return es[i].Predicate < es[j].Predicate
		})
	}
}

// ---------------------------------------------------------------------------
// Verify emulator seed data integrity
// ---------------------------------------------------------------------------

func TestEmulatorSeedData_NodeCount(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	// Verify that nodes we expect exist.
	props, err := client.GetNodeProps(context.Background(), []string{
		"Count_Person", "Count_Migration", "Count_Household",
		"StatisticalVariable", "geoId/06", "country/USA",
	}, true)
	if err != nil {
		t.Fatalf("GetNodeProps() error = %v", err)
	}

	for _, id := range []string{"Count_Person", "Count_Migration", "Count_Household", "StatisticalVariable", "geoId/06", "country/USA"} {
		if _, ok := props[id]; !ok {
			t.Errorf("props[%q] not found in seed data", id)
		}
	}
}

func TestEmulatorSeedData_EdgeCount(t *testing.T) {
	t.Parallel()
	client := requireSuite(t).spannerClient

	edges, err := client.GetNodeEdgesByID(
		context.Background(),
		[]string{"Count_Migration"},
		&v2.Arc{Out: true, SingleProp: "*"},
		500, 0,
	)
	if err != nil {
		t.Fatalf("GetNodeEdgesByID() error = %v", err)
	}

	migrationEdges := edges["Count_Migration"]
	if migrationEdges == nil {
		t.Fatal("edges[Count_Migration] = nil")
	}

	predicates := map[string]bool{}
	for _, e := range migrationEdges {
		predicates[e.Predicate] = true
	}

	// Count_Migration should have typeOf, observationProperties (sourceCountry, destinationCountry).
	expectedPredicates := []string{"typeOf", "observationProperties"}
	for _, p := range expectedPredicates {
		if !predicates[p] {
			t.Errorf("predicate %q not found in Count_Migration edges", p)
		}
	}
}
