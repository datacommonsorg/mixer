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

package spanner

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

// mockSpannerClient embeds SpannerClient to reduce boilerplate.
type mockSpannerClient struct {
	SpannerClient
	resolveByIDRes                     map[string][]string
	getNodeEdgesRes                    map[string][]*Edge
	checkVariableExistenceRes          [][]string
	filterNodesByTypeRes                 map[string][]string
	getObservationsRes                 []*Observation
	getObservationsContainedInPlaceRes []*Observation
}

func (m *mockSpannerClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error) {
	return nil, nil
}
func (m *mockSpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*Edge, error) {
	return m.getNodeEdgesRes, nil
}
func (m *mockSpannerClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	return m.getObservationsRes, nil
}
func (m *mockSpannerClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	return m.checkVariableExistenceRes, nil
}
func (m *mockSpannerClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	return m.getObservationsContainedInPlaceRes, nil
}
func (m *mockSpannerClient) ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error) {
	return m.resolveByIDRes, nil
}
func (m *mockSpannerClient) FilterNodesByTypes(ctx context.Context, nodes []string, typeFilters []string) (map[string][]string, error) {
	res := map[string][]string{}
	for _, typeFilter := range typeFilters {
		allowedNodes := m.filterNodesByTypeRes[typeFilter]
		for _, node := range nodes {
			if slices.Contains(allowedNodes, node) {
				res[node] = append(res[node], typeFilter)
			}
		}
	}
	return res, nil
}
func (m *mockSpannerClient) Id() string { return "mock" }

func TestSelectorClient_GetObservations(t *testing.T) {
	mockDefault := &mockSpannerClient{
		getObservationsRes: []*Observation{{VariableMeasured: "var_default"}},
	}
	mockNormalized := &mockSpannerClient{
		getObservationsRes: []*Observation{{VariableMeasured: "var_normalized"}},
	}

	client := &selectorClient{
		SpannerClient: mockDefault,
		normalized:    mockNormalized,
	}

	// Test Case 1: Default path (no header)
	ctx := context.Background()
	got, err := client.GetObservations(ctx, []string{"var1"}, []string{"entity1"})
	if err != nil {
		t.Fatalf("GetObservations failed: %v", err)
	}
	if len(got) != 1 || got[0].VariableMeasured != "var_default" {
		t.Errorf("Expected var_default, got %v", got)
	}

	// Test Case 2: Normalized path (header set to true)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(util.XUseNormalizedSchema, "true"))
	
	// Capture slog output
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)
	logger := slog.New(handler)
	originalLogger := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(originalLogger)

	got, err = client.GetObservations(ctx, []string{"var1"}, []string{"entity1"})
	if err != nil {
		t.Fatalf("GetObservations failed: %v", err)
	}
	if len(got) != 1 || got[0].VariableMeasured != "var_normalized" {
		t.Errorf("Expected var_normalized, got %v", got)
	}
	if !strings.Contains(buf.String(), "Invoking normalized Spanner schema") {
		t.Errorf("Expected log not found. Logs: %s", buf.String())
	}
}

func TestSelectorClient_CheckVariableExistence(t *testing.T) {
	mockDefault := &mockSpannerClient{
		checkVariableExistenceRes: [][]string{{"var_default"}},
	}
	mockNormalized := &mockSpannerClient{
		checkVariableExistenceRes: [][]string{{"var_normalized"}},
	}

	client := &selectorClient{
		SpannerClient: mockDefault,
		normalized:    mockNormalized,
	}

	// Test Case 1: Default path
	ctx := context.Background()
	got, err := client.CheckVariableExistence(ctx, []string{"var1"}, []string{"entity1"})
	if err != nil {
		t.Fatalf("CheckVariableExistence failed: %v", err)
	}
	if len(got) != 1 || got[0][0] != "var_default" {
		t.Errorf("Expected var_default, got %v", got)
	}

	// Test Case 2: Normalized path
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(util.XUseNormalizedSchema, "true"))
	got, err = client.CheckVariableExistence(ctx, []string{"var1"}, []string{"entity1"})
	if err != nil {
		t.Fatalf("CheckVariableExistence failed: %v", err)
	}
	if len(got) != 1 || got[0][0] != "var_normalized" {
		t.Errorf("Expected var_normalized, got %v", got)
	}
}
