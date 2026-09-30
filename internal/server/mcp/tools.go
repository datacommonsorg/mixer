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

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultPerSearchLimit  = 10
	defaultObservationDate = "latest"
	slotObservationAbout   = "observationAbout"
)

// McpTools holds dependencies for executing Data Commons MCP tools in-process.
type McpTools struct {
	agentService AgentBackend
	searchScope  string
}

// NewMcpTools creates a new McpTools instance backed by the provided AgentBackend.
func NewMcpTools(agentService AgentBackend, searchScope string) *McpTools {
	if agentService != nil {
		if v := reflect.ValueOf(agentService); v.Kind() == reflect.Ptr && v.IsNil() {
			agentService = nil
		}
	}
	return &McpTools{
		agentService: agentService,
		searchScope:  searchScope,
	}
}

// SearchIndicators searches for statistical indicators matching a natural language query.
func (t *McpTools) SearchIndicators(ctx context.Context, input SearchIndicatorsInput) (map[string]any, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	limit := resolveSearchLimit(input.PerSearchLimit)
	includeTopics := resolveIncludeTopics(input.IncludeTopics)
	req := &pbv2.SearchIndicatorsRequest{
		Query:          input.Query,
		Places:         input.Places,
		PerSearchLimit: limit,
		IncludeTopics:  &includeTopics,
	}
	if t.searchScope != "" {
		scope := t.searchScope
		req.Target = &scope
	}
	resp, err := t.agentService.SearchIndicators(ctx, req)
	if err != nil {
		return nil, err
	}
	return protoToMap(resp)
}

// SearchChildIndicators searches for statistical indicators available at the child-place level.
func (t *McpTools) SearchChildIndicators(ctx context.Context, input SearchChildIndicatorsInput) (map[string]any, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	limit := resolveSearchLimit(input.PerSearchLimit)
	includeTopics := resolveIncludeTopics(input.IncludeTopics)
	req := &pbv2.SearchIndicatorsRequest{
		Query:          input.Query,
		ParentPlace:    input.ParentPlace,
		Places:         input.SampleChildPlaces,
		PerSearchLimit: limit,
		IncludeTopics:  &includeTopics,
	}
	if t.searchScope != "" {
		scope := t.searchScope
		req.Target = &scope
	}
	resp, err := t.agentService.SearchIndicators(ctx, req)
	if err != nil {
		return nil, err
	}
	return protoToMap(resp)
}

// GetVariableMetadata retrieves definitions, coverage, and provenances for a list of variables.
func (t *McpTools) GetVariableMetadata(ctx context.Context, input GetVariableMetadataInput) (map[string]any, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	req := &pbv2.GetVariableMetadataRequest{
		VariableDcids: input.VariableDcids,
		EntityDcids:   input.EntityDcids,
	}
	resp, err := t.agentService.GetVariableMetadata(ctx, req)
	if err != nil {
		return nil, err
	}
	return protoToMap(resp)
}

// GetObservations fetches time-series observations for a statistical variable at a specific place.
func (t *McpTools) GetObservations(ctx context.Context, input GetObservationsInput) (map[string]any, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	placeListVal, err := newStringListValue([]string{input.PlaceDcid})
	if err != nil {
		return nil, err
	}
	entities := map[string]*structpb.Value{
		slotObservationAbout: placeListVal,
	}
	req := buildObservationsRequest(
		input.VariableDcid,
		entities,
		input.SourceOverride,
		input.Date,
		input.DateRangeStart,
		input.DateRangeEnd,
	)
	resp, err := t.agentService.GetObservations(ctx, req)
	if err != nil {
		return nil, err
	}
	return protoToMap(resp)
}

// GetChildObservations fetches time-series observations for a statistical variable across child places.
func (t *McpTools) GetChildObservations(ctx context.Context, input GetChildObservationsInput) (map[string]any, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	parentSpecVal, err := newParentPlaceStructValue(input.ParentPlaceDcid, input.ChildPlaceType)
	if err != nil {
		return nil, err
	}
	entities := map[string]*structpb.Value{
		slotObservationAbout: parentSpecVal,
	}
	req := buildObservationsRequest(
		input.VariableDcid,
		entities,
		input.SourceOverride,
		input.Date,
		input.DateRangeStart,
		input.DateRangeEnd,
	)
	resp, err := t.agentService.GetObservations(ctx, req)
	if err != nil {
		return nil, err
	}
	return protoToMap(resp)
}

// GetMultiEntityObservations fetches observations for multi-entity relationship statistical variables.
func (t *McpTools) GetMultiEntityObservations(ctx context.Context, input GetMultiEntityObservationsInput) (map[string]any, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	entities := make(map[string]*structpb.Value, len(input.Entities)+1)
	for prop, dcids := range input.Entities {
		listVal, err := newStringListValue(dcids)
		if err != nil {
			return nil, err
		}
		entities[prop] = listVal
	}

	hasAnyChildParam := input.ParentEntityProperty != "" || input.ParentEntityDcid != "" || input.ChildEntityType != ""
	if hasAnyChildParam {
		hasAllChildParams := input.ParentEntityProperty != "" && input.ParentEntityDcid != "" && input.ChildEntityType != ""
		if !hasAllChildParams {
			return nil, fmt.Errorf("to use child entity expansion, all of 'parent_entity_property', 'parent_entity_dcid', and 'child_entity_type' must be provided")
		}
		if _, exists := entities[input.ParentEntityProperty]; exists {
			return nil, fmt.Errorf("property '%s' cannot be specified in both 'entities' and child expansion parameters", input.ParentEntityProperty)
		}
		parentSpecVal, err := newParentPlaceStructValue(input.ParentEntityDcid, input.ChildEntityType)
		if err != nil {
			return nil, err
		}
		entities[input.ParentEntityProperty] = parentSpecVal
	}

	req := buildObservationsRequest(
		input.VariableDcid,
		entities,
		input.SourceOverride,
		input.Date,
		input.DateRangeStart,
		input.DateRangeEnd,
	)
	resp, err := t.agentService.GetObservations(ctx, req)
	if err != nil {
		return nil, err
	}
	return protoToMap(resp)
}

// resolveSearchLimit returns the per-search limit, defaulting to defaultPerSearchLimit when non-positive.
func resolveSearchLimit(limit int) int32 {
	if limit <= 0 {
		return defaultPerSearchLimit
	}
	return int32(limit)
}

// resolveIncludeTopics returns the include_topics flag, defaulting to true when nil.
func resolveIncludeTopics(includeTopics *bool) bool {
	if includeTopics == nil {
		return true
	}
	return *includeTopics
}

// buildObservationsRequest constructs a pbv2.GetObservationsRequest with optional date and source parameters.
func buildObservationsRequest(
	variableDcid string,
	entities map[string]*structpb.Value,
	sourceOverride string,
	date string,
	dateRangeStart string,
	dateRangeEnd string,
) *pbv2.GetObservationsRequest {
	resolvedDate := date
	if resolvedDate == "" {
		resolvedDate = defaultObservationDate
	}
	req := &pbv2.GetObservationsRequest{
		VariableDcid: variableDcid,
		Entities:     entities,
		Date:         &resolvedDate,
	}
	if sourceOverride != "" {
		req.SourceOverride = &sourceOverride
	}
	if dateRangeStart != "" {
		req.DateRangeStart = &dateRangeStart
	}
	if dateRangeEnd != "" {
		req.DateRangeEnd = &dateRangeEnd
	}
	return req
}

// newStringListValue creates a protobuf Value containing a list of string values.
func newStringListValue(items []string) (*structpb.Value, error) {
	anyItems := make([]any, len(items))
	for i, s := range items {
		anyItems[i] = s
	}
	return structpb.NewValue(anyItems)
}

// newParentPlaceStructValue creates a protobuf Value representing {"parent_dcid": ..., "child_type": ...}.
func newParentPlaceStructValue(parentDcid, childType string) (*structpb.Value, error) {
	return structpb.NewValue(map[string]any{
		"parent_dcid": parentDcid,
		"child_type":  childType,
	})
}

// protoToMap marshals a protobuf message to standard protojson and decodes it into a JSON map.
func protoToMap(msg proto.Message) (map[string]any, error) {
	if msg == nil {
		return map[string]any{}, nil
	}
	jsonBytes, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal protobuf response: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(jsonBytes, &out); err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf JSON response: %w", err)
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}
