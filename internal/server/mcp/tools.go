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
	"fmt"
	"reflect"

	"github.com/datacommonsorg/mixer/internal/agent"
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
func (t *McpTools) SearchIndicators(ctx context.Context, input SearchIndicatorsInput) (*agent.SearchIndicatorsResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	req := &agent.SearchIndicatorsRequest{
		Query:          input.Query,
		Places:         input.Places,
		PerSearchLimit: resolveSearchLimit(input.PerSearchLimit),
		IncludeTopics:  resolveIncludeTopics(input.IncludeTopics),
		Target:         t.searchScope,
	}
	return t.agentService.SearchIndicators(ctx, req)
}

// SearchChildIndicators searches for statistical indicators available at the child-place level.
func (t *McpTools) SearchChildIndicators(ctx context.Context, input SearchChildIndicatorsInput) (*agent.SearchIndicatorsResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	req := &agent.SearchIndicatorsRequest{
		Query:          input.Query,
		ParentPlace:    input.ParentPlace,
		Places:         input.SampleChildPlaces,
		PerSearchLimit: resolveSearchLimit(input.PerSearchLimit),
		IncludeTopics:  resolveIncludeTopics(input.IncludeTopics),
		Target:         t.searchScope,
	}
	return t.agentService.SearchIndicators(ctx, req)
}

// GetVariableMetadata retrieves definitions, coverage, and provenances for a list of variables.
func (t *McpTools) GetVariableMetadata(ctx context.Context, input GetVariableMetadataInput) (*agent.GetVariableMetadataResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	req := &agent.GetVariableMetadataRequest{
		VariableDcids: input.VariableDcids,
		EntityDcids:   input.EntityDcids,
	}
	return t.agentService.GetVariableMetadata(ctx, req)
}

// GetObservations fetches time-series observations for a statistical variable at a specific place.
func (t *McpTools) GetObservations(ctx context.Context, input GetObservationsInput) (*agent.GetObservationsResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	entities := map[string]*agent.EntityConstraint{
		slotObservationAbout: {
			Dcids: []string{input.PlaceDcid},
		},
	}
	req := buildObservationsRequest(
		input.VariableDcid,
		entities,
		input.SourceOverride,
		input.Date,
		input.DateRangeStart,
		input.DateRangeEnd,
	)
	return t.agentService.GetObservations(ctx, req)
}

// GetChildObservations fetches time-series observations for a statistical variable across child places.
func (t *McpTools) GetChildObservations(ctx context.Context, input GetChildObservationsInput) (*agent.GetObservationsResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	entities := map[string]*agent.EntityConstraint{
		slotObservationAbout: {
			ParentSpec: &agent.ParentPlaceSpec{
				ParentDcid: input.ParentPlaceDcid,
				ChildType:  input.ChildPlaceType,
			},
		},
	}
	req := buildObservationsRequest(
		input.VariableDcid,
		entities,
		input.SourceOverride,
		input.Date,
		input.DateRangeStart,
		input.DateRangeEnd,
	)
	return t.agentService.GetObservations(ctx, req)
}

// GetMultiEntityObservations fetches observations for multi-entity relationship statistical variables.
func (t *McpTools) GetMultiEntityObservations(ctx context.Context, input GetMultiEntityObservationsInput) (*agent.GetObservationsResponse, error) {
	if t.agentService == nil {
		return nil, fmt.Errorf("agent service is not initialized")
	}
	entities := make(map[string]*agent.EntityConstraint, len(input.Entities)+1)
	for prop, dcids := range input.Entities {
		entities[prop] = &agent.EntityConstraint{
			Dcids: dcids,
		}
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
		entities[input.ParentEntityProperty] = &agent.EntityConstraint{
			ParentSpec: &agent.ParentPlaceSpec{
				ParentDcid: input.ParentEntityDcid,
				ChildType:  input.ChildEntityType,
			},
		}
	}

	req := buildObservationsRequest(
		input.VariableDcid,
		entities,
		input.SourceOverride,
		input.Date,
		input.DateRangeStart,
		input.DateRangeEnd,
	)
	return t.agentService.GetObservations(ctx, req)
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

// buildObservationsRequest constructs an agent.GetObservationsRequest with optional date and source parameters.
func buildObservationsRequest(
	variableDcid string,
	entities map[string]*agent.EntityConstraint,
	sourceOverride string,
	date string,
	dateRangeStart string,
	dateRangeEnd string,
) *agent.GetObservationsRequest {
	resolvedDate := date
	if resolvedDate == "" {
		resolvedDate = defaultObservationDate
	}
	return &agent.GetObservationsRequest{
		VariableDcid:   variableDcid,
		Entities:       entities,
		SourceOverride: sourceOverride,
		Date:           resolvedDate,
		DateRangeStart: dateRangeStart,
		DateRangeEnd:   dateRangeEnd,
	}
}
