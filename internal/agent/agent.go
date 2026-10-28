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

// Package agent implements the consolidated business logic and L1 caching for agentic AI helper tools.
package agent

import (
	"context"
	"fmt"
	"slices"

	"github.com/datacommonsorg/mixer/internal/nodefetcher"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// Mixer defines the strict subset of Mixer API capabilities
// required by the agent package in-process.
type Mixer interface {
	// V2Resolve resolves entities matching coordinates, Wikidata IDs, description names,
	// or executes vector similarity searches for matched indicators.
	V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error)

	// V2Observation fetches observations for a batch of variables and entities to check availability.
	V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error)

	// V2Node fetches property values for a list of entities.
	V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error)

	// V2BulkVariableInfo fetches summary metadata for statistical variables.
	V2BulkVariableInfo(ctx context.Context, in *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error)

	// SdmxData fetches structured SDMX data.
	SdmxData(ctx context.Context, in *sdmxpb.SdmxDataQuery) (*sdmxpb.SdmxDataResult, error)

	// SdmxAvailability fetches available SDMX component values.
	SdmxAvailability(ctx context.Context, in *sdmxpb.SdmxAvailabilityQuery) (*sdmxpb.SdmxAvailabilityResult, error)

	// ChildTypes fetches the types of entities contained in each ancestor entity, via the (transitive)
	// containedInPlace relation.
	ChildTypes(ctx context.Context, in *pbv2.ChildTypesRequest) (*pbv2.ChildTypesResponse, error)

	// SampleChildEntities fetches, for each ancestor entity, a fixed sample of the entities of one type
	// contained (transitively, via containedInPlace) in it.
	SampleChildEntities(ctx context.Context, in *pbv2.SampleChildEntitiesRequest) (*pbv2.SampleChildEntitiesResponse, error)
}

// Service orchestrates the aggregation and formatting of lower-level API data in response
// to conversational assistant/agent queries.
type Service struct {
	mixer               Mixer
	cache               *Cache
	defaultExpandTopics bool
	placeTypes          placeTypeCache
}

// ServiceOptions holds optional configuration for agent.Service.
type ServiceOptions struct {
	DefaultExpandTopics *bool
}

// NewService constructs a new Service instance backed by the provided Mixer and Cache.
func NewService(mixer Mixer, cache *Cache, opts *ServiceOptions) *Service {
	defaultExpandTopics := true
	if opts != nil && opts.DefaultExpandTopics != nil {
		defaultExpandTopics = *opts.DefaultExpandTopics
	}
	return &Service{
		mixer:               mixer,
		cache:               cache,
		defaultExpandTopics: defaultExpandTopics,
	}
}

// Reset flushes all cached state inside the service L1 Cache atomically,
// called when databases undergo static reloads.
func (s *Service) Reset() {
	if s.cache != nil {
		s.cache.Reset()
	}
	s.placeTypes.reset()
}

// fetchAllNodes fetches all pages of a V2Node request. Large node lists are split
// into chunks that are fetched in parallel.
//
// If any chunk fails the whole call fails, so callers that log and continue get
// nothing back instead of a partial result.
func (s *Service) fetchAllNodes(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	return nodefetcher.NodeFetchAllChunkedFunc(ctx, s.mixer.V2Node, req)
}

// fetchNodeGraphs fetches the given property for the unique, non-empty DCIDs (in first-seen order) via
// fetchAllNodes and returns the node graphs keyed by DCID, or nil when there is nothing to fetch.
func (s *Service) fetchNodeGraphs(ctx context.Context, dcids []string, property string) (map[string]*pbv2.LinkedGraph, error) {
	var unique []string
	for _, dcid := range dcids {
		if dcid != "" && !slices.Contains(unique, dcid) {
			unique = append(unique, dcid)
		}
	}
	if len(unique) == 0 {
		return nil, nil
	}
	resp, err := s.fetchAllNodes(ctx, &pbv2.NodeRequest{Nodes: unique, Property: property})
	if err != nil {
		return nil, fmt.Errorf("fetch %d nodes: %w", len(unique), err)
	}
	return resp.GetData(), nil
}
