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

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

const (
	ResolverPlace     = "place"
	ResolverIndicator = "indicator"
	PropDescription   = "<-description->dcid"
	DateLatest        = "LATEST"
)

// Mixer defines the strict subset of Mixer V2 API capabilities
// required by the agent package in-process.
type Mixer interface {
	// V2Resolve resolves entities matching coordinates, Wikidata IDs, description names,
	// or executes vector similarity searches for matched indicators.
	V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error)

	// V2Observation fetches observations for a batch of variables and entities to check availability.
	V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error)
}

// Service orchestrates the aggregation and formatting of lower-level API data in response
// to conversational assistant/agent queries.
type Service struct {
	mixer Mixer
	cache *Cache
}

// NewService constructs a new Service instance backed by the provided Mixer and Cache.
func NewService(mixer Mixer, cache *Cache) *Service {
	return &Service{
		mixer: mixer,
		cache: cache,
	}
}
