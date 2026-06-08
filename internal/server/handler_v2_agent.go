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

// Package server implements core Mixer handlers and data routing.
package server

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// V2AgentSearchIndicators implements API for mixer.V2AgentSearchIndicators.
// It delegates incoming RPC requests directly to the isolated agent.Service layer.
func (s *Server) V2AgentSearchIndicators(
	ctx context.Context,
	in *pbv2.SearchIndicatorsRequest,
) (*pbv2.SearchIndicatorsResponse, error) {
	return s.agentService.SearchIndicators(ctx, in)
}

// V2AgentGetObservations implements API for mixer.V2AgentGetObservations.
// It delegates incoming RPC requests directly to the isolated agent.Service layer.
func (s *Server) V2AgentGetObservations(
	ctx context.Context,
	in *pbv2.GetObservationsRequest,
) (*pbv2.GetObservationsResponse, error) {
	return s.agentService.GetObservations(ctx, in)
}
