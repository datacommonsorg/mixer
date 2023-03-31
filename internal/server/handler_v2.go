// Copyright 2023 Google LLC
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

// Package server
package server

import (
	"context"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2observation "github.com/datacommonsorg/mixer/internal/server/v2/observation"
	v2pv "github.com/datacommonsorg/mixer/internal/server/v2/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/util"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// V2Node implements API for mixer.V2Node.
func (s *Server) V2Node(
	ctx context.Context, in *pbv2.NodeRequest,
) (*pbv2.NodeResponse, error) {
	arcStrings, err := v2.SplitArc(in.GetProperty())
	if err != nil {
		return nil, err
	}
	arcs := []*v2.Arc{}
	for _, s := range arcStrings {
		arc, err := v2.ParseArc(s)
		if err != nil {
			return nil, err
		}
		arcs = append(arcs, arc)
	}
	// TODO: abstract this out to a router module.
	// Simple Property Values
	// Examples:
	//   ->name
	//   <-containedInPlace
	//   ->[name, address]
	if len(arcs) == 1 {
		arc := arcs[0]
		direction := util.DirectionOut
		if !arc.Out {
			direction = util.DirectionIn
		}
		if arc.SingleProp != "" && arc.Wildcard == "" {
			// Examples:
			//   ->name
			//   <-containedInPlace
			return v2pv.API(
				ctx,
				s.store,
				in.GetNodes(),
				[]string{arc.SingleProp},
				direction,
				int(in.GetLimit()),
				in.NextToken,
			)
		} else if len(arc.BracketProps) > 0 {
			// Examples:
			//   ->[name, address]
			return v2pv.API(
				ctx,
				s.store,
				in.GetNodes(),
				arc.BracketProps,
				direction,
				int(in.GetLimit()),
				in.GetNextToken(),
			)
		}
	}
	return nil, nil
}

// V2Observation implements API for mixer.V2Observation.
func (s *Server) V2Observation(
	ctx context.Context, in *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	return v2observation.Series(ctx, s.store, in.GetEntities(), in.GetVariables())
}
