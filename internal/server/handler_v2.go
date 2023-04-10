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

	v1e "github.com/datacommonsorg/mixer/internal/server/v1/event"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2e "github.com/datacommonsorg/mixer/internal/server/v2/event"
	v2observation "github.com/datacommonsorg/mixer/internal/server/v2/observation"
	v2p "github.com/datacommonsorg/mixer/internal/server/v2/properties"
	v2pv "github.com/datacommonsorg/mixer/internal/server/v2/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// V2Resolve implements API for mixer.V2Resolve.
func (s *Server) V2Resolve(
	ctx context.Context, in *pbv2.ResolveRequest,
) (*pbv2.ResolveResponse, error) {
	arcs, err := v2.ParseProperty(in.GetProperty())
	if err != nil {
		return nil, err
	}

	if len(arcs) != 2 {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid property for resolving: %s", in.GetProperty())
	}

	inArc := arcs[0]
	outArc := arcs[1]
	if inArc.Out || !outArc.Out {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid property for resolving: %s", in.GetProperty())
	}

	if inArc.SingleProp == "geoCoordinate" && outArc.SingleProp == "dcid" {
		// Coordinate to ID:
		// Example:
		//   <-geoCoordinate
		return resolve.Coordinate(ctx, s.store, in.GetNodes())
	}

	if inArc.SingleProp == "description" && outArc.SingleProp == "dcid" {
		// Description (name) to ID:
		// Examples:
		//   <-description
		//   <-description{typeOf:City}
		typeOf := inArc.Filter["typeOf"] // Could be empty.
		return resolve.Description(
			ctx,
			s.store,
			s.mapsClient,
			in.GetNodes(),
			typeOf)
	}

	// ID to ID:
	// Example:
	//   <-wikidataId->nutsCode
	return resolve.ID(
		ctx,
		s.store,
		in.GetNodes(),
		inArc.SingleProp,
		outArc.SingleProp)
}

// V2Node implements API for mixer.V2Node.
func (s *Server) V2Node(
	ctx context.Context, in *pbv2.NodeRequest,
) (*pbv2.NodeResponse, error) {
	arcs, err := v2.ParseProperty(in.GetProperty())
	if err != nil {
		return nil, err
	}
	if len(arcs) == 1 {
		arc := arcs[0]
		direction := util.DirectionOut
		if !arc.Out {
			direction = util.DirectionIn
		}
		if arc.SingleProp != "" {
			if arc.Wildcard == "" {
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
			}
			if arc.Wildcard == "+" {
				// Examples:
				//   <-containedInPlace+{typeOf:City}
				return v2pv.LinkedPropertyValues(
					ctx,
					s.store,
					s.cache,
					in.GetNodes(),
					arc.SingleProp,
					direction,
					arc.Filter,
				)
			}
		} else { // arc.SingleProp == ""
			if len(arc.BracketProps) == 0 {
				// Examples:
				//   ->
				//   <-
				return v2p.API(ctx, s.store, in.GetNodes(), direction)
			}

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

// V2Event implements API for mixer.V2Event.
func (s *Server) V2Event(
	ctx context.Context, in *pbv2.EventRequest,
) (*pbv2.EventResponse, error) {
	arcs, err := v2.ParseProperty(in.GetProperty())
	if err != nil {
		return nil, err
	}

	// EventCollection.
	// Example:
	//   <-location{typeOf:FireEvent, date:2020-10, area:3.1#6.2#Acre}'
	if len(arcs) == 1 {
		arc := arcs[0]
		eventType, eventTypeOK := arc.Filter["typeOf"]
		date, dateOK := arc.Filter["date"]

		if !arc.Out &&
			arc.SingleProp == "location" &&
			(len(arc.Filter) == 2 || len(arc.Filter) == 3) &&
			eventTypeOK &&
			dateOK {
			var eventFilterSpec *v1e.FilterSpec
			hasEventFilter := len(arc.Filter) == 3
			if hasEventFilter {
				for k, v := range arc.Filter {
					if k == "typeOf" || k == "date" {
						continue
					}
					eventFilterSpec, err = v2e.ParseEventCollectionFilter(k, v)
					if err != nil {
						return nil, err
					}
				}
			} else {
				eventFilterSpec = nil
			}

			return v2e.EventCollection(
				ctx,
				s.store,
				in.GetNode(),
				eventType,
				date,
				eventFilterSpec)
		}

		return nil, status.Errorf(codes.InvalidArgument,
			"invalid property: %s", in.GetProperty())
	}

	// EventCollection.
	// Example:
	//   <-location{typeOf:FireEvent}->date
	if len(arcs) == 2 {
		arc1, arc2 := arcs[0], arcs[1]
		eventType, eventTypeOK := arc1.Filter["typeOf"]

		if !arc1.Out &&
			arc1.SingleProp == "location" &&
			eventTypeOK &&
			arc2.Out &&
			arc2.SingleProp == "date" {
			return v2e.EventCollectionDate(ctx, s.store, in.GetNode(), eventType)
		}

		return nil, status.Errorf(codes.InvalidArgument,
			"invalid property: %s", in.GetProperty())
	}

	return nil, nil
}

// V2Observation implements API for mixer.V2Observation.
func (s *Server) V2Observation(
	ctx context.Context, in *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	// (TODO): The routing logic here is very rough. This needs more work.
	var queryDate, queryValue, queryVariable, queryEntity bool
	for _, item := range in.GetSelect() {
		if item == "date" {
			queryDate = true
		} else if item == "value" {
			queryValue = true
		} else if item == "variable" {
			queryVariable = true
		} else if item == "entity" {
			queryEntity = true
		}
	}
	if !queryVariable || !queryEntity {
		return nil, status.Error(
			codes.InvalidArgument, "Must select 'variable' and 'entity'")
	}
	// Observation date and value query.
	if queryDate && queryValue {
		if len(in.GetVariable().GetDcids()) > 0 && len(in.GetEntity().GetDcids()) > 0 {
			return v2observation.FetchFromSeries(
				ctx,
				s.store,
				in.GetVariable().GetDcids(),
				in.GetEntity().GetDcids(),
				in.GetDate(),
			)
		}
		if len(in.GetVariable().GetDcids()) > 0 && in.GetEntity().GetExpression() != "" {
			// Example of expression
			// "geoId/06<-containedInPlace+{typeOf: City}"
			expr := in.GetEntity().GetExpression()
			g, err := v2.ParseLinkedNodes(expr)
			if err != nil {
				return nil, err
			}
			if len(g.Arcs) != 1 {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid expression string: %s", expr)
			}
			arc := g.Arcs[0]
			if arc.SingleProp != "containedInPlace" ||
				arc.Wildcard != "+" ||
				arc.Filter == nil ||
				arc.Filter["typeOf"] == "" {
				return nil, status.Errorf(
					codes.InvalidArgument, "invalid expression string: %s", expr)
			}
			return v2observation.FetchFromCollection(
				ctx,
				s.store,
				in.GetVariable().GetDcids(),
				g.Subject,
				arc.Filter["typeOf"],
				in.GetDate(),
			)
		}
	}

	// Get existence of <variable, entity> pair.
	if !queryDate && !queryValue {
		if len(in.GetEntity().GetDcids()) > 0 {
			if len(in.GetVariable().GetDcids()) > 0 {
				// Have both entity.dcids and variable.dcids. Check existence cache.
				return v2observation.Existence(
					ctx, s.store, in.GetVariable().GetDcids(), in.GetEntity().GetDcids())
			} else {
				// TODO: Support appending entities from entity.expression
				// Only have entity.dcids, fetch variables for each entity.
				return v2observation.Variable(ctx, s.store, in.GetEntity().GetDcids())
			}
		}
	}
	return &pbv2.ObservationResponse{}, nil
}
