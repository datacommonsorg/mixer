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

// Package server is the main server
package server

import (
	"context"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2observation "github.com/datacommonsorg/mixer/internal/server/v2/observation"
	"github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"googlemaps.github.io/maps"
)

// V2ResolveCore gets resolve results from Cloud Bigtable and Maps API.
func V2ResolveCore(
	ctx context.Context,
	store *store.Store,
	mapsClient *maps.Client,
	in *pbv2.ResolveRequest,
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
		return resolve.Coordinate(ctx, store, in.GetNodes())
	}

	if inArc.SingleProp == "description" && outArc.SingleProp == "dcid" {
		// Description (name) to ID:
		// Examples:
		//   <-description
		//   <-description{typeOf:City}
		typeOf := inArc.Filter["typeOf"] // Could be empty.
		return resolve.Description(
			ctx,
			store,
			mapsClient,
			in.GetNodes(),
			typeOf)
	}

	// ID to ID:
	// Example:
	//   <-wikidataId->nutsCode
	return resolve.ID(
		ctx,
		store,
		in.GetNodes(),
		inArc.SingleProp,
		outArc.SingleProp)
}

// V2ObservationBigtable fetches observation from Cloud Bigtable.
func V2ObservationCore(
	ctx context.Context, store *store.Store, in *pbv2.ObservationRequest,
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

	variable := in.GetVariable()
	entity := in.GetEntity()

	// Observation date and value query.
	if queryDate && queryValue {
		// Series.
		if len(variable.GetDcids()) > 0 && len(entity.GetDcids()) > 0 {
			return v2observation.FetchFromSeries(
				ctx,
				store,
				variable.GetDcids(),
				entity.GetDcids(),
				in.GetDate(),
			)
		}

		// Collection.
		if len(variable.GetDcids()) > 0 && entity.GetExpression() != "" {
			// Example of expression
			// "geoId/06<-containedInPlace+{typeOf: City}"
			expr := entity.GetExpression()
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
				store,
				variable.GetDcids(),
				g.Subject,
				arc.Filter["typeOf"],
				in.GetDate(),
			)
		}

		// Derived series.
		if variable.GetFormula() != "" && len(entity.GetDcids()) > 0 {
			return v2observation.DerivedSeries(
				ctx,
				store,
				variable.GetFormula(),
				entity.GetDcids(),
			)
		}
	}

	// Get existence of <variable, entity> pair.
	if !queryDate && !queryValue {
		if len(entity.GetDcids()) > 0 {
			if len(variable.GetDcids()) > 0 {
				// Have both entity.dcids and variable.dcids. Check existence cache.
				return v2observation.Existence(
					ctx, store, variable.GetDcids(), entity.GetDcids())
			}
			// TODO: Support appending entities from entity.expression
			// Only have entity.dcids, fetch variables for each entity.
			return v2observation.Variable(ctx, store, entity.GetDcids())
		}
	}
	return &pbv2.ObservationResponse{}, nil
}
