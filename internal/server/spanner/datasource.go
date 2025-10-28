// Copyright 2024 Google LLC
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
	"context"
	"fmt"
	"log/slog"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SpannerDataSource represents a data source that interacts with Spanner.
type SpannerDataSource struct {
	client *SpannerClient
}

func NewSpannerDataSource(client *SpannerClient) *SpannerDataSource {
	return &SpannerDataSource{client: client}
}

// Type returns the type of the data source.
func (sds *SpannerDataSource) Type() datasource.DataSourceType {
	return datasource.TypeSpanner
}

// Id returns the id of the data source.
func (sds *SpannerDataSource) Id() string {
	return fmt.Sprintf("%s-%s", string(sds.Type()), sds.client.client.DatabaseName())
}

// Node retrieves node data from Spanner.
func (sds *SpannerDataSource) Node(ctx context.Context, req *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	arcs, err := v2.ParseProperty(req.GetProperty())
	if err != nil {
		return nil, err
	}
	if len(arcs) == 0 {
		return &pbv2.NodeResponse{}, nil
	}
	// Validate input.
	if len(arcs) > 1 {
		return nil, fmt.Errorf("multiple arcs in node request")
	}
	arc := arcs[0]
	if arc.Decorator != "" && (arc.SingleProp == "" || arc.SingleProp == WILDCARD || len(arc.BracketProps) > 0) {
		return nil, fmt.Errorf("chain expressions are only supported for a single property")
	}

	artifacts := processNodeRequest(arc)
	var resp *pbv2.NodeResponse
	if arc.SingleProp == "" && len(arc.BracketProps) == 0 {
		props, err := sds.client.GetNodeProps(ctx, req.Nodes, arc.Out)
		if err != nil {
			return nil, fmt.Errorf("error getting node properties: %v", err)
		}
		resp = nodePropsToNodeResponse(props)
	} else {
		offset, err := getOffset(req.NextToken, sds.Id())
		if err != nil {
			return nil, fmt.Errorf("error decoding pagination info: %v", err)
		}
		edges, err := sds.client.GetNodeEdgesByID(ctx, req.Nodes, arc, offset)
		if err != nil {
			return nil, fmt.Errorf("error getting node edges: %v", err)
		}
		resp, err = nodeEdgesToNodeResponse(req.Nodes, edges, sds.Id(), offset, artifacts)
		if err != nil {
			return nil, err
		}
	}
	processNodeResponse(resp, artifacts)
	return resp, nil
}

// Observation retrieves observation data from Spanner.
func (sds *SpannerDataSource) Observation(ctx context.Context, req *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	if req.Entity == nil {
		return nil, fmt.Errorf("entity must be specified")
	}

	entities, entityExpr := req.Entity.Dcids, req.Entity.Expression
	if len(entities) > 0 && entityExpr != "" {
		return nil, fmt.Errorf("only one of entity.dcids and entity.expression should be specified")
	}

	variables := []string{}
	if req.Variable != nil {
		// Variable expressions are not yet supported in Spanner.
		if req.Variable.Expression != "" {
			slog.Warn("Received spanner request with variable expression. Variable expressions are not yet supported in Spanner.", "expression", req.Variable.Expression)
			return nil, nil
		}
		variables = req.Variable.Dcids
	}
	if entityExpr != "" && len(variables) == 0 {
		return nil, fmt.Errorf("variable must be specified for entity.expression")
	}

	date := req.Date
	var observations []*Observation
	var err error

	if entityExpr != "" {
		containedInPlace, err := v2.ParseContainedInPlace(entityExpr)
		if err != nil {
			return nil, fmt.Errorf("error getting observations (contained in): %v", err)
		}
		observations, err = sds.client.GetObservationsContainedInPlace(ctx, variables, containedInPlace)
		if err != nil {
			return nil, fmt.Errorf("error getting observations (contained in): %v", err)
		}
	} else {
		observations, err = sds.client.GetObservations(ctx, variables, entities)
		if err != nil {
			return nil, fmt.Errorf("error getting observations: %v", err)
		}
	}

	observations = filterObservationsByDateAndFacet(observations, date, req.Filter)

	return observationsToObservationResponse(req, observations), nil
}

// NodeSearch searches nodes in the spanner graph.
func (sds *SpannerDataSource) NodeSearch(ctx context.Context, req *pbv2.NodeSearchRequest) (*pbv2.NodeSearchResponse, error) {
	nodes, err := sds.client.SearchNodes(ctx, req.Query, req.Types)
	if err != nil {
		return nil, fmt.Errorf("error searching nodes: %v", err)
	}
	return searchNodesToNodeSearchResponse(nodes), nil
}

// Resolve searches for nodes in the graph.
func (sds *SpannerDataSource) Resolve(ctx context.Context, req *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	arcs, err := v2.ParseProperty(req.GetProperty())
	if err != nil {
		return nil, err
	}

	if len(arcs) != 2 {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid property for resolving: %s", req.GetProperty())
	}

	inArc := arcs[0]
	outArc := arcs[1]
	if inArc.Out || !outArc.Out {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid property for resolving: %s", req.GetProperty())
	}

	if inArc.SingleProp == "geoCoordinate" && outArc.SingleProp == "dcid" {
		// Coordinate to ID:
		// Example:
		//   <-geoCoordinate->dcid
		// TODO: Support coordinate recon with Spanner.
		return nil, fmt.Errorf("unimplemented")
	}

	if inArc.SingleProp == "description" && outArc.SingleProp == "dcid" {
		// Description (name) to ID:
		// Examples:
		//   <-description->dcid
		//   <-description{typeOf:City}->dcid
		//   <-description{typeOf:[City, County]}->dcid
		// TODO: Support text resolution with Spanner.
		return nil, fmt.Errorf("unimplemented")
	}

	// ID to ID:
	// Example:
	//   <-wikidataId->nutsCode
	nodeToCandidates, err := sds.client.ResolveByID(ctx, req.GetNodes(), inArc.SingleProp, outArc.SingleProp)
	if err != nil {
		return nil, fmt.Errorf("error resolving ids: %v", err)
	}
	return candidatesToResolveResponse(nodeToCandidates), nil
}
