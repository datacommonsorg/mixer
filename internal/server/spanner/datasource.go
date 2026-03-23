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
	"sort"

	internalmaps "github.com/datacommonsorg/mixer/internal/maps"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2e "github.com/datacommonsorg/mixer/internal/server/v2/event"
	v3 "github.com/datacommonsorg/mixer/internal/server/v3"
	"github.com/datacommonsorg/mixer/internal/store/files"
	"github.com/datacommonsorg/mixer/internal/translator/sparql"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SpannerDataSource represents a data source that interacts with Spanner.
type SpannerDataSource struct {
	client          SpannerClient
	recogPlaceStore *files.RecogPlaceStore
	mapsClient      internalmaps.MapsClient
}

func NewSpannerDataSource(
	client SpannerClient,
	recogPlaceStore *files.RecogPlaceStore,
	mapsClient internalmaps.MapsClient,
) *SpannerDataSource {
	return &SpannerDataSource{
		client:          client,
		recogPlaceStore: recogPlaceStore,
		mapsClient:      mapsClient,
	}
}

// Type returns the type of the data source.
func (sds *SpannerDataSource) Type() datasource.DataSourceType {
	return datasource.TypeSpanner
}

// Id returns the id of the data source.
func (sds *SpannerDataSource) Id() string {
	return fmt.Sprintf("%s-%s", string(sds.Type()), sds.client.Id())
}

// Node retrieves node data from Spanner.
func (sds *SpannerDataSource) Node(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
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
	if arc.Decorator != "" && (arc.SingleProp == "" || arc.SingleProp == v3.Wildcard || len(arc.BracketProps) > 0) {
		return nil, fmt.Errorf("chain expressions are only supported for a single property")
	}

	artifacts := addOptimizationsToNodeRequest(arc)
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
		edges, err := sds.client.GetNodeEdgesByID(ctx, req.Nodes, arc, pageSize, offset)
		if err != nil {
			return nil, fmt.Errorf("error getting node edges: %v", err)
		}
		resp, err = nodeEdgesToNodeResponse(req.Nodes, edges, sds.Id(), pageSize, offset)
		if err != nil {
			return nil, err
		}
	}
	removeOptimizationsFromNodeResponse(resp, artifacts)
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

// FilterStatVarsByEntity retrieves a list of variables that have data for the given entities.
func (sds *SpannerDataSource) FilterStatVarsByEntity(ctx context.Context, req *pb.FilterStatVarsByEntityRequest) (*pb.FilterStatVarsByEntityResponse, error) {
	if len(req.GetStatVars()) == 0 || len(req.GetEntities()) == 0 {
		return &pb.FilterStatVarsByEntityResponse{}, nil
	}

	variables := []string{}
	for _, sv := range req.GetStatVars() {
		variables = append(variables, sv.GetDcid())
	}

	rows, err := sds.client.FilterStatVarsByEntity(ctx, variables, req.GetEntities())
	if err != nil {
		return nil, fmt.Errorf("error filtering stat vars by entity from Spanner: %v", err)
	}

	// Spanner query returns a list of matched variables.
	matchedVars := map[string]struct{}{}
	for _, row := range rows {
		if len(row) == 1 {
			matchedVars[row[0]] = struct{}{}
		}
	}

	resp := &pb.FilterStatVarsByEntityResponse{}
	for _, sv := range req.GetStatVars() {
		if _, ok := matchedVars[sv.GetDcid()]; ok {
			resp.StatVars = append(resp.StatVars, sv)
		}
	}

	return resp, nil
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
		return sds.resolveDescription(ctx, req, inArc)
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

// Sparql executes a SPARQL query against the Spanner data source.
func (sds *SpannerDataSource) Sparql(ctx context.Context, req *pb.SparqlRequest) (*pb.QueryResponse, error) {
	nodes, queries, opts, err := sparql.ParseQuery(req.GetQuery())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error parsing sparql request: %v", err)
	}
	results, err := sds.client.Sparql(ctx, nodes, queries, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error executing sparql query: %v", err)
	}
	response, err := sparqlResultsToQueryResponse(nodes, results)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error converting sparql results to query response: %v", err)
	}
	return response, nil
}

// resolveDescription resolves entity descriptions to DCIDs.
func (sds *SpannerDataSource) resolveDescription(
	ctx context.Context,
	req *pbv2.ResolveRequest,
	inArc *v2.Arc,
) (*pbv2.ResolveResponse, error) {
	// Extract typeOf from filter.
	typeOfs, ok := inArc.Filter["typeOf"]
	if !ok {
		typeOfs = []string{""}
	}

	// Prepare entity info set.
	entityInfoSet := map[recon.EntityInfo]struct{}{}
	for _, node := range req.GetNodes() {
		for _, typeOf := range typeOfs {
			entityInfoSet[recon.EntityInfo{Description: node, TypeOf: typeOf}] = struct{}{}
		}
	}

	// Define Spanner-specific lookup functions.
	placeIdToDcidFunc := func(ctx context.Context, placeIds []string) (map[string][]string, error) {
		return sds.client.ResolveByID(ctx, placeIds, "placeId", "dcid")
	}

	// Resolve DCIDs.
	entityInfoToDCIDs, dcidSet, err := recon.ResolveDCIDs(
		ctx, sds.mapsClient, sds.recogPlaceStore, placeIdToDcidFunc, entityInfoSet)
	if err != nil {
		return nil, err
	}

	// Get types of the DCIDs from Spanner.
	dcidToTypeSet, err := sds.fetchTypes(ctx, dcidSet)
	if err != nil {
		return nil, err
	}

	// Assemble results.
	resp := &pbv2.ResolveResponse{}
	for _, node := range req.GetNodes() {
		resEntity := &pbv2.ResolveResponse_Entity{
			Node: node,
		}
		candidateSet := map[string]struct{}{}
		for _, typeOf := range typeOfs {
			e := recon.EntityInfo{Description: node, TypeOf: typeOf}
			if dcids, ok := entityInfoToDCIDs[e]; ok {
				for _, dcid := range dcids {
					if typeOf != "" {
						// Filter by type.
						types, ok := dcidToTypeSet[dcid]
						if !ok {
							continue
						}
						if _, ok := types[typeOf]; !ok {
							continue
						}
					}
					candidateSet[dcid] = struct{}{}
				}
			}
		}
		for candidate := range candidateSet {
			resEntity.Candidates = append(resEntity.Candidates, &pbv2.ResolveResponse_Entity_Candidate{
				Dcid: candidate,
			})
		}
		// Sort candidates for determinism.
		sort.Slice(resEntity.Candidates, func(i, j int) bool {
			return resEntity.Candidates[i].Dcid < resEntity.Candidates[j].Dcid
		})
		resp.Entities = append(resp.Entities, resEntity)
	}

	return resp, nil
}

// fetchTypes gets the types of the provided DCIDs from Spanner.
func (sds *SpannerDataSource) fetchTypes(
	ctx context.Context,
	dcidSet map[string]struct{},
) (map[string]map[string]struct{}, error) {
	dcidToTypeSet := map[string]map[string]struct{}{}
	if len(dcidSet) == 0 {
		return dcidToTypeSet, nil
	}

	typeArc := &v2.Arc{SingleProp: "typeOf", Out: true}
	dcidToEdges, err := sds.client.GetNodeEdgesByID(ctx, util.StringSetToSlice(dcidSet), typeArc, 0, 0)
	if err != nil {
		return nil, err
	}
	for dcid, edges := range dcidToEdges {
		dcidToTypeSet[dcid] = map[string]struct{}{}
		for _, edge := range edges {
			dcidToTypeSet[dcid][edge.Value] = struct{}{}
		}
	}
	return dcidToTypeSet, nil
}

type eventCollectionDateRequest struct {
	placeID   string
	eventType string
}

// Event retrieves event data from Spanner.
func (sds *SpannerDataSource) Event(ctx context.Context, req *pbv2.EventRequest) (*pbv2.EventResponse, error) {
	arcs, err := v2.ParseProperty(req.GetProperty())
	if err != nil {
		return nil, err
	}

	if parsedReq := parseEventCollectionDate(req, arcs); parsedReq != nil {
		return sds.handleEventCollectionDate(ctx, parsedReq)
	}

	parsedReq, err := parseEventCollection(req, arcs)
	if err != nil {
		return nil, err
	}
	if parsedReq != nil {
		return sds.handleEventCollection(ctx, parsedReq)
	}

	return nil, status.Errorf(codes.InvalidArgument, "unsupported event request property: %s", req.GetProperty())
}

// parseEventCollectionDate checks if the arcs match the pattern for EventCollectionDate and returns the parsed elements.
// Pattern: <-location{typeOf:EventType}->date
func parseEventCollectionDate(req *pbv2.EventRequest, arcs []*v2.Arc) *eventCollectionDateRequest {
	if len(arcs) != 2 {
		return nil
	}
	// arcs[0] should be <-location
	// arcs[1] should be ->date
	if arcs[0].Out || !arcs[1].Out {
		return nil
	}
	if arcs[0].SingleProp != "location" || arcs[1].SingleProp != "date" {
		return nil
	}
	typeOfs, ok := arcs[0].Filter["typeOf"]
	if !ok || len(typeOfs) == 0 {
		return nil
	}
	return &eventCollectionDateRequest{
		placeID:   req.GetNode(),
		eventType: typeOfs[0],
	}
}

// handleEventCollectionDate handles EventCollectionDate requests.
func (sds *SpannerDataSource) handleEventCollectionDate(ctx context.Context, req *eventCollectionDateRequest) (*pbv2.EventResponse, error) {
	dates, err := sds.client.GetEventCollectionDate(ctx, req.placeID, req.eventType)
	if err != nil {
		return nil, fmt.Errorf("error getting event collection date: %v", err)
	}

	return &pbv2.EventResponse{
		EventCollectionDate: &pbv1.EventCollectionDate{
			Dates: dates,
		},
	}, nil
}

func (sds *SpannerDataSource) BulkVariableInfo(ctx context.Context, req *pbv1.BulkVariableInfoRequest) (*pbv1.BulkVariableInfoResponse, error) {
	metadata, err := sds.client.GetProvenanceSummary(ctx, req.GetNodes())
	if err != nil {
		return nil, fmt.Errorf("error getting variable metadata from Spanner: %v", err)
	}
	return generateBulkVariableInfoResponse(metadata), nil
}

// parseEventCollection checks if the arcs match the pattern for EventCollection and returns the parsed request.
// Pattern: <-location{typeOf:EventType, date:Date, filter_prop:filter_val}
func parseEventCollection(req *pbv2.EventRequest, arcs []*v2.Arc) (*pbv1.EventCollectionRequest, error) {
	if len(arcs) != 1 {
		return nil, nil
	}
	arc := arcs[0]
	if arc.Out || arc.SingleProp != "location" {
		return nil, nil
	}
	typeOfs, ok := arc.Filter["typeOf"]
	if !ok || len(typeOfs) == 0 {
		return nil, fmt.Errorf("event collection requires 'typeOf' filter")
	}

	res := &pbv1.EventCollectionRequest{
		EventType:         typeOfs[0],
		AffectedPlaceDcid: req.GetNode(),
	}

	if dates, ok := arc.Filter["date"]; ok && len(dates) > 0 {
		res.Date = dates[0]
	}

	// Handle standard filters (e.g. area).
	for k, v := range arc.Filter {
		if k == "typeOf" || k == "date" {
			continue
		}
		if len(v) != 1 {
			return nil, fmt.Errorf("extra filter '%s' can only have one value", k)
		}
		spec, err := v2e.ParseEventCollectionFilter(k, v[0])
		if err != nil {
			return nil, fmt.Errorf("invalid filter format for '%s': %v", k, err)
		}
		res.FilterProp = spec.Prop
		res.FilterLowerLimit = spec.LowerLimit
		res.FilterUpperLimit = spec.UpperLimit
		res.FilterUnit = spec.Unit
		break // V2 supports at most one extra filter
	}

	return res, nil
}

// handleEventCollection handles EventCollection requests.
func (sds *SpannerDataSource) handleEventCollection(ctx context.Context, req *pbv1.EventCollectionRequest) (*pbv2.EventResponse, error) {
	collection, err := sds.client.GetEventCollection(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error getting event collection: %v", err)
	}

	return &pbv2.EventResponse{
		EventCollection: collection,
	}, nil

}
