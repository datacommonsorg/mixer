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
	"strings"

	internalmaps "github.com/datacommonsorg/mixer/internal/maps"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	v2e "github.com/datacommonsorg/mixer/internal/server/v2/event"
	resolvev2 "github.com/datacommonsorg/mixer/internal/server/v2/resolve"
	v3 "github.com/datacommonsorg/mixer/internal/server/v3"
	"github.com/datacommonsorg/mixer/internal/store/files"
	"github.com/datacommonsorg/mixer/internal/translator/sparql"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/golang/geo/s2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SpannerDataSource represents a data source that interacts with Spanner.
type SpannerDataSource struct {
	client          SpannerClient
	recogPlaceStore *files.RecogPlaceStore
	mapsClient      internalmaps.MapsClient
}

const (
	maxContainedInPlaceEdgesPerS2Cell = 50
	s2CellIDPrefix                    = "s2CellId/"
	s2CellTypePrefix                  = "S2CellLevel"
)

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
	if len(entities) == 0 && entityExpr == "" {
		return nil, fmt.Errorf("entity must be specified")
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

	qo := selectFieldsToQueryOptions(req.Select)

	// Check if this is an existence-only request:
	// 1. Only 'variable' and 'entity' are requested (no date, value, or facet).
	// 2. A simple list of entities is provided (no complex entity expression).
	isExistenceRequest := !qo.date && !qo.value && !qo.facet && len(entities) > 0 && entityExpr == ""

	if isExistenceRequest {
		rows, err := sds.client.CheckVariableExistence(ctx, variables, entities)
		if err != nil {
			return nil, fmt.Errorf("error checking variable existence: %w", err)
		}

		obs := make([]*Observation, 0, len(rows))
		for _, row := range rows {
			if len(row) != 2 {
				slog.Warn("CheckVariableExistence returned row with unexpected length", "length", len(row))
				continue
			}
			obs = append(obs, &Observation{
				VariableMeasured: row[0],
				ObservationAbout: row[1],
			})
		}
		return obsToExistenceResponse(req, obs), nil
	}

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
	normalizedResolveRequest, err := resolvev2.ValidateAndParseResolveInputs(req)
	if err != nil {
		return nil, err
	}

	if resolver := normalizedResolveRequest.Request.GetResolver(); resolver == resolvev2.ResolveResolverIndicator {
		// Spanner doesn't do embeddings resolution yet.
		slog.Warn("Received unsupported ResolveResolverIndicator request to Spanner", "request", req)
		return &pbv2.ResolveResponse{}, nil
	}

	switch normalizedResolveRequest.InProp {
	case resolvev2.GeoCoordinateProperty:
		return sds.resolveCoordinate(ctx, normalizedResolveRequest)
	case resolvev2.DescriptionProperty:
		return sds.resolveDescription(ctx, normalizedResolveRequest)
	default:
		return sds.resolveID(ctx, normalizedResolveRequest)
	}
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

// resolveCoordinate resolves geo coordinates to DCIDs using S2 level-10 cell mappings.
func (sds *SpannerDataSource) resolveCoordinate(
	ctx context.Context,
	req *resolvev2.NormalizedResolveRequest,
) (*pbv2.ResolveResponse, error) {
	type coordinateNode struct {
		node   string
		cellID string
	}

	coordinateNodes := []coordinateNode{}
	cellIDSet := map[string]struct{}{}
	for _, node := range req.Request.GetNodes() {
		lat, lng, err := resolvev2.ParseCoordinate(node)
		if err != nil {
			return nil, err
		}
		cellID := level10S2CellID(lat, lng)
		coordinateNodes = append(coordinateNodes, coordinateNode{
			node:   node,
			cellID: cellID,
		})
		cellIDSet[cellID] = struct{}{}
	}

	containedInPlaceArc := &v2.Arc{SingleProp: v2.ContainedInPlaceProperty, Out: true}
	// GetNodeEdgesByIDQuery always applies LIMIT pageSize+1. Coordinate resolve
	// expects at most 50 relevant place edges per S2 cell, so size the batched
	// lookup by the number of unique cells in the request.
	cellToEdges, err := sds.client.GetNodeEdgesByID(
		ctx,
		util.StringSetToSlice(cellIDSet),
		containedInPlaceArc,
		len(cellIDSet)*maxContainedInPlaceEdgesPerS2Cell,
		0,
	)
	if err != nil {
		return nil, err
	}

	resp := &pbv2.ResolveResponse{}
	for _, coordinateNode := range coordinateNodes {
		candidateSet := map[string]struct{}{}
		places := []*pb.ResolveCoordinatesResponse_Place{}
		for _, edge := range cellToEdges[coordinateNode.cellID] {
			// Skip S2 cells to return actual places only (S2 cells can point to other
			// S2 cells via containedInPlace).
			if edge.Value == "" || isS2CellNode(edge) {
				continue
			}
			dominantType, err := util.GetDominantType(edge.Types)
			if err != nil {
				return nil, err
			}
			// Coordinate resolve filters by dominant type only. Secondary types on
			// the place node do not qualify a candidate for a typeOf match.
			if len(req.TypeOfValues) > 0 && !matchesRequestedType(dominantType, req.TypeOfValues) {
				continue
			}
			if _, ok := candidateSet[edge.Value]; ok {
				continue
			}
			candidateSet[edge.Value] = struct{}{}
			places = append(places, &pb.ResolveCoordinatesResponse_Place{
				Dcid:         edge.Value,
				DominantType: dominantType,
			})
		}
		resp.Entities = append(resp.Entities, &pbv2.ResolveResponse_Entity{
			Node:       coordinateNode.node,
			Candidates: resolvev2.GetSortedResolvedPlaceCandidates(places),
		})
	}

	return resp, nil
}

// resolveDescription resolves entity descriptions to DCIDs.
func (sds *SpannerDataSource) resolveDescription(
	ctx context.Context,
	req *resolvev2.NormalizedResolveRequest,
) (*pbv2.ResolveResponse, error) {
	typeOfs := req.TypeOfValues
	if len(typeOfs) == 0 {
		typeOfs = []string{""}
	}

	// Prepare entity info set.
	entityInfoSet := map[recon.EntityInfo]struct{}{}
	for _, node := range req.Request.GetNodes() {
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
	for _, node := range req.Request.GetNodes() {
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

func (sds *SpannerDataSource) resolveID(ctx context.Context, req *resolvev2.NormalizedResolveRequest) (*pbv2.ResolveResponse, error) {
	nodeToCandidates, err := sds.client.ResolveByID(ctx, req.Request.GetNodes(), req.InProp, req.OutProp)
	if err != nil {
		return nil, fmt.Errorf("error resolving ids: %v", err)
	}
	return candidatesToResolveResponse(nodeToCandidates), nil
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
	dcidToEdges, err := sds.client.GetNodeEdgesByID(ctx, util.StringSetToSlice(dcidSet), typeArc, datasources.DefaultPageSize, 0)
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

func level10S2CellID(lat, lng float64) string {
	cellID := s2.CellIDFromLatLng(s2.LatLngFromDegrees(lat, lng)).Parent(10)
	return fmt.Sprintf("%s0x%016x", s2CellIDPrefix, uint64(cellID))
}

func matchesRequestedType(candidateType string, filterTypes []string) bool {
	for _, filterType := range filterTypes {
		if candidateType == filterType {
			return true
		}
	}
	return false
}

// isS2CellNode checks if the edge points to an S2 cell node by checking its types.
func isS2CellNode(edge *Edge) bool {
	for _, nodeType := range edge.Types {
		if strings.HasPrefix(nodeType, s2CellTypePrefix) {
			return true
		}
	}
	return false
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

func (sds *SpannerDataSource) BulkVariableGroupInfo(ctx context.Context, req *pbv1.BulkVariableGroupInfoRequest) (*pbv1.BulkVariableGroupInfoResponse, error) {
	// Validate input.
	if len(req.GetNodes()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "request must specify at least one node")
	}
	if req.NumEntitiesExistence < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "numEntitiesExistence must be non-negative")
	}
	var svgs []string
	var topics []string
	for _, node := range req.GetNodes() {
		if strings.HasPrefix(node, prefixTopic) {
			topics = append(topics, node)
		} else if strings.HasPrefix(node, prefixSVG) {
			svgs = append(svgs, node)
		} else {
			return nil, status.Errorf(codes.InvalidArgument, "node %s is not a valid StatVarGroup or Topic node", node)
		}
	}
	if len(svgs) > 0 && len(topics) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "cannot mix Topic and StatVarGroup nodes in request")
	}

	// Unfiltered case.
	if len(req.ConstrainedEntities) == 0 && req.NumEntitiesExistence == 0 {
		svgInfo, err := sds.client.GetStatVarGroupNode(ctx, svgs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error getting StatVarGroupNode from Spanner: %v", err)
		}
		return svgInfoToBulkVariableGroupInfoResponse(svgInfo, req.GetNodes()), nil
	}

	var constrainedPlaces []string
	var constrainedImport string
	for _, constraint := range req.ConstrainedEntities {
		if strings.HasPrefix(constraint, prefixDataset) || strings.HasPrefix(constraint, prefixSource) {
			if constrainedImport != "" {
				return nil, status.Errorf(codes.InvalidArgument, "only one import constraint can be specified")
			}
			constrainedImport = constraint
		} else {
			constrainedPlaces = append(constrainedPlaces, constraint)
		}
	}

	// Filter Topic.
	if len(topics) > 0 {
		counts, err := sds.client.GetFilteredTopic(ctx, req.GetNodes(), constrainedPlaces, constrainedImport, int(req.NumEntitiesExistence))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error getting filtered topic count from Spanner: %v", err)
		}
		return filteredTopicInfoToBulkVariableGroupInfoResponse(counts, topics), nil
	}

	// Filter StatVarGroup.
	filteredSVGInfo, err := sds.client.GetFilteredStatVarGroupNode(ctx, svgs, constrainedPlaces, constrainedImport, int(req.NumEntitiesExistence))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting filtered StatVarGroupNode from Spanner: %v", err)
	}
	return filteredSVGInfoToBulkVariableGroupInfoResponse(filteredSVGInfo), nil
}
