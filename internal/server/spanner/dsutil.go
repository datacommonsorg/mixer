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

// Utility functions used by the SpannerDataSource.

package spanner

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sort"
	"strconv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/protobuf/proto"
)

const (
	// Used for Arc.SingleProp in Node requests and indicates that all properties should be returned.
	WILDCARD = "*"
	// Used for Arc.Decorator in Node requests and indicates that recursive property paths should be returned.
	CHAIN = "+"
	// Used for Facet responses with an entity expression.
	ENTITY_PLACEHOLDER = ""
	WHERE              = "\n\t\tWHERE\n\t\t\t"
	AND                = "\n\t\t\tAND "
)

// Select options for Observation.
const (
	ENTITY   = "entity"
	VARIABLE = "variable"
	DATE     = "date"
	VALUE    = "value"
	FACET    = "facet"
)

// Chained Node properties that can be optimized before fetching from Spanner.
var optimizedChainProps = map[string]string{
	"containedInPlace":       "linkedContainedInPlace",
	"linkedContainedInPlace": "linkedContainedInPlace",
}

// Represents optimizations made to Node requests.
type nodeArtifacts struct {
	chainProp string
}

// Represents options for Observation response.
type queryOptions struct {
	entity   bool
	variable bool
	date     bool
	value    bool
	facet    bool
}

// Variable and entity combination.
type variableEntity struct {
	variable string
	entity   string
}

// nodePropsToNodeResponse converts a map from subject id to its properties to a NodeResponse proto.
func nodePropsToNodeResponse(propsBySubjectID map[string][]*Property) *pbv2.NodeResponse {
	nodeResponse := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	for subjectID, props := range propsBySubjectID {
		linkedGraph, ok := nodeResponse.Data[subjectID]
		if !ok {
			nodeResponse.Data[subjectID] = &pbv2.LinkedGraph{}
			linkedGraph = nodeResponse.Data[subjectID]
		}
		for _, prop := range props {
			linkedGraph.Properties = append(linkedGraph.Properties, prop.Predicate)
		}
	}
	return nodeResponse
}

// getOffset returns the offset for a given Spanner data source id.
func getOffset(nextToken, dataSourceID string) (int32, error) {
	if nextToken == "" {
		return 0, nil
	}

	info, err := pagination.DecodeNextToken(nextToken)
	if err != nil {
		return 0, err
	}

	for _, dataSourceInfo := range info.Info {
		if dataSourceInfo.GetId() == dataSourceID {
			spannerInfo, ok := dataSourceInfo.GetDataSourceInfo().(*pbv2.Pagination_DataSourceInfo_SpannerInfo)
			if !ok {
				return 0, fmt.Errorf("found different data source info for spanner data source id: %s", dataSourceID)
			}
			return spannerInfo.SpannerInfo.GetOffset(), nil
		}
	}

	return 0, nil
}

// getNextToken encodes next offset in a nextToken string.
func getNextToken(offset int32, dataSourceID string) (string, error) {
	pi := &pbv2.Pagination{
		Info: []*pbv2.Pagination_DataSourceInfo{
			{
				Id: dataSourceID,
				DataSourceInfo: &pbv2.Pagination_DataSourceInfo_SpannerInfo{
					SpannerInfo: &pbv2.SpannerInfo{
						Offset: offset,
					},
				},
			},
		},
	}
	nextToken, err := util.EncodeProto(pi)
	if err != nil {
		return "", err
	}

	return nextToken, nil
}

// processNodeRequest optimizes a Node request for fetching from Spanner.
func processNodeRequest(arc *v2.Arc) *nodeArtifacts {
	artifacts := &nodeArtifacts{}

	// Maybe optimize chaining.
	if arc.Decorator == CHAIN {
		if replacement, ok := optimizedChainProps[arc.SingleProp]; ok {
			artifacts.chainProp = arc.SingleProp
			arc.Decorator = ""
			arc.SingleProp = replacement
		}
	}

	return artifacts
}

// processNodeResponse cleans up the intermediate Node response based on any optimizations made to the request.
func processNodeResponse(resp *pbv2.NodeResponse, artifacts *nodeArtifacts) {
	// Maybe optimize chaining.
	if artifacts.chainProp != "" {
		for _, lg := range resp.Data {
			if nodes, ok := lg.Arcs[optimizedChainProps[artifacts.chainProp]]; ok {
				lg.Arcs[artifacts.chainProp+CHAIN] = nodes
				delete(lg.Arcs, optimizedChainProps[artifacts.chainProp])
			}
		}
	}
}

// nodeEdgesToNodeResponse converts a map from subject id to its edges to a NodeResponse proto.
func nodeEdgesToNodeResponse(nodes []string, edgesBySubjectID map[string][]*Edge, id string, offset int32) (*pbv2.NodeResponse, error) {
	nodeResponse := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	// Sort nodes to preserve order from Spanner.
	sort.Strings(nodes)

	rows := 0
	for _, subjectID := range nodes {
		edges, ok := edgesBySubjectID[subjectID]
		if !ok {
			nodeResponse.Data[subjectID] = &pbv2.LinkedGraph{}
			continue
		}

		rows += len(edges)

		// We requested PAGE_SIZE+1 rows,
		// so having this many rows indicates that we have at least one more request,
		// so generate nextToken.
		if rows == PAGE_SIZE+1 && nodeResponse.NextToken == "" {
			edges = edges[:len(edges)-1]
			nextToken, err := getNextToken(offset+PAGE_SIZE, id)
			if err != nil {
				return nil, err
			}
			nodeResponse.NextToken = nextToken
		}

		linkedGraph, err := nodeEdgesToLinkedGraph(edges)
		if err != nil {
			return nil, err
		}
		nodeResponse.Data[subjectID] = linkedGraph
	}

	return nodeResponse, nil
}

// nodeEdgesToLinkedGraph converts an array of edges to a LinkedGraph proto.
// This method assumes all edges are from the same entity.
func nodeEdgesToLinkedGraph(edges []*Edge) (*pbv2.LinkedGraph, error) {
	linkedGraph := &pbv2.LinkedGraph{
		Arcs: make(map[string]*pbv2.Nodes),
	}

	for _, edge := range edges {
		nodes, ok := linkedGraph.Arcs[edge.Predicate]
		if !ok {
			nodes = &pbv2.Nodes{}
		}
		node := &pb.EntityInfo{
			Name:         edge.Name,
			Types:        edge.Types,
			ProvenanceId: edge.Provenance,
		}

		if len(edge.Types) == 0 { // If a node has no types, it's a terminal value.
			if edge.Bytes != nil { // Use bytes if set.
				bytes, err := util.Unzip(edge.Bytes)
				if err != nil {
					return nil, err
				}
				node.Value = string(bytes)
			} else {
				node.Value = edge.Value
			}
		} else { // Otherwise, it's a reference node with a dcid.
			node.Dcid = edge.Value
		}

		nodes.Nodes = append(nodes.Nodes, node)

		linkedGraph.Arcs[edge.Predicate] = nodes
	}

	return linkedGraph, nil
}

func selectFieldsToQueryOptions(selectFields []string) queryOptions {
	var qo queryOptions
	for _, field := range selectFields {
		switch field {
		case ENTITY:
			qo.entity = true
		case VARIABLE:
			qo.variable = true
		case DATE:
			qo.date = true
		case VALUE:
			qo.value = true
		case FACET:
			qo.facet = true
		}
	}
	return qo
}

// Whether the queryOptions are for a full observation request.
func isObservationRequest(qo *queryOptions) bool {
	return qo.date && qo.value
}

func filterTimeSeriesByDate(ts *TimeSeries, date string) {
	switch date {
	case "":
	case shared.LATEST:
		if ts == nil || *ts == nil || len(*ts) == 0 {
			*ts = TimeSeries{}
		} else {
			*ts = TimeSeries{(*ts)[len(*ts)-1]}
		}
	default:
		for _, dv := range *ts {
			if dv.Date == date {
				*ts = TimeSeries{dv}
				return
			}
		}
		*ts = TimeSeries{}
	}
}

func filterObservationsByDateAndFacet(
	observations []*Observation,
	date string,
	filter *pbv2.FacetFilter,
) []*Observation {
	var filtered []*Observation
	for _, observation := range observations {
		filterTimeSeriesByDate(&observation.Observations, date)
		facet := observationToFacet(observation)
		if util.ShouldIncludeFacet(filter, facet, observation.FacetId) {
			filtered = append(filtered, observation)
		}
	}
	return filtered
}

func observationsToObservationResponse(
	req *pbv2.ObservationRequest,
	observations []*Observation,
) *pbv2.ObservationResponse {
	// The select options are handled separately since each has a different behavior in V2.
	// This includes:
	// - Whether to include requested entities that are missing data
	// - Whether to merge responses for entity expressions
	// For now, V3 will match the behavior of V2 to preserve backward compatibility and allow datasource merging.
	// TODO: Unify these responses more.
	qo := selectFieldsToQueryOptions(req.Select)
	if isObservationRequest(&qo) {
		// Returns FacetObservations with PointStats.
		return obsToObsResponse(req, observations)
	} else if qo.facet {
		// Returns FacetObservations without PointStats.
		return obsToFacetResponse(req, observations)
	} else {
		// Returns variable and entities with data.
		return obsToExistenceResponse(req, observations)
	}
}

func newObservationResponse(variable *pbv2.DcidOrExpression) *pbv2.ObservationResponse {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	if variable == nil || len(variable.Dcids) == 0 {
		return result
	}

	for _, variable := range variable.Dcids {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
	}
	return result
}

func groupObservationsByVariableAndEntity(observations []*Observation) map[variableEntity][]*Observation {
	result := map[variableEntity][]*Observation{}

	for _, obs := range observations {
		variableEntity := variableEntity{
			variable: obs.VariableMeasured,
			entity:   obs.ObservationAbout,
		}
		if result[variableEntity] == nil {
			result[variableEntity] = []*Observation{}
		}
		result[variableEntity] = append(result[variableEntity], obs)
	}

	return result
}

func generateObsResponse(variable *pbv2.DcidOrExpression, observations []*Observation, includeObs bool) *pbv2.ObservationResponse {
	response := newObservationResponse(variable)

	variableEntityObs := groupObservationsByVariableAndEntity(observations)
	for variableEntity, obs := range variableEntityObs {
		orderedFacets, facets := observationsToOrderedFacets(obs, includeObs)
		variableObs, ok := response.ByVariable[variableEntity.variable]
		if !ok {
			variableObs = &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{},
			}
			response.ByVariable[variableEntity.variable] = variableObs
		}
		variableObs.ByEntity[variableEntity.entity] = &pbv2.EntityObservation{
			OrderedFacets: orderedFacets,
		}
		for facetId, facet := range facets {
			response.Facets[facetId] = facet
		}
	}

	return response
}

// The rows are sorted when returned from Spanner,  so the entities will be in order.
func getDistinctEntities(observations []*Observation) []string {
	entities := []string{}
	entitySet := map[string]bool{}
	for _, obs := range observations {
		entity := obs.ObservationAbout
		_, ok := entitySet[entity]
		if !ok {
			entities = append(entities, entity)
			entitySet[entity] = true
		}
	}
	return entities
}

func mergeEntityOrderedFacets(
	byEntity map[string]*pbv2.EntityObservation,
	childPlaces []string,
) []*pbv2.FacetObservation {
	// Reuse merging logic from ContainedInFacet for consistency.
	result := []*pbv2.FacetObservation{}

	seenFacet := map[string]*pbv2.FacetObservation{}
	orderedFacetId := []string{}
	for _, entity := range childPlaces {
		if facetData, ok := byEntity[entity]; ok {
			for _, item := range facetData.OrderedFacets {
				if facetObs, ok := seenFacet[item.FacetId]; ok {
					facetObs.ObsCount += item.ObsCount
					if item.EarliestDate < facetObs.EarliestDate {
						facetObs.EarliestDate = item.EarliestDate
					}
					if item.LatestDate > facetObs.LatestDate {
						facetObs.LatestDate = item.LatestDate
					}
				} else {
					orderedFacetId = append(orderedFacetId, item.FacetId)
					seenFacet[item.FacetId] = item
				}
			}
		}
	}

	for _, facetId := range orderedFacetId {
		result = append(result, seenFacet[facetId])
	}

	return result
}

func obsToObsResponse(req *pbv2.ObservationRequest, observations []*Observation) *pbv2.ObservationResponse {
	response := generateObsResponse(req.Variable, observations, true /*includeObs*/)

	// Attach all requested entity dcids to response.
	if len(req.Entity.Dcids) > 0 {
		for _, variableObs := range response.ByVariable {
			for _, entity := range req.Entity.Dcids {
				_, ok := variableObs.ByEntity[entity]
				if !ok {
					variableObs.ByEntity[entity] = &pbv2.EntityObservation{}
				}
			}
		}
	}

	return response
}

func obsToFacetResponse(req *pbv2.ObservationRequest, observations []*Observation) *pbv2.ObservationResponse {
	response := generateObsResponse(req.Variable, observations, false /*includeObs*/)

	if len(req.Entity.Dcids) > 0 {
		return response
	}

	// Merge child places for entity expression.
	mergedResponse := newObservationResponse(req.Variable)
	mergedResponse.Facets = response.Facets
	childPlaces := getDistinctEntities(observations)
	for variable, initialVariableObs := range response.ByVariable {
		variableObs, ok := mergedResponse.ByVariable[variable]
		if !ok {
			variableObs = &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{},
			}
			mergedResponse.ByVariable[variable] = variableObs
		}
		variableObs.ByEntity[ENTITY_PLACEHOLDER] = &pbv2.EntityObservation{
			OrderedFacets: mergeEntityOrderedFacets(initialVariableObs.ByEntity, childPlaces),
		}
	}
	return mergedResponse
}

func obsToExistenceResponse(req *pbv2.ObservationRequest, observations []*Observation) *pbv2.ObservationResponse {
	response := newObservationResponse(req.Variable)
	for _, obs := range observations {
		variableObs, ok := response.ByVariable[obs.VariableMeasured]
		if !ok {
			variableObs = &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{},
			}
			response.ByVariable[obs.VariableMeasured] = variableObs
		}
		variableObs.ByEntity[obs.ObservationAbout] = &pbv2.EntityObservation{}
	}
	return response
}

func observationsToOrderedFacets(
	observations []*Observation,
	includeObs bool,
) ([]*pbv2.FacetObservation, map[string]*pb.Facet) {
	facets := map[string]*pb.Facet{}
	placeVariableFacets := []*pb.PlaceVariableFacet{}
	facetIdToFacetObs := map[string]*pbv2.FacetObservation{}
	for _, obs := range observations {
		pvf, facetObs := observationToFacetObservation(obs, includeObs)

		// Skip rows with no time series.
		if pvf == nil {
			continue
		}

		placeVariableFacets = append(placeVariableFacets, pvf)
		facetIdToFacetObs[obs.FacetId] = facetObs
		facets[obs.FacetId] = pvf.Facet
	}

	// Rank FacetObservations.
	orderedFacets := []*pbv2.FacetObservation{}
	sort.Sort(ranking.FacetByRank(placeVariableFacets))
	for _, pvf := range placeVariableFacets {
		orderedFacets = append(orderedFacets, facetIdToFacetObs[pvf.FacetId])
	}

	return orderedFacets, facets
}

func observationToFacetObservation(
	observation *Observation,
	includeObs bool,
) (*pb.PlaceVariableFacet, *pbv2.FacetObservation) {
	facet := observationToFacet(observation)

	var observations []*pb.PointStat
	for _, dateValue := range observation.Observations {
		pointStat, err := dateValueToPointStat(dateValue)

		// Skip observations with non-numeric values.
		if err != nil {
			slog.Warn(
				"Error decoding PointStat",
				"variable", observation.VariableMeasured,
				"entity", observation.ObservationAbout,
				"error", err,
			)
			continue
		}

		observations = append(observations, pointStat)
	}

	if len(observations) == 0 {
		return nil, nil
	}

	facetObservation := &pbv2.FacetObservation{
		FacetId:      observation.FacetId,
		ObsCount:     *proto.Int32(int32(len(observations))),
		EarliestDate: observations[0].Date,
		LatestDate:   observations[len(observations)-1].Date,
	}

	if includeObs {
		facetObservation.Observations = observations
	}

	placeVariableFacet := &pb.PlaceVariableFacet{
		Facet:        facet,
		FacetId:      observation.FacetId,
		ObsCount:     facetObservation.ObsCount,
		EarliestDate: facetObservation.EarliestDate,
		LatestDate:   facetObservation.LatestDate,
	}

	return placeVariableFacet, facetObservation
}

func observationToFacet(observation *Observation) *pb.Facet {
	facet := pb.Facet{
		ImportName:        observation.ImportName,
		ProvenanceUrl:     observation.ProvenanceURL,
		MeasurementMethod: observation.MeasurementMethod,
		ObservationPeriod: observation.ObservationPeriod,
		ScalingFactor:     observation.ScalingFactor,
		Unit:              observation.Unit,
		IsDcAggregate:     observation.IsDcAggregate,
	}
	return &facet
}

func dateValueToPointStat(dateValue *DateValue) (*pb.PointStat, error) {
	floatVal, err := strconv.ParseFloat(dateValue.Value, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode TimeSeries float value: (%v) for date: (%v)", floatVal, dateValue.Date)
	}
	return &pb.PointStat{
		Date:  dateValue.Date,
		Value: proto.Float64(floatVal),
	}, nil
}

func searchNodesToNodeSearchResponse(nodes []*SearchNode) *pbv2.NodeSearchResponse {
	response := &pbv2.NodeSearchResponse{}

	for _, node := range nodes {
		response.Results = append(response.Results, searchNodeToNodeSearchResult(node))
	}

	return response
}

func searchNodeToNodeSearchResult(node *SearchNode) *pbv2.NodeSearchResult {
	return &pbv2.NodeSearchResult{
		Node: &pb.EntityInfo{
			Dcid:  node.SubjectID,
			Name:  node.Name,
			Types: node.Types,
		},
	}
}

// candidatesToResolveResponse converts a map of node to candidates into a ResolveResponse.
func candidatesToResolveResponse(nodeToCandidates map[string][]string) *pbv2.ResolveResponse {
	response := &pbv2.ResolveResponse{}
	for node, candidates := range nodeToCandidates {
		entity := &pbv2.ResolveResponse_Entity{
			Node:       node,
			Candidates: make([]*pbv2.ResolveResponse_Entity_Candidate, 0, len(candidates)),
		}
		for _, candidate := range candidates {
			entity.Candidates = append(entity.Candidates, &pbv2.ResolveResponse_Entity_Candidate{
				Dcid: candidate,
			})
		}
		sort.Strings(candidates)
		response.Entities = append(response.Entities, entity)
	}
	sort.Slice(response.Entities, func(i, j int) bool {
		return response.Entities[i].GetNode() > response.Entities[j].GetNode()
	})
	return response
}

func generateValueHash(input string) string {
	data := []byte(input)
	hash := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(hash[:])
}

func addValueHashes(input []string) []string {
	result := make([]string, 0, len(input)*2)
	for _, v := range input {
		result = append(result, v)
		result = append(result, generateValueHash(v))
	}
	return result
}
