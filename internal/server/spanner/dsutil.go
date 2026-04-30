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
	"cmp"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	v3 "github.com/datacommonsorg/mixer/internal/server/v3"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/protobuf/proto"
)

const (
	// Used for Facet responses with an entity expression.
	entityPlaceholder = ""
)

// Select options for Observation.
const (
	ENTITY   = "entity"
	VARIABLE = "variable"
	DATE     = "date"
	VALUE    = "value"
	FACET    = "facet"
)

// Map of original chained property to optimized property.
// These are chained node properties that can be replaced with optimized versions before fetching from Spanner.
var optimizedChainProps = map[string]string{
	"containedInPlace":       "linkedContainedInPlace",
	"linkedContainedInPlace": "linkedContainedInPlace",
}

// Struct to hold optimizations made to Node requests.
// This is used to recover the original response.
type nodeArtifacts struct {
	// Original chained property in request that was replaced.
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
func getOffset(nextToken, dataSourceID string) (int, error) {
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
			return int(spannerInfo.SpannerInfo.GetOffset()), nil
		}
	}

	return 0, nil
}

// getNextToken encodes next offset in a nextToken string.
func getNextToken(offset int, dataSourceID string) (string, error) {
	pi := &pbv2.Pagination{
		Info: []*pbv2.Pagination_DataSourceInfo{
			{
				Id: dataSourceID,
				DataSourceInfo: &pbv2.Pagination_DataSourceInfo_SpannerInfo{
					SpannerInfo: &pbv2.SpannerInfo{
						Offset: int32(offset),
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

// addOptimizationsToNodeRequest optimizes a Node request for fetching from Spanner, modifying the input arc in-place.
func addOptimizationsToNodeRequest(arc *v2.Arc) *nodeArtifacts {
	artifacts := &nodeArtifacts{}

	// Maybe optimize chaining.
	if arc.Decorator == v3.Chain {
		if replacementProp, ok := optimizedChainProps[arc.SingleProp]; ok {
			artifacts.chainProp = arc.SingleProp
			arc.Decorator = ""
			arc.SingleProp = replacementProp
		}
	}

	return artifacts
}

// removeOptimizationsFromNodeResponse cleans up the intermediate Node response based on request optimizations, modifying the response in-place.
func removeOptimizationsFromNodeResponse(resp *pbv2.NodeResponse, artifacts *nodeArtifacts) {
	// Maybe optimize chaining.
	if artifacts.chainProp != "" {
		replacementProp := optimizedChainProps[artifacts.chainProp]
		for _, lg := range resp.Data {
			if nodes, ok := lg.Arcs[replacementProp]; ok {
				// Clear provenance, since chained responses do not return a provenance.
				for _, node := range nodes.Nodes {
					node.ProvenanceId = ""
				}
				lg.Arcs[artifacts.chainProp+v3.Chain] = nodes
				delete(lg.Arcs, replacementProp)
			}
		}
	}
}

// nodeEdgesToNodeResponse converts a map from subject id to its edges to a NodeResponse proto.
func nodeEdgesToNodeResponse(nodes []string, edgesBySubjectID map[string][]*Edge, id string, pageSize, offset int) (*pbv2.NodeResponse, error) {
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

		// We requested pageSize+1 rows,
		// so having this many rows indicates that we have at least one more request,
		// so generate nextToken.
		if rows == pageSize+1 && nodeResponse.NextToken == "" {
			edges = edges[:len(edges)-1]
			nextToken, err := getNextToken(offset+pageSize, id)
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

	// Sort nodes in each arc: Dcid -> Value -> ProvenanceId
	for _, nodes := range linkedGraph.Arcs {
		slices.SortFunc(nodes.Nodes, func(i, j *pb.EntityInfo) int {
			if c := cmp.Compare(i.GetDcid(), j.GetDcid()); c != 0 {
				return c
			}
			if c := cmp.Compare(i.GetValue(), j.GetValue()); c != 0 {
				return c
			}
			return cmp.Compare(i.GetProvenanceId(), j.GetProvenanceId())
		})
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
		variableObs.ByEntity[entityPlaceholder] = &pbv2.EntityObservation{
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

// sparqlResultsToQueryResponse converts SPARQL row data results into a QueryResponse.
func sparqlResultsToQueryResponse(nodes []types.Node, results [][]string) (*pb.QueryResponse, error) {
	response := &pb.QueryResponse{
		Header: make([]string, 0, len(nodes)),
		Rows:   make([]*pb.QueryResponseRow, 0, len(results)),
	}
	for _, node := range nodes {
		response.Header = append(response.Header, node.Alias)
	}
	for _, data := range results {
		if len(data) != len(nodes) {
			return nil, fmt.Errorf("mismatched number of columns in SPARQL result row: got %d, want %d", len(data), len(nodes))
		}
		row := &pb.QueryResponseRow{
			Cells: make([]*pb.QueryResponseCell, 0, len(data)),
		}
		for _, value := range data {
			row.Cells = append(row.Cells, &pb.QueryResponseCell{
				Value: value,
			})
		}
		response.Rows = append(response.Rows, row)
	}
	return response, nil
}

func generateBulkVariableInfoResponse(variableInfo map[string]map[string]*pb.StatVarSummary_ProvenanceSummary) *pbv1.BulkVariableInfoResponse {
	response := &pbv1.BulkVariableInfoResponse{
		Data: make([]*pbv1.VariableInfoResponse, 0, len(variableInfo)),
	}
	for key, provToValue := range variableInfo {
		response.Data = append(response.Data, &pbv1.VariableInfoResponse{
			Node: key,
			Info: &pb.StatVarSummary{
				ProvenanceSummary: provToValue,
			},
		})
	}
	// Sort response by variable.
	slices.SortFunc(response.Data, func(a, b *pbv1.VariableInfoResponse) int {
		return strings.Compare(a.Node, b.Node)
	})
	return response
}

// splitPascalCase splits a PascalCase string into separate words with spaces.
func splitPascalCase(s string) string {
	var builder strings.Builder

	// Pre-allocate memory.
	builder.Grow(len(s) + 5)

	runes := []rune(s)
	for i, r := range runes {
		if i > 0 {
			prev := runes[i-1]

			// Rule 1: Lowercase to Uppercase (e.g., "m" -> "I")
			isLowerToUpper := unicode.IsLower(prev) && unicode.IsUpper(r)

			// Rule 2: Acronym boundary (e.g., "X" -> "M" -> "L")
			isAcronym := unicode.IsUpper(prev) && unicode.IsUpper(r) &&
				i+1 < len(runes) && unicode.IsLower(runes[i+1])

			// Rule 3: Letter to Number (e.g., "s" -> "6")
			isLetterToDigit := unicode.IsLetter(prev) && unicode.IsDigit(r)

			// Rule 4: Number to Letter (e.g., "5" -> "Y")
			isDigitToLetter := unicode.IsDigit(prev) && unicode.IsLetter(r)

			// If any of our boundaries are met, insert a space
			if isLowerToUpper || isAcronym || isLetterToDigit || isDigitToLetter {
				builder.WriteRune(' ')
			}
		}
		builder.WriteRune(r)
	}

	return builder.String()
}

// processSvgId removes the SVG prefix up through the population type and splits into pvs.
func processSvgId(svg string) []string {
	var trimmed string
	_, after, found := strings.Cut(svg, "_")
	if found {
		trimmed = after
	} else {
		trimmed = strings.TrimPrefix(svg, prefixSVG)
	}

	return strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == '_' || r == '-'
	})
}

// isCuratedHierarchy checks if the SVG is part of a curated hierarchy, which have different naming conventions that do not follow the standard specialization pattern.
func isCuratedHierarchy(svg string) bool {
	return strings.HasPrefix(svg, "dc/g/UN") || strings.HasPrefix(svg, "dc/g/SDG")
}

// getSpecializedEntity returns the specialized entity for a child SVG given its parent SVG.
func getSpecializedEntity(parent, child, childName string) string {
	if !strings.Contains(child, "_") || isCuratedHierarchy(child) { // Child is likely curated.
		return childName
	}
	parentParts := processSvgId(parent)
	childParts := processSvgId(child)

	for i := 0; i < len(parentParts) && i < len(childParts); i++ {
		if parentParts[i] != childParts[i] {
			return splitPascalCase(childParts[i])
		}
	}

	return splitPascalCase(childParts[len(childParts)-1])
}

// sortSVGNode sorts the child SVGs and SVs of a StatVarGroupNode. Child SVGs are sorted by specialized entity, with special handling for curated hierarchies, and child SVs are sorted by display name.
func sortSVGNode(node *pb.StatVarGroupNode) {
	sort.Slice(node.ChildStatVarGroups, func(i, j int) bool {
		// Use numeric sorting for special groups.
		if isCuratedHierarchy(node.ChildStatVarGroups[i].Id) {
			iName := strings.Split(node.ChildStatVarGroups[i].SpecializedEntity, ":")[0]
			jName := strings.Split(node.ChildStatVarGroups[j].SpecializedEntity, ":")[0]
			iNum, err1 := strconv.Atoi(iName)
			jNum, err2 := strconv.Atoi(jName)
			if err1 == nil && err2 == nil {
				return iNum < jNum
			}
			// Fall back to string comparison if numeric parsing fails.
		}
		return node.ChildStatVarGroups[i].SpecializedEntity < node.ChildStatVarGroups[j].SpecializedEntity
	})
	sort.Slice(node.ChildStatVars, func(i, j int) bool {
		return node.ChildStatVars[i].DisplayName < node.ChildStatVars[j].DisplayName
	})
}

// svgInfoToBulkVariableGroupInfoResponse converts a list of StatVarGroupNode info to a BulkVariableGroupInfoResponse.
func svgInfoToBulkVariableGroupInfoResponse(svgInfo []*StatVarGroupNode, nodes []string) *pbv1.BulkVariableGroupInfoResponse {
	response := &pbv1.BulkVariableGroupInfoResponse{
		Data: make([]*pbv1.VariableGroupInfoResponse, 0, len(nodes)),
	}

	nodeToSVG := map[string]*pb.StatVarGroupNode{}
	for _, node := range nodes {
		nodeToSVG[node] = &pb.StatVarGroupNode{}
	}
	for _, row := range svgInfo {
		// We are trimming excess quotes to help with sorting.
		// TODO: This should really be handled at ingestion time instead.
		name := strings.Trim(row.Name, "\"")
		svgNode := nodeToSVG[row.SVG]
		if row.DescendentStatVarCount >= 0 { // Child SVG.
			if row.SubjectID == row.SVG { // Self.
				svgNode.AbsoluteName = name
				svgNode.DescendentStatVarCount = int32(row.DescendentStatVarCount)
				continue
			}
			childSVG := &pb.StatVarGroupNode_ChildSVG{
				Id:                     row.SubjectID,
				SpecializedEntity:      getSpecializedEntity(row.SVG, row.SubjectID, name),
				DisplayName:            name,
				DescendentStatVarCount: int32(row.DescendentStatVarCount),
			}
			svgNode.ChildStatVarGroups = append(svgNode.ChildStatVarGroups, childSVG)
		} else { // Child SV.
			childSV := &pb.StatVarGroupNode_ChildSV{
				Id:          row.SubjectID,
				DisplayName: name,
				HasData:     row.HasData,
				Definition:  row.Definition,
			}
			svgNode.ChildStatVars = append(svgNode.ChildStatVars, childSV)
		}
	}

	// Sort results.
	for node, svgNode := range nodeToSVG {
		sortSVGNode(svgNode)
		response.Data = append(response.Data, &pbv1.VariableGroupInfoResponse{
			Node: node,
			Info: nodeToSVG[node],
		})
	}
	slices.SortFunc(response.Data, func(a, b *pbv1.VariableGroupInfoResponse) int {
		return strings.Compare(a.Node, b.Node)
	})
	return response
}

// filteredSVGInfoToBulkVariableGroupInfoResponse converts a list of FilteredStatVarGroupNode info to a BulkVariableGroupInfoResponse.
func filteredSVGInfoToBulkVariableGroupInfoResponse(svgInfo *FilteredStatVarGroupNode, node string) *pbv1.BulkVariableGroupInfoResponse {
	response := &pbv1.BulkVariableGroupInfoResponse{
		Data: []*pbv1.VariableGroupInfoResponse{
			{
				Node: node,
				Info: &pb.StatVarGroupNode{},
			},
		},
	}

	svgNode := response.Data[0].Info
	allChildren := map[string]bool{}

	// Attach Child SVGs.
	for _, row := range svgInfo.ChildSVG {
		name := strings.Trim(row.Name, "\"")
		if row.SubjectID == node { // Self.
			svgNode.AbsoluteName = name
			svgNode.DescendentStatVarCount = int32(row.DescendentStatVarCount)
			continue
		}
		svgNode.ChildStatVarGroups = append(svgNode.ChildStatVarGroups, &pb.StatVarGroupNode_ChildSVG{
			Id:                     row.SubjectID,
			SpecializedEntity:      getSpecializedEntity(node, row.SubjectID, name),
			DisplayName:            name,
			DescendentStatVarCount: int32(row.DescendentStatVarCount),
		})
		allChildren[row.SubjectID] = true
	}

	// Attach Child SVs.
	for _, row := range svgInfo.ChildSV {
		name := strings.Trim(row.Name, "\"")
		svgNode.ChildStatVars = append(svgNode.ChildStatVars, &pb.StatVarGroupNode_ChildSV{
			Id:          row.SubjectID,
			DisplayName: name,
			HasData:     true,
			Definition:  row.Definition,
		})
		allChildren[row.SubjectID] = true
	}

	// Attach missing children.
	for _, row := range svgInfo.SVGChild {
		if _, ok := allChildren[row.SubjectID]; ok {
			continue
		}
		name := strings.Trim(row.Name, "\"")
		switch row.Predicate {
		case predicateSpecializationOf:
			svgNode.ChildStatVarGroups = append(svgNode.ChildStatVarGroups, &pb.StatVarGroupNode_ChildSVG{
				Id:                row.SubjectID,
				SpecializedEntity: getSpecializedEntity(node, row.SubjectID, name),
				DisplayName:       name,
			})
		case predicateMemberOf:
			svgNode.ChildStatVars = append(svgNode.ChildStatVars, &pb.StatVarGroupNode_ChildSV{
				Id:          row.SubjectID,
				DisplayName: name,
				Definition:  row.Definition,
			})
		}
	}

	sortSVGNode(svgNode)

	return response
}
