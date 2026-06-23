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

package agent

import (
	"context"
	"log"
	"sort"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	wildcardPropertyQuery = "->*"
	maxConcurrentFetchers = 20
)

var (
	obsMetadataSelect = []string{"variable", "entity", "facet"}

	// properties to exclude from Statistical Variable properties Struct
	excludedSVProperties = map[string]struct{}{
		"name":           {},
		"description":    {},
		"typeOf":         {},
		"provenance":     {},
		"definition":     {},
		"memberOf":       {},
		"linkedMemberOf": {},
		"linkedMember":   {},
	}

	// properties to explicitly include for Provenance and Source dataset properties Struct.
	// These must be the exact raw property keys returned from the knowledge graph (V2Node).
	includedDatasetProperties = map[string]struct{}{
		"url":                     {},
		"license":                 {},
		"licenseType":             {},
		"lastDataRefreshDate":     {},
		"nextDataRefreshDate":     {},
		"nextSourceReleaseDate":   {},
		"sourceReleaseFrequency":  {},
		"latestObservationDate":   {},
		"earliestObservationDate": {},
		"isPartOf":                {},
		"source":                  {},
		"domain":                  {},
		"description":             {},
		"descriptionUrl":          {},
	}
)

// rawVariableData holds the raw, unmapped graph and summary data for a single Statistical Variable.
type rawVariableData struct {
	properties *pbv2.LinkedGraph
	summary    *pb.StatVarSummary
}

// rawDatasetData holds the raw, unmapped graphs for a dataset's provenance and parent source.
type rawDatasetData struct {
	provenance *pbv2.LinkedGraph
	source     *pbv2.LinkedGraph
}

// rawFetchResult aggregates all raw data retrieved during the concurrent fetch phase.
type rawFetchResult struct {
	variables    map[string]*rawVariableData
	observations *pbv2.ObservationResponse
	provenances  map[string]*rawDatasetData
}

// translatedProperties holds the resolved name, description, and filtered dynamic properties Struct.
type translatedProperties struct {
	name        string
	description string
	properties  *structpb.Struct
}

// GetVariableMetadata assesses Statistical Variables by retrieving their definitions,
// temporal/entity coverage, and source provenance descriptions.
func (s *Service) GetVariableMetadata(
	ctx context.Context,
	req *pbv2.GetVariableMetadataRequest,
) (*pbv2.GetVariableMetadataResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	defer util.TimeTrack(time.Now(), "Agent: GetVariableMetadata")

	if len(req.GetVariableDcids()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "variable_dcids cannot be empty")
	}
	if len(req.GetVariableDcids()) > 10 {
		return nil, status.Error(codes.InvalidArgument, "variable_dcids cannot exceed 10")
	}
	if len(req.GetEntityDcids()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "entity_dcids cannot be empty")
	}
	if len(req.GetEntityDcids()) > 10 {
		return nil, status.Error(codes.InvalidArgument, "entity_dcids cannot exceed 10")
	}

	varDcids := dedup(req.GetVariableDcids())
	entityDcids := dedup(req.GetEntityDcids())

	// Phase 1: Fetch raw data concurrently
	raw, err := s.fetchRawData(ctx, varDcids, entityDcids)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch raw metadata: %v", err)
	}

	// Phase 2: Translate and compose response synchronously
	return s.composeResponse(raw, varDcids, entityDcids), nil
}

// dedup removes duplicate strings from a slice.
func dedup(dcids []string) []string {
	seen := make(map[string]struct{})
	var res []string
	for _, id := range dcids {
		if id != "" {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				res = append(res, id)
			}
		}
	}
	return res
}

// fetchRawData retrieves all raw variables, summaries, and observations concurrently.
func (s *Service) fetchRawData(ctx context.Context, varDcids, entityDcids []string) (*rawFetchResult, error) {
	raw := &rawFetchResult{
		variables:   make(map[string]*rawVariableData),
		provenances: make(map[string]*rawDatasetData),
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentFetchers)

	// Pre-allocate local slices to prevent CPU false sharing/cache bouncing across goroutines
	properties := make([]*pbv2.LinkedGraph, len(varDcids))
	summaries := make([]*pb.StatVarSummary, len(varDcids))

	for i, vDcid := range varDcids {
		idx := i
		dcid := vDcid

		// Worker 1: Fetch Node Graph (Non-critical: log and skip on error)
		g.Go(func() error {
			graph, err := s.fetchNodeGraph(gCtx, dcid)
			if err != nil {
				log.Printf("Warning: failed to fetch node graph for variable %s: %v", dcid, err)
				return nil // Graceful degradation: do not fail the errgroup
			}
			properties[idx] = graph // Safe: write to unique index
			return nil
		})

		// Worker 2: Fetch Bulk Variable Info (Non-critical: log and skip on error)
		g.Go(func() error {
			req := &pbv1.BulkVariableInfoRequest{Nodes: []string{dcid}}
			resp, err := s.mixer.V2BulkVariableInfo(gCtx, req)
			if err != nil {
				log.Printf("Warning: failed to fetch bulk variable info for %s: %v", dcid, err)
				return nil // Graceful degradation
			}
			if resp != nil && resp.GetData() != nil {
				for _, info := range resp.GetData() {
					if info.GetNode() == dcid {
						summaries[idx] = info.GetInfo() // Safe: write to unique index
					}
				}
			}
			return nil
		})
	}

	// Worker 3: Fetch Observations (Critical bulk query: fail if this fails)
	g.Go(func() error {
		obsReq := &pbv2.ObservationRequest{
			Variable: &pbv2.DcidOrExpression{Dcids: varDcids},
			Entity:   &pbv2.DcidOrExpression{Dcids: entityDcids},
			Select:   obsMetadataSelect,
		}
		resp, err := s.mixer.V2Observation(gCtx, obsReq)
		if err != nil {
			return err // Fail the request if the bulk observations query fails
		}
		raw.observations = resp
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Populate variables map sequentially from local slices after concurrent execution completes
	for i, dcid := range varDcids {
		if properties[i] != nil || summaries[i] != nil {
			raw.variables[dcid] = &rawVariableData{
				properties: properties[i],
				summary:    summaries[i],
			}
		}
	}

	// Now hydrate provenances using the fetched data (lock-free!)
	if err := s.fetchRawProvenances(ctx, raw); err != nil {
		return nil, err
	}

	return raw, nil
}

// fetchRawProvenances identifies all unique provenance IDs and fetches their graphs concurrently in a lock-free manner.
func (s *Service) fetchRawProvenances(ctx context.Context, raw *rawFetchResult) error {
	provIDs := extractProvenanceIDs(raw.observations)
	if len(provIDs) == 0 {
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentFetchers)

	// Pre-allocate slots sequentially to enable lock-free parallel execution
	for _, pDcid := range provIDs {
		provDcid := pDcid
		rawDataset := &rawDatasetData{}
		raw.provenances[provDcid] = rawDataset // Safe: sequential map write in main thread

		g.Go(func() error {
			dataset, err := s.fetchDatasetGraphs(gCtx, provDcid)
			if err != nil {
				log.Printf("Warning: failed to fetch dataset graphs for provenance %s: %v", provDcid, err)
				return nil // Graceful degradation: do not fail the entire request
			}
			// Safe: lock-free direct pointer writes!
			rawDataset.provenance = dataset.provenance
			rawDataset.source = dataset.source
			return nil
		})
	}

	return g.Wait()
}

// fetchNodeGraph fetches wildcard outbound properties (->*) for a single node.
func (s *Service) fetchNodeGraph(ctx context.Context, dcid string) (*pbv2.LinkedGraph, error) {
	req := &pbv2.NodeRequest{Nodes: []string{dcid}, Property: wildcardPropertyQuery}
	resp, err := s.mixer.V2Node(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp != nil && resp.GetData() != nil {
		return resp.GetData()[dcid], nil
	}
	return nil, nil
}

// fetchDatasetGraphs fetches the provenance graph and parent source graph for a dataset.
func (s *Service) fetchDatasetGraphs(ctx context.Context, provDcid string) (*rawDatasetData, error) {
	provGraph, err := s.fetchNodeGraph(ctx, provDcid)
	if err != nil {
		return nil, err
	}

	var sourceGraph *pbv2.LinkedGraph
	if sourceDcid := extractSourceDcid(provGraph); sourceDcid != "" {
		sGraph, err := s.fetchNodeGraph(ctx, sourceDcid)
		if err != nil {
			return nil, err
		}
		sourceGraph = sGraph
	}

	return &rawDatasetData{
		provenance: provGraph,
		source:     sourceGraph,
	}, nil
}

// extractProvenanceIDs gathers unique provenance identifiers from the observation response.
func extractProvenanceIDs(obs *pbv2.ObservationResponse) []string {
	if obs == nil || len(obs.GetFacets()) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	for _, fObj := range obs.GetFacets() {
		if fObj != nil && fObj.GetProvenanceId() != "" {
			seen[fObj.GetProvenanceId()] = struct{}{}
		}
	}
	var ids []string
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// extractSourceDcid extracts the parent source DCID from a provenance graph.
func extractSourceDcid(graph *pbv2.LinkedGraph) string {
	if graph == nil || graph.GetArcs() == nil {
		return ""
	}
	if srcArc, ok := graph.GetArcs()["source"]; ok && srcArc != nil {
		for _, n := range srcArc.GetNodes() {
			if n != nil && n.GetDcid() != "" {
				return n.GetDcid()
			}
		}
	}
	return ""
}

// composeResponse synchronously transforms raw fetched data into the final response payload.
func (s *Service) composeResponse(raw *rawFetchResult, varDcids, entityDcids []string) *pbv2.GetVariableMetadataResponse {
	resp := &pbv2.GetVariableMetadataResponse{
		Status:      StatusSuccess,
		Variables:   make(map[string]*pbv2.GetVariableMetadataResponse_VariableMetadata),
		Provenances: make(map[string]*pbv2.GetVariableMetadataResponse_ProvenanceMetadata),
	}

	// Translate variables
	for _, dcid := range varDcids {
		if rawVar, ok := raw.variables[dcid]; ok && rawVar != nil {
			// Skip variables that failed all metadata fetches entirely (graceful degradation)
			if rawVar.properties == nil && rawVar.summary == nil {
				continue
			}
			resp.Variables[dcid] = translateVariable(dcid, rawVar, raw.observations, entityDcids)
		}
	}

	// Translate provenances
	for provDcid, rawDataset := range raw.provenances {
		resp.Provenances[provDcid] = translateProvenance(provDcid, rawDataset)
	}

	return resp
}

// translateVariable resolves a single variable's headers, properties, and active facets list.
func translateVariable(
	dcid string,
	rawVar *rawVariableData,
	obs *pbv2.ObservationResponse,
	entityDcids []string,
) *pbv2.GetVariableMetadataResponse_VariableMetadata {
	tp := translateProperties(rawVar.properties)

	varMeta := &pbv2.GetVariableMetadataResponse_VariableMetadata{
		Id:          dcid,
		Name:        tp.name,
		Description: tp.description,
		Properties:  tp.properties,
		Facets:      translateFacets(dcid, obs, rawVar.summary, entityDcids),
	}

	return varMeta
}

// translateProperties extracts headers and builds the dynamic properties Struct, filtering out primary name/description.
func translateProperties(graph *pbv2.LinkedGraph) *translatedProperties {
	tp := &translatedProperties{
		properties: &structpb.Struct{Fields: make(map[string]*structpb.Value)},
	}

	if graph == nil || graph.GetArcs() == nil {
		return tp
	}

	// 1. Resolve root Name (primary name or label)
	if nameArc, ok := graph.GetArcs()["name"]; ok && nameArc != nil {
		tp.name = resolveFirstValue(nameArc.GetNodes())
	}
	if tp.name == "" {
		if labelArc, ok := graph.GetArcs()["label"]; ok && labelArc != nil {
			tp.name = resolveFirstValue(labelArc.GetNodes())
		}
	}

	// 2. Resolve root Description
	if descArc, ok := graph.GetArcs()["description"]; ok && descArc != nil {
		tp.description = resolveFirstValue(descArc.GetNodes())
	}

	// 3. Build filtered dynamic properties Struct
	for prop, nodesArc := range graph.GetArcs() {
		if nodesArc == nil || len(nodesArc.GetNodes()) == 0 {
			continue
		}

		// Filter out properties that are mapped to root or represent internal detail
		if _, shouldExclude := excludedSVProperties[prop]; shouldExclude {
			continue
		}

		// Convert values completely as-is (no root-value duplication filtering)
		if structVal, ok := toStructValue(nodesArc.GetNodes()); ok {
			tp.properties.Fields[prop] = structVal
		}
	}

	return tp
}

// translateFacets maps observation responses to the list of structured FacetMetadata.
func translateFacets(
	varDcid string,
	obs *pbv2.ObservationResponse,
	summary *pb.StatVarSummary,
	entityDcids []string,
) []*pbv2.GetVariableMetadataResponse_FacetMetadata {
	var facets []*pbv2.GetVariableMetadataResponse_FacetMetadata
	if obs == nil || obs.GetByVariable() == nil {
		return facets
	}

	vObs, exists := obs.GetByVariable()[varDcid]
	if !exists || vObs == nil {
		return facets
	}

	// We aggregate by Facet ID first, then map to list of Structs
	type facetStats struct {
		facetObj *pb.Facet
		obsCount int32
		earliest string
		latest   string
		covered  map[string]struct{}
	}

	facetGroups := make(map[string]*facetStats)

	for eDcid, eObs := range vObs.GetByEntity() {
		if eObs == nil {
			continue
		}
		for _, fObs := range eObs.GetOrderedFacets() {
			if fObs == nil {
				continue
			}
			fID := fObs.GetFacetId()
			fObj, ok := obs.GetFacets()[fID]
			if !ok || fObj == nil {
				continue
			}

			stats, exists := facetGroups[fID]
			if !exists {
				stats = &facetStats{
					facetObj: fObj,
					obsCount: 0,
					covered:  make(map[string]struct{}),
				}
				facetGroups[fID] = stats
			}

			// Accumulate obs counts
			stats.obsCount += fObs.GetObsCount()
			stats.covered[eDcid] = struct{}{}

			// Date ranges
			if fObs.GetEarliestDate() != "" {
				if stats.earliest == "" || fObs.GetEarliestDate() < stats.earliest {
					stats.earliest = fObs.GetEarliestDate()
				}
			}
			if fObs.GetLatestDate() != "" {
				if stats.latest == "" || fObs.GetLatestDate() > stats.latest {
					stats.latest = fObs.GetLatestDate()
				}
			}
		}
	}

	// Sort facet IDs to ensure deterministic response order across requests
	var facetIDs []string
	for fID := range facetGroups {
		facetIDs = append(facetIDs, fID)
	}
	sort.Strings(facetIDs)

	for _, fID := range facetIDs {
		stats := facetGroups[fID]

		// Compile geographic granularities for this specific provenance from summary
		geoGranularities := extractGeographicGranularities(summary, stats.facetObj.GetProvenanceId())

		// Build scope location coverage (active subset of queried places)
		var activeCoverage []string
		for _, reqPlace := range entityDcids {
			if _, ok := stats.covered[reqPlace]; ok {
				activeCoverage = append(activeCoverage, reqPlace)
			}
		}

		// Build flat dynamic properties map for facet details
		fPropsMap := map[string]any{
			"measurementMethod": stats.facetObj.GetMeasurementMethod(),
			"observationPeriod": stats.facetObj.GetObservationPeriod(),
			"unit":              stats.facetObj.GetUnit(),
			"scalingFactor":     stats.facetObj.GetScalingFactor(),
		}

		// Strip empty keys to keep JSON pristine
		for k, v := range fPropsMap {
			if s, ok := v.(string); ok && s == "" {
				delete(fPropsMap, k)
			}
		}

		fProps, err := structpb.NewStruct(fPropsMap)
		if err != nil {
			fProps = &structpb.Struct{Fields: make(map[string]*structpb.Value)}
		}

		facetMeta := &pbv2.GetVariableMetadataResponse_FacetMetadata{
			Id:           fID,
			ProvenanceId: stats.facetObj.GetProvenanceId(),
			ObsCount:     stats.obsCount,
			DateRange: &pbv2.GetVariableMetadataResponse_FacetMetadata_DateRange{
				Start: stats.earliest,
				End:   stats.latest,
			},
			Scope: &pbv2.GetVariableMetadataResponse_FacetMetadata_Scope{
				EntityGranularity: geoGranularities,
				EntityCoverage:    activeCoverage,
			},
			Properties: fProps,
		}
		facets = append(facets, facetMeta)
	}

	return facets
}

// translateProvenance merges provenance and parent source properties into a single dynamic Struct.
func translateProvenance(id string, rawDataset *rawDatasetData) *pbv2.GetVariableMetadataResponse_ProvenanceMetadata {
	provMeta := &pbv2.GetVariableMetadataResponse_ProvenanceMetadata{
		Id:         id,
		Properties: &structpb.Struct{Fields: make(map[string]*structpb.Value)},
	}
	if rawDataset == nil {
		return provMeta
	}

	mergeProperties := func(graph *pbv2.LinkedGraph) {
		if graph == nil || graph.GetArcs() == nil {
			return
		}
		for prop, nodesArc := range graph.GetArcs() {
			if nodesArc == nil || len(nodesArc.GetNodes()) == 0 {
				continue
			}
			if _, shouldInclude := includedDatasetProperties[prop]; !shouldInclude {
				continue
			}
			if structVal, ok := toStructValue(nodesArc.GetNodes()); ok {
				provMeta.Properties.Fields[prop] = structVal
			}
		}
	}

	// Merge parent source first, then overlay provenance specific properties
	mergeProperties(rawDataset.source)
	mergeProperties(rawDataset.provenance)

	return provMeta
}

// resolveFirstValue retrieves the raw string value of the first available candidate.
func resolveFirstValue(nodes []*pb.EntityInfo) string {
	if len(nodes) == 0 {
		return ""
	}
	for _, n := range nodes {
		if n == nil {
			continue
		}
		val := n.GetValue()
		if val == "" {
			val = n.GetName()
		}
		if val == "" {
			val = n.GetDcid()
		}
		if val != "" {
			return val
		}
	}
	return ""
}

// toStructValue converts a slice of properties into a flat Struct Value.
func toStructValue(nodes []*pb.EntityInfo) (*structpb.Value, bool) {
	var values []string
	seen := make(map[string]struct{})

	for _, n := range nodes {
		if n == nil {
			continue
		}
		val := n.GetValue()
		if val == "" {
			val = n.GetName()
		}
		if val == "" {
			val = n.GetDcid()
		}
		if val == "" {
			continue
		}

		if _, ok := seen[val]; !ok {
			seen[val] = struct{}{}
			values = append(values, val)
		}
	}

	if len(values) == 0 {
		return nil, false
	}

	// If exactly 1 value, return as a flat string
	if len(values) == 1 {
		return structpb.NewStringValue(values[0]), true
	}

	// If multiple values, return as a list of strings
	var listVals []*structpb.Value
	for _, v := range values {
		listVals = append(listVals, structpb.NewStringValue(v))
	}
	return structpb.NewListValue(&structpb.ListValue{Values: listVals}), true
}

// extractGeographicGranularities retrieves sorted unique place types for a specific provenance from a StatVarSummary.
func extractGeographicGranularities(summary *pb.StatVarSummary, provID string) []string {
	var granularities []string
	if summary == nil || summary.GetProvenanceSummary() == nil {
		return []string{}
	}
	provSummary, ok := summary.GetProvenanceSummary()[provID]
	if !ok || provSummary == nil {
		return []string{}
	}

	seenPlaceTypes := make(map[string]struct{})
	for _, seriesSummary := range provSummary.GetSeriesSummary() {
		if seriesSummary != nil {
			for placeType := range seriesSummary.GetPlaceTypeSummary() {
				seenPlaceTypes[placeType] = struct{}{}
			}
		}
	}

	for placeType := range seenPlaceTypes {
		granularities = append(granularities, placeType)
	}

	// Sort to ensure deterministic order
	sort.Strings(granularities)

	if len(granularities) == 0 {
		return []string{}
	}
	return granularities
}
