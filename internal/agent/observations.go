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
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	colDcid     = "dcid"
	colName     = "name"
	colTypeOf   = "typeOf"
	colDate     = "date"
	colValue    = "value"
	attrFacetID = "facetId"
	arcName     = "name"
	arcTypeOf   = "typeOf"
)

var metadataDimensions = map[string]struct{}{
	datacommons.ComponentVariableMeasured:  {},
	datacommons.ComponentProvenance:        {},
	datacommons.ComponentMeasurementMethod: {},
	datacommons.ComponentObservationPeriod: {},
	datacommons.ComponentUnit:              {},
}

// GetObservations aggregates and formats observations for statistical variables.
//
// Dispatcher Routing Note:
// Uses presence of non-empty 'entities' map to differentiate between legacy (V2Observation)
// and new multi-entity SDMX execution paths. Legacy path will be removed once MCP server
// integration is completed.
func (s *Service) GetObservations(
	ctx context.Context,
	in *pbv2.GetObservationsRequest,
) (*pbv2.GetObservationsResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if in.GetVariableDcid() == "" {
		return nil, status.Error(codes.InvalidArgument, "variable_dcid must be specified")
	}

	if len(in.GetEntities()) > 0 {
		log.Printf("Agent GetObservations: routing to SDMX engine (variable=%s, entities_count=%d)", in.GetVariableDcid(), len(in.GetEntities()))
		return s.getObservationsSdmx(ctx, in)
	}
	//nolint:staticcheck // Deprecated field checked for backward compatibility routing
	if in.GetPlaceDcid() != "" {
		log.Printf("Agent GetObservations: routing to legacy V2Observation engine (variable=%s, place=%s)", in.GetVariableDcid(), in.GetPlaceDcid())
		return s.getObservationsLegacy(ctx, in)
	}

	return nil, status.Error(codes.InvalidArgument, "either entities map or place_dcid must be specified")
}

type parentPlaceSpec struct {
	parentDcid string
	childType  string
}

// parseEntityConstraint extracts direct DCIDs or parent place specs from a protobuf Value.
func parseEntityConstraint(val *structpb.Value) ([]string, *parentPlaceSpec, error) {
	if val == nil {
		return nil, nil, nil
	}

	switch kind := val.GetKind().(type) {
	case *structpb.Value_ListValue:
		dcids, err := parseDirectDcids(kind.ListValue)
		return dcids, nil, err

	case *structpb.Value_StructValue:
		spec, err := parseParentPlaceSpec(kind.StructValue)
		return nil, spec, err

	default:
		return nil, nil, fmt.Errorf("invalid entity specification format; expected array or object")
	}
}

// parseDirectDcids extracts DCID string values from a protobuf ListValue, ensuring type safety.
func parseDirectDcids(list *structpb.ListValue) ([]string, error) {
	if len(list.GetValues()) == 0 {
		return nil, fmt.Errorf("entity specification list cannot be empty")
	}
	var dcids []string
	for _, item := range list.GetValues() {
		strVal, ok := item.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("list contains non-string element: %v", item)
		}
		if strVal.StringValue == "" {
			return nil, fmt.Errorf("list contains empty string DCID")
		}
		dcids = append(dcids, strVal.StringValue)
	}
	return dcids, nil
}

// parseParentPlaceSpec extracts parent place configuration fields from a protobuf Struct, verifying constraints.
func parseParentPlaceSpec(st *structpb.Struct) (*parentPlaceSpec, error) {
	fields := st.GetFields()

	parentDcidVal, ok := fields["parent_dcid"]
	if !ok {
		return nil, fmt.Errorf("missing 'parent_dcid' in entity specification")
	}
	parentDcidStr, ok := parentDcidVal.GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, fmt.Errorf("'parent_dcid' must be a string")
	}

	childTypeVal, ok := fields["child_type"]
	if !ok {
		return nil, fmt.Errorf("missing 'child_type' in entity specification")
	}
	childTypeStr, ok := childTypeVal.GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, fmt.Errorf("'child_type' must be a string")
	}

	if parentDcidStr.StringValue == "" || childTypeStr.StringValue == "" {
		return nil, fmt.Errorf("parent_dcid and child_type cannot be empty")
	}

	return &parentPlaceSpec{
		parentDcid: parentDcidStr.StringValue,
		childType:  childTypeStr.StringValue,
	}, nil
}

// getObservationsSdmx executes multi-entity SDMX observation lookup, metadata enrichment, and dual-table formatting.
func (s *Service) getObservationsSdmx(
	ctx context.Context,
	in *pbv2.GetObservationsRequest,
) (*pbv2.GetObservationsResponse, error) {
	filter, err := parseDateFilter(in.GetDate(), in.GetDateRangeStart(), in.GetDateRangeEnd())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	sdmxReq, err := buildSdmxDataQuery(in)
	if err != nil {
		return nil, err
	}
	sdmxResult, err := s.mixer.SdmxData(ctx, sdmxReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to execute SDMX data query: %v", err)
	}

	allowedSlots := make(map[string]bool)
	for slot := range in.GetEntities() {
		allowedSlots[slot] = true
	}
	entityDcids, provenanceDcids := extractEntityAndProvenanceDcids(sdmxResult, allowedSlots)

	var entityProps map[string]*nodeProperties
	var provProps map[string]*provenanceProperties

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		entityBatch := append([]string{in.GetVariableDcid()}, entityDcids...)
		entityProps, err = s.fetchEntityProperties(gCtx, entityBatch)
		if err != nil {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}
			if _, isStatusErr := status.FromError(err); isStatusErr {
				log.Printf("Agent getObservationsSdmx: entity metadata enrichment failed: %v", err)
				entityProps = make(map[string]*nodeProperties)
				return nil
			}
			return err
		}
		return nil
	})

	g.Go(func() error {
		var err error
		provProps, err = s.fetchProvenanceProperties(gCtx, provenanceDcids)
		if err != nil {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}
			if _, isStatusErr := status.FromError(err); isStatusErr {
				log.Printf("Agent getObservationsSdmx: provenance metadata enrichment failed: %v", err)
				provProps = make(map[string]*provenanceProperties)
				return nil
			}
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "metadata enrichment serialization failed: %v", err)
	}

	entityMetaTable, err := buildEntityMetadataTable(entityDcids, entityProps)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build entity metadata table: %v", err)
	}

	return s.buildSdmxResponse(in, sdmxResult, entityMetaTable, filter, entityProps, provProps)
}

// buildSdmxDataQuery maps agent request constraints into an SDMX data query, including native facetId pushdown.
func buildSdmxDataQuery(in *pbv2.GetObservationsRequest) (*sdmxpb.SdmxDataQuery, error) {
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		datacommons.ComponentVariableMeasured: makeSdmxConstraint(in.GetVariableDcid()),
	}

	var parentCount int
	for slot, val := range in.GetEntities() {
		dcids, parentSpec, err := parseEntityConstraint(val)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "slot %q: %v", slot, err)
		}

		c := &sdmxpb.SdmxComponentConstraint{}

		if len(dcids) > 0 {
			c.Predicates = makeSdmxPredicates(dcids...)
		}

		if parentSpec != nil {
			parentCount++
			if parentCount > 1 {
				return nil, status.Error(codes.InvalidArgument, "at most 1 parent entity can be expanded per request")
			}

			c.PropertyConstraints = map[string]*sdmxpb.SdmxPropertyConstraint{
				"containedInPlace": {
					Transitive: true,
					Predicates: makeSdmxPredicates(parentSpec.parentDcid),
				},
				"typeOf": {
					Predicates: makeSdmxPredicates(parentSpec.childType),
				},
			}
		}

		constraints[slot] = c
	}

	if in.GetSourceOverride() != "" {
		constraints[datacommons.ComponentFacetID] = makeSdmxConstraint(in.GetSourceOverride())
	}
	return &sdmxpb.SdmxDataQuery{Constraints: constraints}, nil
}

// makeSdmxPredicates constructs a slice of SdmxPredicates from string values.
func makeSdmxPredicates(values ...string) []*sdmxpb.SdmxPredicate {
	preds := make([]*sdmxpb.SdmxPredicate, 0, len(values))
	for _, v := range values {
		preds = append(preds, &sdmxpb.SdmxPredicate{Value: v})
	}
	return preds
}

// makeSdmxConstraint constructs an SdmxComponentConstraint from a slice of string values.
func makeSdmxConstraint(values ...string) *sdmxpb.SdmxComponentConstraint {
	return &sdmxpb.SdmxComponentConstraint{Predicates: makeSdmxPredicates(values...)}
}

// extractEntityAndProvenanceDcids separates target spatial entities and provenance node DCIDs from the SDMX result.
func extractEntityAndProvenanceDcids(
	result *sdmxpb.SdmxDataResult,
	allowedSlots map[string]bool,
) ([]string, []string) {
	if result == nil {
		return nil, nil
	}
	entitySet := make(map[string]bool)
	provSet := make(map[string]bool)
	for _, series := range result.GetSeries() {
		// Collect target entities only from slots present in request entities
		for dimKey, dimVal := range series.GetDimensions() {
			if allowedSlots[dimKey] && dimVal != "" {
				entitySet[dimVal] = true
			}
		}
		// Collect provenance DCIDs
		if prov, ok := series.GetDimensions()[datacommons.ComponentProvenance]; ok && prov != "" {
			provSet[prov] = true
		}
	}

	entities := make([]string, 0, len(entitySet))
	for dcid := range entitySet {
		entities = append(entities, dcid)
	}
	sort.Strings(entities)

	provenances := make([]string, 0, len(provSet))
	for dcid := range provSet {
		provenances = append(provenances, dcid)
	}
	sort.Strings(provenances)

	return entities, provenances
}

type nodeProperties struct {
	name   string
	typeOf []string
}

type provenanceProperties struct {
	provenanceUrl string
}

// fetchEntityProperties performs a single V2Node call to resolve names and typeOfs for spatial entities and variables.
func (s *Service) fetchEntityProperties(ctx context.Context, dcids []string) (map[string]*nodeProperties, error) {
	props := make(map[string]*nodeProperties)
	if len(dcids) == 0 {
		return props, nil
	}

	nodeReq := &pbv2.NodeRequest{
		Nodes:    dcids,
		Property: "->[name, typeOf]",
	}
	nodeResp, err := s.mixer.V2Node(ctx, nodeReq)
	if err != nil {
		return nil, err
	}

	for _, dcid := range dcids {
		p := &nodeProperties{}
		if nodeResp != nil && nodeResp.GetData() != nil {
			if graph, ok := nodeResp.GetData()[dcid]; ok && graph.GetArcs() != nil {
				if names, ok := graph.GetArcs()[arcName]; ok && len(names.GetNodes()) > 0 {
					p.name = names.GetNodes()[0].GetValue()
				}
				if types, ok := graph.GetArcs()[arcTypeOf]; ok {
					p.typeOf = extractUniqueNodeDcids(types.GetNodes())
				}
			}
		}
		props[dcid] = p
	}
	return props, nil
}

// extractUniqueNodeDcids extracts non-empty, deduplicated DCIDs from protobuf entity nodes.
func extractUniqueNodeDcids(nodes []*pb.EntityInfo) []string {
	if len(nodes) == 0 {
		return nil
	}
	var result []string
	seen := make(map[string]bool)
	for _, node := range nodes {
		dcid := node.GetDcid()
		if dcid != "" && !seen[dcid] {
			seen[dcid] = true
			result = append(result, dcid)
		}
	}
	return result
}

// fetchProvenanceProperties performs a single V2Node call to resolve URLs for provenances.
func (s *Service) fetchProvenanceProperties(ctx context.Context, dcids []string) (map[string]*provenanceProperties, error) {
	props := make(map[string]*provenanceProperties)
	if len(dcids) == 0 {
		return props, nil
	}

	nodeReq := &pbv2.NodeRequest{
		Nodes:    dcids,
		Property: "->[url]",
	}
	nodeResp, err := s.mixer.V2Node(ctx, nodeReq)
	if err != nil {
		return nil, err
	}

	for _, dcid := range dcids {
		p := &provenanceProperties{}
		if nodeResp != nil && nodeResp.GetData() != nil {
			if graph, ok := nodeResp.GetData()[dcid]; ok && graph.GetArcs() != nil {
				if urls, ok := graph.GetArcs()["url"]; ok && len(urls.GetNodes()) > 0 {
					p.provenanceUrl = urls.GetNodes()[0].GetValue()
				}
			}
		}
		props[dcid] = p
	}
	return props, nil
}

// buildEntityMetadataTable compiles names and types for spatial entities into a flat metadata Table.
func buildEntityMetadataTable(dcids []string, props map[string]*nodeProperties) (*pbv2.Table, error) {
	table := &pbv2.Table{
		Columns: []string{colDcid, colName, colTypeOf},
	}
	for _, dcid := range dcids {
		var name string
		typeOfVals := []interface{}{}

		if p, ok := props[dcid]; ok {
			name = p.name
			for _, t := range p.typeOf {
				typeOfVals = append(typeOfVals, t)
			}
		}

		row, err := structpb.NewList([]interface{}{
			dcid,
			name,
			typeOfVals,
		})
		if err != nil {
			return nil, fmt.Errorf("buildEntityMetadataTable: failed to serialize row for %s: %w", dcid, err)
		}
		table.Rows = append(table.Rows, row)
	}
	return table, nil
}


type facetStats struct {
	facetID          string
	placesFoundCount int
	dateCount        int
	latestDate       time.Time
	staticScore      int
}

// buildSdmxResponse compiles SDMX data result into GetObservationsResponse with dual tables.
func (s *Service) buildSdmxResponse(
	in *pbv2.GetObservationsRequest,
	result *sdmxpb.SdmxDataResult,
	entityMetaTable *pbv2.Table,
	filter *dateFilter,
	entityProps map[string]*nodeProperties,
	provProps map[string]*provenanceProperties,
) (*pbv2.GetObservationsResponse, error) {
	resp := &pbv2.GetObservationsResponse{
		Variable:       &pbv2.GetObservationsResponse_Node{Dcid: in.GetVariableDcid()},
		EntityMetadata: entityMetaTable,
		Data:           &pbv2.Table{},
	}

	// Enrich Variable Node Metadata
	if p, ok := entityProps[in.GetVariableDcid()]; ok {
		resp.Variable.Name = p.name
		resp.Variable.TypeOf = p.typeOf
	}

	if result == nil || len(result.GetSeries()) == 0 {
		return resp, nil
	}

	dimSlots := extractDimensionSlots(result)
	resp.Data.Columns = append(append([]string{}, dimSlots...), colDate, colValue)

	primaryFacetID, facetsMap, statsMap := rankSdmxFacets(in, result, filter, provProps)
	if primaryFacetID == "" {
		return resp, nil
	}

	rows, err := buildSdmxDataRows(result, primaryFacetID, dimSlots, filter)
	if err != nil {
		return nil, err
	}
	resp.Data.Rows = rows

	sourceMetadata, altSources := buildSourceMetadata(primaryFacetID, facetsMap, statsMap, len(entityMetaTable.GetRows()))
	resp.SourceMetadata = sourceMetadata
	resp.AlternativeSources = altSources

	return resp, nil
}

// extractDimensionSlots extracts and sorts non-metadata dimension slot column names.
func extractDimensionSlots(result *sdmxpb.SdmxDataResult) []string {
	dimSlotsSet := make(map[string]bool)
	for _, series := range result.GetSeries() {
		for dimKey := range series.GetDimensions() {
			if _, isMetadata := metadataDimensions[dimKey]; !isMetadata {
				dimSlotsSet[dimKey] = true
			}
		}
	}
	return util.SortedStringKeys(dimSlotsSet)
}

// filterPointsByDate filters series data points based on the specified date configuration.
func filterPointsByDate(points []*sdmxpb.SdmxDataPoint, filter *dateFilter) []*sdmxpb.SdmxDataPoint {
	var targetPoints []*sdmxpb.SdmxDataPoint
	if filter != nil && filter.dateType == dateTypeLatest {
		if latest := findLatestPoint(points); latest != nil {
			targetPoints = append(targetPoints, latest)
		}
	} else {
		for _, point := range points {
			if isDateInInterval(point.GetTimePeriod(), filter) {
				targetPoints = append(targetPoints, point)
			}
		}
	}
	return targetPoints
}

// buildFacetMetadata constructs a FacetMetadata struct from a time series.
func buildFacetMetadata(series *sdmxpb.SdmxTimeSeries, facetID string, provProps map[string]*provenanceProperties) *pbv2.GetObservationsResponse_FacetMetadata {
	provDcid := series.GetDimensions()[datacommons.ComponentProvenance]
	var provUrl string
	if p, ok := provProps[provDcid]; ok {
		provUrl = p.provenanceUrl
	}

	unit := series.GetAttributes()[datacommons.ComponentUnit]
	if unit == "" {
		unit = series.GetDimensions()[datacommons.ComponentUnit]
	}

	return &pbv2.GetObservationsResponse_FacetMetadata{
		SourceId:          facetID,
		MeasurementMethod: series.GetDimensions()[datacommons.ComponentMeasurementMethod],
		ObservationPeriod: series.GetDimensions()[datacommons.ComponentObservationPeriod],
		ProvenanceUrl:     provUrl,
		Unit:              unit,
	}
}

// rankSdmxFacets evaluates and ranks all facets present in the SDMX result.
func rankSdmxFacets(
	in *pbv2.GetObservationsRequest,
	result *sdmxpb.SdmxDataResult,
	filter *dateFilter,
	provProps map[string]*provenanceProperties,
) (string, map[string]*pbv2.GetObservationsResponse_FacetMetadata, map[string]*facetStats) {
	facetsMap := make(map[string]*pbv2.GetObservationsResponse_FacetMetadata)
	statsMap := make(map[string]*facetStats)
	placesByFacet := make(map[string]map[string]bool)

	slots := util.SortedStringKeys(in.GetEntities())

	for _, series := range result.GetSeries() {
		facetID := series.GetAttributes()[attrFacetID]
		if facetID == "" {
			facetID = "unknown"
		}

		if _, ok := facetsMap[facetID]; !ok {
			facetsMap[facetID] = buildFacetMetadata(series, facetID, provProps)
		}

		// Track places found count across all requested slots
		for _, slot := range slots {
			if val, ok := series.GetDimensions()[slot]; ok && val != "" {
				if _, ok := placesByFacet[facetID]; !ok {
					placesByFacet[facetID] = make(map[string]bool)
				}
				placesByFacet[facetID][val] = true
			}
		}

		targetPoints := filterPointsByDate(series.GetPoints(), filter)
		if len(targetPoints) > 0 {
			if _, ok := statsMap[facetID]; !ok {
				statsMap[facetID] = &facetStats{facetID: facetID}
			}
			statsMap[facetID].dateCount += len(targetPoints)

			for _, pt := range targetPoints {
				t, _, err := parseDateStringToInterval(pt.GetTimePeriod())
				if err == nil && t.After(statsMap[facetID].latestDate) {
					statsMap[facetID].latestDate = t
				}
			}
		}
	}

	if len(facetsMap) == 0 {
		return "", facetsMap, statsMap
	}

	var statsList []*facetStats
	for facetID, stats := range statsMap {
		stats.placesFoundCount = len(placesByFacet[facetID])
		stats.staticScore = computeFacetScore(facetsMap[facetID])
		statsList = append(statsList, stats)
	}

	if len(statsList) == 0 {
		for facetID := range facetsMap {
			statsList = append(statsList, &facetStats{
				facetID:     facetID,
				staticScore: computeFacetScore(facetsMap[facetID]),
			})
		}
	}

	sortFacetStats(statsList)
	return statsList[0].facetID, facetsMap, statsMap
}

// sortFacetStats orders facet statistics using multi-tier evaluation heuristics.
func sortFacetStats(statsList []*facetStats) {
	// Sort stats according to heuristics:
	// - Most places found (higher is better)
	// - Most observation points (higher is better)
	// - Most recent data (latest date, later is better)
	// - Static rank score (higher is better)
	// - Final tie-breaker: string comparison of source ID
	sort.Slice(statsList, func(i, j int) bool {
		si, sj := statsList[i], statsList[j]
		if si.placesFoundCount != sj.placesFoundCount {
			return si.placesFoundCount > sj.placesFoundCount
		}
		if si.dateCount != sj.dateCount {
			return si.dateCount > sj.dateCount
		}
		if !si.latestDate.Equal(sj.latestDate) {
			return si.latestDate.After(sj.latestDate)
		}
		if si.staticScore != sj.staticScore {
			return si.staticScore > sj.staticScore
		}
		return si.facetID < sj.facetID
	})
}

// buildSdmxDataRows constructs table rows for observation data matching the primary facet.
func buildSdmxDataRows(
	result *sdmxpb.SdmxDataResult,
	primaryFacetID string,
	dimSlots []string,
	filter *dateFilter,
) ([]*structpb.ListValue, error) {
	var rows []*structpb.ListValue

	for _, series := range result.GetSeries() {
		facetID := series.GetAttributes()[attrFacetID]
		if facetID == "" {
			facetID = "unknown"
		}
		if facetID != primaryFacetID {
			continue
		}

		var dimVals []interface{}
		for _, slot := range dimSlots {
			dimVals = append(dimVals, series.GetDimensions()[slot])
		}

		targetPoints := filterPointsByDate(series.GetPoints(), filter)
		for _, point := range targetPoints {
			val, err := strconv.ParseFloat(point.GetObservationValue(), 64)
			var parsedVal interface{} = val
			if err != nil {
				parsedVal = point.GetObservationValue()
			}

			rowVals := make([]interface{}, 0, len(dimVals)+2)
			rowVals = append(rowVals, dimVals...)
			rowVals = append(rowVals, point.GetTimePeriod(), parsedVal)

			row, err := structpb.NewList(rowVals)
			if err != nil {
				return nil, fmt.Errorf("buildSdmxDataRows: failed to serialize data row: %w", err)
			}
			rows = append(rows, row)
		}
	}

	sortSdmxRows(rows)
	return rows, nil
}

// buildSourceMetadata constructs the primary and alternative source metadata for response.
func buildSourceMetadata(
	primaryFacetID string,
	facetsMap map[string]*pbv2.GetObservationsResponse_FacetMetadata,
	statsMap map[string]*facetStats,
	entityRowCount int,
) (*pbv2.GetObservationsResponse_FacetMetadata, []*pbv2.GetObservationsResponse_AlternativeSource) {
	if primaryFacetID == "" {
		return nil, nil
	}

	sourceMetadata := facetsMap[primaryFacetID]

	var altIDs []string
	for id := range facetsMap {
		if id != primaryFacetID {
			altIDs = append(altIDs, id)
		}
	}
	sort.Strings(altIDs)

	var altSources []*pbv2.GetObservationsResponse_AlternativeSource
	for _, id := range altIDs {
		alt := &pbv2.GetObservationsResponse_AlternativeSource{
			SourceMetadata: facetsMap[id],
		}
		if entityRowCount > 1 && statsMap[id] != nil {
			c := int32(statsMap[id].placesFoundCount)
			alt.PlacesFoundCount = &c
		}
		altSources = append(altSources, alt)
	}

	return sourceMetadata, altSources
}

// computeFacetScore maps response facet metadata to core pb.Facet and fetches its ranking score.
func computeFacetScore(f *pbv2.GetObservationsResponse_FacetMetadata) int {
	pbFacet := &pb.Facet{
		MeasurementMethod: f.MeasurementMethod,
		ObservationPeriod: f.ObservationPeriod,
		Unit:              f.Unit,
		ImportName:        f.ImportName,
	}
	return ranking.GetFacetScore(pbFacet)
}

// findLatestPoint identifies the temporally latest point in a series using parseDateStringToInterval.
func findLatestPoint(points []*sdmxpb.SdmxDataPoint) *sdmxpb.SdmxDataPoint {
	var latestPoint *sdmxpb.SdmxDataPoint
	var latestTime time.Time
	for _, point := range points {
		t, _, err := parseDateStringToInterval(point.GetTimePeriod())
		if err != nil {
			continue
		}
		if latestPoint == nil || t.After(latestTime) {
			latestPoint = point
			latestTime = t
		}
	}
	return latestPoint
}

// sortSdmxRows sorts SDMX data table rows lexicographically on dimension values and date keys.
func sortSdmxRows(rows []*structpb.ListValue) {
	sort.SliceStable(rows, func(i, j int) bool {
		rowI := rows[i].GetValues()
		rowJ := rows[j].GetValues()
		limit := len(rowI) - 1
		for k := 0; k < limit; k++ {
			valI := rowI[k].GetStringValue()
			valJ := rowJ[k].GetStringValue()
			if valI != valJ {
				return valI < valJ
			}
		}
		return false
	})
}
