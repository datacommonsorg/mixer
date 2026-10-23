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

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	colDate     = "date"
	colValue    = "value"
	attrFacetID = "facetId"
	arcName     = "name"
)

var reservedSlotDimensions = map[string]struct{}{
	datacommons.ComponentVariableMeasured:  {},
	datacommons.ComponentProvenance:        {},
	datacommons.ComponentMeasurementMethod: {},
	datacommons.ComponentObservationPeriod: {},
	datacommons.ComponentUnit:              {},
	datacommons.ComponentTimePeriod:        {},
	datacommons.ComponentObservationValue:  {},
	datacommons.ComponentScalingFactor:     {},
	datacommons.ComponentFacetID:           {},
}

// nodeProperties holds the display name fetched for an entity or variable DCID via V2Node.
type nodeProperties struct {
	name string
}

// provenanceProperties holds the provenance URL fetched for a facet import DCID via V2Node.
type provenanceProperties struct {
	provenanceUrl string
}

// facetStats tracks coverage and date metrics used to rank and select the primary SDMX facet.
type facetStats struct {
	facetID          string
	placesFoundCount int
	dateCount        int
	latestDate       time.Time
}

// GetObservations aggregates and formats observations for statistical variables using the SDMX engine.
func (s *Service) GetObservations(
	ctx context.Context,
	in *GetObservationsRequest,
) (*GetObservationsResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if in.VariableDcid == "" {
		return nil, status.Error(codes.InvalidArgument, "variable_dcid must be specified")
	}
	if len(in.Entities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "entities map must be specified")
	}

	log.Printf("Agent GetObservations: routing to SDMX engine (variable=%s, entities_count=%d)", in.VariableDcid, len(in.Entities))
	return s.getObservationsSdmx(ctx, in)
}

// getObservationsSdmx executes multi-entity SDMX observation lookup, metadata enrichment, and dual-table formatting.
func (s *Service) getObservationsSdmx(
	ctx context.Context,
	in *GetObservationsRequest,
) (*GetObservationsResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	filter, err := parseDateFilter(in.Date, in.DateRangeStart, in.DateRangeEnd)
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
	for slot := range in.Entities {
		allowedSlots[slot] = true
	}
	entityDcids, provenanceDcids := extractEntityAndProvenanceDcids(sdmxResult, allowedSlots)

	var entityProps map[string]*nodeProperties
	var provProps map[string]*provenanceProperties

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		entityBatch := append([]string{in.VariableDcid}, entityDcids...)
		entityProps, err = s.fetchEntityProperties(gCtx, entityBatch)
		if err != nil {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}
			log.Printf("Agent getObservationsSdmx: entity metadata enrichment failed: %v", err)
			entityProps = make(map[string]*nodeProperties)
			return nil
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
			log.Printf("Agent getObservationsSdmx: provenance metadata enrichment failed: %v", err)
			provProps = make(map[string]*provenanceProperties)
			return nil
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, status.Errorf(codes.Internal, "metadata enrichment failed: %v", err)
	}

	entityMetaTable := buildEntityMetadataTable(entityDcids, entityProps)
	return s.buildSdmxResponse(in, sdmxResult, entityMetaTable, filter, entityProps, provProps), nil
}

// buildSdmxDataQuery maps agent request constraints into an SDMX data query, including native facetId pushdown.
func buildSdmxDataQuery(in *GetObservationsRequest) (*sdmxpb.SdmxDataQuery, error) {
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		datacommons.ComponentVariableMeasured: makeSdmxConstraint(in.VariableDcid),
	}

	var parentCount int
	for slot, val := range in.Entities {
		if slot == "" {
			return nil, status.Error(codes.InvalidArgument, "entity slot name cannot be empty")
		}
		if _, isReserved := reservedSlotDimensions[slot]; isReserved {
			return nil, status.Errorf(codes.InvalidArgument, "entity slot name %q is a reserved component dimension", slot)
		}
		if err := validateEntityConstraint(val); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "slot %q: %v", slot, err)
		}

		c := &sdmxpb.SdmxComponentConstraint{}

		if len(val.Dcids) > 0 {
			c.Predicates = makeSdmxPredicates(val.Dcids...)
		}

		if val.ParentSpec != nil {
			parentCount++
			if parentCount > 1 {
				return nil, status.Error(codes.InvalidArgument, "at most 1 parent entity can be expanded per request")
			}

			c.PropertyConstraints = map[string]*sdmxpb.SdmxPropertyConstraint{
				"containedInPlace": {
					Transitive: true,
					Predicates: makeSdmxPredicates(val.ParentSpec.ParentDcid),
				},
				"typeOf": {
					Predicates: makeSdmxPredicates(val.ParentSpec.ChildType),
				},
			}
		}

		constraints[slot] = c
	}

	if in.SourceOverride != "" {
		constraints[datacommons.ComponentFacetID] = makeSdmxConstraint(in.SourceOverride)
	}
	return &sdmxpb.SdmxDataQuery{Constraints: constraints}, nil
}

// validateEntityConstraint verifies that an EntityConstraint specifies valid direct DCIDs or a valid ParentPlaceSpec.
func validateEntityConstraint(val *EntityConstraint) error {
	if val == nil || (len(val.Dcids) == 0 && val.ParentSpec == nil) {
		return fmt.Errorf("entity specification cannot be empty")
	}
	if len(val.Dcids) > 0 && val.ParentSpec != nil {
		return fmt.Errorf("entity specification cannot specify both dcids and parent_spec")
	}
	for _, dcid := range val.Dcids {
		if dcid == "" {
			return fmt.Errorf("list contains empty string DCID")
		}
	}
	if val.ParentSpec != nil {
		if val.ParentSpec.ParentDcid == "" || val.ParentSpec.ChildType == "" {
			return fmt.Errorf("parent_dcid and child_type cannot be empty")
		}
	}
	return nil
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

// fetchEntityProperties resolves names for spatial entities and variables.
func (s *Service) fetchEntityProperties(ctx context.Context, dcids []string) (map[string]*nodeProperties, error) {
	props := make(map[string]*nodeProperties)
	if len(dcids) == 0 {
		return props, nil
	}

	nodeReq := &pbv2.NodeRequest{
		Nodes:    dcids,
		Property: "->" + arcName,
	}
	nodeResp, err := s.fetchAllNodes(ctx, nodeReq)
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
			}
		}
		props[dcid] = p
	}
	return props, nil
}

// fetchProvenanceProperties resolves URLs for provenances.
func (s *Service) fetchProvenanceProperties(ctx context.Context, dcids []string) (map[string]*provenanceProperties, error) {
	props := make(map[string]*provenanceProperties)
	if len(dcids) == 0 {
		return props, nil
	}

	nodeReq := &pbv2.NodeRequest{
		Nodes:    dcids,
		Property: "->[url]",
	}
	nodeResp, err := s.fetchAllNodes(ctx, nodeReq)
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

// buildEntityMetadataTable compiles names for spatial entities into a flat metadata Table.
func buildEntityMetadataTable(dcids []string, props map[string]*nodeProperties) *Table {
	table := &Table{
		Columns: []string{colDcid, colName},
	}
	for _, dcid := range dcids {
		var name string
		if p, ok := props[dcid]; ok {
			name = p.name
		}
		table.Rows = append(table.Rows, []any{dcid, name})
	}
	return table
}

// buildSdmxResponse compiles SDMX data result into GetObservationsResponse with dual tables.
// Entity metadata is limited to the entities that appear in the returned data rows.
func (s *Service) buildSdmxResponse(
	in *GetObservationsRequest,
	result *sdmxpb.SdmxDataResult,
	entityMetaTable *Table,
	filter *dateFilter,
	entityProps map[string]*nodeProperties,
	provProps map[string]*provenanceProperties,
) *GetObservationsResponse {
	resp := &GetObservationsResponse{
		Variable:       &ObservationNode{Dcid: in.VariableDcid},
		EntityMetadata: entityMetaTable,
		Data:           &Table{},
	}

	if p, ok := entityProps[in.VariableDcid]; ok {
		resp.Variable.Name = p.name
	}

	if result == nil || len(result.GetSeries()) == 0 {
		return resp
	}

	dimSlots := extractDimensionSlots(result)
	resp.Data.Columns = append(append([]string{}, dimSlots...), colDate, colValue)

	primaryFacetID, facetsMap, statsMap := rankSdmxFacets(in, result, filter, provProps)
	if primaryFacetID == "" {
		return resp
	}

	rows, activeEntities := buildSdmxDataRows(result, primaryFacetID, dimSlots, filter)
	resp.Data.Rows = rows
	resp.EntityMetadata.Rows = filterEntityMetadataRows(resp.EntityMetadata.Rows, activeEntities)

	sourceMetadata, altSources := buildSourceMetadata(primaryFacetID, facetsMap, statsMap, len(resp.EntityMetadata.Rows))
	resp.SourceMetadata = sourceMetadata
	resp.AlternativeSources = altSources

	return resp
}

// filterEntityMetadataRows keeps the entity metadata rows whose DCID (first column) is in dcids.
func filterEntityMetadataRows(rows [][]any, dcids map[string]bool) [][]any {
	var kept [][]any
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		if dcid, ok := row[0].(string); ok && dcids[dcid] {
			kept = append(kept, row)
		}
	}
	return kept
}

// extractDimensionSlots extracts and sorts non-metadata dimension slot column names.
func extractDimensionSlots(result *sdmxpb.SdmxDataResult) []string {
	dimSlotsSet := make(map[string]bool)
	for _, series := range result.GetSeries() {
		for dimKey := range series.GetDimensions() {
			if _, isReserved := reservedSlotDimensions[dimKey]; !isReserved {
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

// buildFacetMetadata constructs an ObservationFacetMetadata struct from a time series.
func buildFacetMetadata(series *sdmxpb.SdmxTimeSeries, facetID string, provProps map[string]*provenanceProperties) *ObservationFacetMetadata {
	provDcid := series.GetDimensions()[datacommons.ComponentProvenance]
	var provUrl string
	if p, ok := provProps[provDcid]; ok {
		provUrl = p.provenanceUrl
	}

	unit := series.GetAttributes()[datacommons.ComponentUnit]
	if unit == "" {
		unit = series.GetDimensions()[datacommons.ComponentUnit]
	}

	return &ObservationFacetMetadata{
		SourceID:          facetID,
		MeasurementMethod: series.GetDimensions()[datacommons.ComponentMeasurementMethod],
		ObservationPeriod: series.GetDimensions()[datacommons.ComponentObservationPeriod],
		ProvenanceURL:     provUrl,
		Unit:              unit,
	}
}

// rankSdmxFacets evaluates and ranks all facets present in the SDMX result.
func rankSdmxFacets(
	in *GetObservationsRequest,
	result *sdmxpb.SdmxDataResult,
	filter *dateFilter,
	provProps map[string]*provenanceProperties,
) (string, map[string]*ObservationFacetMetadata, map[string]*facetStats) {
	facetsMap := make(map[string]*ObservationFacetMetadata)
	statsMap := make(map[string]*facetStats)
	placesByFacet := make(map[string]map[string]bool)

	slots := util.SortedStringKeys(in.Entities)

	for _, series := range result.GetSeries() {
		facetID := series.GetAttributes()[attrFacetID]
		if facetID == "" {
			facetID = "unknown"
		}

		if _, ok := facetsMap[facetID]; !ok {
			facetsMap[facetID] = buildFacetMetadata(series, facetID, provProps)
		}

		targetPoints := filterPointsByDate(series.GetPoints(), filter)
		if len(targetPoints) > 0 {
			if _, ok := statsMap[facetID]; !ok {
				statsMap[facetID] = &facetStats{facetID: facetID}
				placesByFacet[facetID] = make(map[string]bool)
			}
			// Only places with data in the requested date window count toward the facet.
			collectSeriesEntities(series, slots, placesByFacet[facetID])
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
		statsList = append(statsList, stats)
	}

	if len(statsList) == 0 {
		for facetID := range facetsMap {
			statsList = append(statsList, &facetStats{facetID: facetID})
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
		return si.facetID < sj.facetID
	})
}

// buildSdmxDataRows constructs table rows for observation data matching the primary facet.
// It also returns the set of entity DCIDs that appear in the emitted rows' dimension columns.
func buildSdmxDataRows(
	result *sdmxpb.SdmxDataResult,
	primaryFacetID string,
	dimSlots []string,
	filter *dateFilter,
) ([][]any, map[string]bool) {
	var rows [][]any
	activeEntities := make(map[string]bool)

	for _, series := range result.GetSeries() {
		facetID := series.GetAttributes()[attrFacetID]
		if facetID == "" {
			facetID = "unknown"
		}
		if facetID != primaryFacetID {
			continue
		}

		targetPoints := filterPointsByDate(series.GetPoints(), filter)
		if len(targetPoints) == 0 {
			continue
		}
		collectSeriesEntities(series, dimSlots, activeEntities)

		var dimVals []any
		for _, slot := range dimSlots {
			dimVals = append(dimVals, series.GetDimensions()[slot])
		}

		for _, point := range targetPoints {
			val, err := strconv.ParseFloat(point.GetObservationValue(), 64)
			var parsedVal any = val
			if err != nil {
				parsedVal = point.GetObservationValue()
			}

			rowVals := make([]any, 0, len(dimVals)+2)
			rowVals = append(rowVals, dimVals...)
			rowVals = append(rowVals, point.GetTimePeriod(), parsedVal)
			rows = append(rows, rowVals)
		}
	}

	sortSdmxRows(rows)
	return rows, activeEntities
}

// collectSeriesEntities adds the series' non-empty dimension values for the given slots to set.
func collectSeriesEntities(series *sdmxpb.SdmxTimeSeries, slots []string, set map[string]bool) {
	for _, slot := range slots {
		if val := series.GetDimensions()[slot]; val != "" {
			set[val] = true
		}
	}
}

// buildSourceMetadata constructs the primary and alternative source metadata for response.
func buildSourceMetadata(
	primaryFacetID string,
	facetsMap map[string]*ObservationFacetMetadata,
	statsMap map[string]*facetStats,
	entityRowCount int,
) (*ObservationFacetMetadata, []*AlternativeSource) {
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

	var altSources []*AlternativeSource
	for _, id := range altIDs {
		alt := &AlternativeSource{
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
func sortSdmxRows(rows [][]any) {
	sort.SliceStable(rows, func(i, j int) bool {
		rowI := rows[i]
		rowJ := rows[j]
		limit := len(rowI) - 1
		for k := 0; k < limit; k++ {
			valI, _ := rowI[k].(string)
			valJ, _ := rowJ[k].(string)
			if valI != valJ {
				return valI < valJ
			}
		}
		return false
	})
}
