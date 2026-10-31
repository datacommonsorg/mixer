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
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

const (
	// v2MaxVariableDcids caps variable_dcids per call when querying explicit entities. Keep in sync with
	// internal/server/mcp/v2_types.go.
	v2MaxVariableDcids = 10
	// v2MaxChildVariableDcids caps variable_dcids per call when child_entity_type_dcid is set, since each
	// variable expands across every child entity under the parent.
	v2MaxChildVariableDcids = 2
	// v2MaxAdditionalEntityProps caps keys in additional_entities alongside entity_property, matching the
	// 3-slot (entity1, entity2, entity3) TimeSeries schema.
	v2MaxAdditionalEntityProps = 2

	v2VarDiagPropertiesQuery    = "->[typeOf, observationProperties]"
	v2ProvenancePropertiesQuery = "->[name, url]"
)

// v2ProvenanceMeta holds the provenance name and URL resolved from the knowledge graph.
type v2ProvenanceMeta struct {
	name string
	url  string
}

// v2RawFacet holds the raw series dimensions/attributes for one facet before provenance enrichment.
type v2RawFacet struct {
	sourceID          string
	provenanceDcid    string
	unit              string
	scalingFactor     string
	measurementMethod string
	observationPeriod string
}

// v2EvaluatedVariable holds the filtered table rows, ranked facets, and empty-result state for one variable.
type v2EvaluatedVariable struct {
	dcid           string
	dimSlots       []string
	rows           [][]any
	activeEntities map[string]bool
	primaryFacetID string
	rankedFacets   []*facetStats
	facetsByID     map[string]*v2RawFacet
	note           string
}

// V2GetObservations fetches observation series for 1 to 10 statistical variables sharing observation_properties
// across explicit entities, child entities of a parent, or multi-entity relationship slots.
func (s *Service) V2GetObservations(
	ctx context.Context,
	req *V2GetObservationsRequest,
) (*V2GetObservationsResponse, error) {
	req = normalizeV2GetObservationsRequest(req)
	filter, err := validateV2GetObservationsRequest(req)
	if err != nil {
		return nil, err
	}
	defer util.TimeTrack(time.Now(), "Agent: V2GetObservations")
	slog.Info("V2GetObservations started",
		"variablesCount", len(req.VariableDcids),
		"entitiesCount", len(req.EntityDcids),
		"childEntityType", req.ChildEntityTypeDcid,
		"entityProperty", req.EntityProperty,
		"additionalPropsCount", len(req.AdditionalEntities),
		"date", req.Date)

	sdmxReq := buildV2SdmxDataQuery(req, filter)
	sdmxResult, err := s.executeV2SdmxData(ctx, req, sdmxReq)
	if err != nil {
		return nil, err
	}

	byVariable := partitionSeriesByVariable(sdmxResult, req.VariableDcids)
	scope := v2ObservationNoteScope(req)
	evaluated, unionActiveEntities, provenanceDcids := evaluateAllV2Variables(req, sdmxResult, byVariable, filter, scope)

	// Entity and provenance metadata are fetched only after per-variable facet selection so child-mode and
	// multi-facet responses resolve names solely for entities and provenances present in the winning rows.
	namesByDcid, provByDcid, err := s.fetchV2ObservationMetadata(ctx, req.VariableDcids, unionActiveEntities, provenanceDcids)
	if err != nil {
		return nil, err
	}

	resp := buildV2GetObservationsResponse(evaluated, unionActiveEntities, namesByDcid, provByDcid)
	slog.Info("V2GetObservations completed",
		"variablesCount", len(resp.Observations),
		"activeEntitiesCount", len(resp.EntityMetadata.Rows))
	return resp, nil
}

// normalizeV2GetObservationsRequest trims strings, deduplicates DCID slices in order, and defaults
// EntityProperty to "observationAbout". Keys in AdditionalEntities whose values are empty or blank are
// preserved with an empty slice so validation can report which property was empty.
func normalizeV2GetObservationsRequest(req *V2GetObservationsRequest) *V2GetObservationsRequest {
	if req == nil {
		return &V2GetObservationsRequest{EntityProperty: datacommons.ComponentObservationAbout}
	}
	entityProp := strings.TrimSpace(req.EntityProperty)
	if entityProp == "" {
		entityProp = datacommons.ComponentObservationAbout
	}
	var addl map[string][]string
	if len(req.AdditionalEntities) > 0 {
		addl = make(map[string][]string, len(req.AdditionalEntities))
		for _, rawProp := range slices.Sorted(maps.Keys(req.AdditionalEntities)) {
			prop := strings.TrimSpace(rawProp)
			cleaned := uniqueNonBlank(req.AdditionalEntities[rawProp])
			if existing, ok := addl[prop]; ok {
				addl[prop] = uniqueNonBlank(append(existing, cleaned...))
			} else {
				addl[prop] = cleaned
			}
		}
	}
	return &V2GetObservationsRequest{
		VariableDcids:       uniqueNonBlank(req.VariableDcids),
		EntityDcids:         uniqueNonBlank(req.EntityDcids),
		ChildEntityTypeDcid: strings.TrimSpace(req.ChildEntityTypeDcid),
		EntityProperty:      entityProp,
		AdditionalEntities:  addl,
		Date:                strings.TrimSpace(req.Date),
		DateRangeStart:      strings.TrimSpace(req.DateRangeStart),
		DateRangeEnd:        strings.TrimSpace(req.DateRangeEnd),
		SourceOverride:      strings.TrimSpace(req.SourceOverride),
	}
}

// validateV2GetObservationsRequest checks all variable, entity, property, and date invariants on a
// normalized request and returns the parsed date filter.
func validateV2GetObservationsRequest(req *V2GetObservationsRequest) (*dateFilter, error) {
	if len(req.VariableDcids) == 0 {
		return nil, errV2NoVariableDcids()
	}
	var topicDcids []string
	for _, dcid := range req.VariableDcids {
		if isV2TopicPrefix(dcid) {
			topicDcids = append(topicDcids, dcid)
		}
	}
	if len(topicDcids) > 0 {
		return nil, errV2TopicAsVariableDcids(topicDcids)
	}
	if req.SourceOverride != "" && len(req.VariableDcids) > 1 {
		return nil, errV2SourceOverrideNeedsOneVariable(len(req.VariableDcids))
	}
	if req.ChildEntityTypeDcid != "" && len(req.VariableDcids) > v2MaxChildVariableDcids {
		return nil, errV2TooManyChildVariableDcids(len(req.VariableDcids))
	}
	if len(req.VariableDcids) > v2MaxVariableDcids {
		return nil, errV2TooManyVariableDcids(len(req.VariableDcids))
	}
	if err := validateV2EntityConstraints(req); err != nil {
		return nil, err
	}
	return parseV2DateFilter(req.Date, req.DateRangeStart, req.DateRangeEnd, req.ChildEntityTypeDcid != "")
}

// validateV2EntityConstraints checks entity_dcids, child_entity_type_dcid, entity_property, and
// additional_entities limits and reserved names.
func validateV2EntityConstraints(req *V2GetObservationsRequest) error {
	if req.ChildEntityTypeDcid != "" {
		if len(req.EntityDcids) != 1 {
			return errV2ChildModeNeedsOneEntity(len(req.EntityDcids))
		}
	} else {
		if len(req.EntityDcids) == 0 {
			return errV2NoEntityDcids()
		}
		if len(req.EntityDcids) > v2MaxEntityDcids {
			return errV2TooManyEntityDcids(len(req.EntityDcids))
		}
	}
	if _, reserved := reservedSlotDimensions[req.EntityProperty]; reserved {
		return errV2ReservedEntityProperty(req.EntityProperty)
	}
	if len(req.AdditionalEntities) > v2MaxAdditionalEntityProps {
		return errV2TooManyAdditionalEntityProps(len(req.AdditionalEntities))
	}
	for _, prop := range slices.Sorted(maps.Keys(req.AdditionalEntities)) {
		if prop == "" {
			return errV2ReservedEntityProperty(prop)
		}
		if _, reserved := reservedSlotDimensions[prop]; reserved {
			return errV2ReservedEntityProperty(prop)
		}
		if prop == req.EntityProperty {
			return errV2DuplicateEntityProperty(prop)
		}
		dcids := req.AdditionalEntities[prop]
		if len(dcids) == 0 {
			return errV2EmptyAdditionalEntityDcids(prop)
		}
		if len(dcids) > v2MaxEntityDcids {
			return errV2TooManyAdditionalEntityDcids(prop, len(dcids))
		}
	}
	return nil
}

// parseV2DateFilter parses v2 date arguments, supporting case-insensitive keywords, range auto-inference
// when date is omitted alongside range bounds, conflict detection when range bounds accompany non-range
// dates, and child-mode boundedness checks.
func parseV2DateFilter(date, startStr, endStr string, childMode bool) (*dateFilter, error) {
	hasBounds := startStr != "" || endStr != ""
	var effective string
	switch {
	case date == "" && hasBounds:
		effective = dateTypeRange
	case date == "":
		effective = dateTypeLatest
	default:
		lower := strings.ToLower(date)
		if lower == dateTypeLatest || lower == dateTypeAll || lower == dateTypeRange {
			effective = lower
		} else {
			effective = date
		}
	}

	if effective != dateTypeRange && hasBounds {
		return nil, errV2ConflictingDateRange(date)
	}

	switch effective {
	case dateTypeLatest:
		return &dateFilter{dateType: dateTypeLatest}, nil
	case dateTypeAll:
		if childMode {
			return nil, errV2UnboundedChildDate()
		}
		return &dateFilter{dateType: dateTypeAll}, nil
	case dateTypeRange:
		return parseV2DateRange(startStr, endStr, childMode)
	default:
		if err := validateDateFormat(effective); err != nil {
			return nil, errV2InvalidDate(date)
		}
		startVal, endVal, err := parseDateStringToInterval(effective)
		if err != nil {
			return nil, errV2InvalidDate(date)
		}
		return &dateFilter{dateType: dateTypeRange, startDate: startVal, endDate: endVal}, nil
	}
}

// parseV2DateRange validates date_range_start and date_range_end and builds a range dateFilter.
func parseV2DateRange(startStr, endStr string, childMode bool) (*dateFilter, error) {
	if startStr == "" && endStr != "" && childMode {
		return nil, errV2UnboundedChildDate()
	}
	if startStr == "" && endStr == "" {
		return nil, errV2MissingDateRangeBounds()
	}
	var startVal, endVal time.Time
	if startStr != "" {
		if err := validateDateFormat(startStr); err != nil {
			return nil, errV2InvalidDateRangeBound("date_range_start", startStr)
		}
		s, _, err := parseDateStringToInterval(startStr)
		if err != nil {
			return nil, errV2InvalidDateRangeBound("date_range_start", startStr)
		}
		startVal = s
	}
	if endStr != "" {
		if err := validateDateFormat(endStr); err != nil {
			return nil, errV2InvalidDateRangeBound("date_range_end", endStr)
		}
		_, e, err := parseDateStringToInterval(endStr)
		if err != nil {
			return nil, errV2InvalidDateRangeBound("date_range_end", endStr)
		}
		endVal = e
	}
	if !startVal.IsZero() && !endVal.IsZero() && startVal.After(endVal) {
		return nil, errV2InvertedDateRange(startStr, endStr)
	}
	return &dateFilter{dateType: dateTypeRange, startDate: startVal, endDate: endVal}, nil
}

// buildV2SdmxDataQuery compiles a validated v2 request into a single multi-variable SdmxDataQuery.
//
// TIME_PERIOD="LATEST" is pushed down to Spanner so "latest" queries scan only the newest observation per
// time series rather than every historical year. Specific ISO dates (such as "2020") and ranges are filtered
// in Go because SdmxData matches TIME_PERIOD predicates literally, whereas partial ISO dates in MCP match
// any sub-period within that interval (for example "2020-06" inside "2020").
func buildV2SdmxDataQuery(req *V2GetObservationsRequest, filter *dateFilter) *sdmxpb.SdmxDataQuery {
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		datacommons.ComponentVariableMeasured: makeSdmxConstraint(req.VariableDcids...),
		req.EntityProperty:                    buildV2EntitySlotConstraint(req),
	}
	for _, prop := range slices.Sorted(maps.Keys(req.AdditionalEntities)) {
		constraints[prop] = makeSdmxConstraint(req.AdditionalEntities[prop]...)
	}
	if req.SourceOverride != "" {
		constraints[datacommons.ComponentFacetID] = makeSdmxConstraint(req.SourceOverride)
	}
	if filter != nil && filter.dateType == dateTypeLatest {
		constraints[datacommons.ComponentTimePeriod] = makeSdmxConstraint(DateLatest)
	}
	return &sdmxpb.SdmxDataQuery{Constraints: constraints}
}

// buildV2EntitySlotConstraint returns either direct entity predicates or a transitive containedInPlace +
// typeOf property constraint for child mode.
func buildV2EntitySlotConstraint(req *V2GetObservationsRequest) *sdmxpb.SdmxComponentConstraint {
	if req.ChildEntityTypeDcid == "" {
		return makeSdmxConstraint(req.EntityDcids...)
	}
	return &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			datacommons.PropertyContainedInPlace: {
				Transitive: true,
				Predicates: makeSdmxPredicates(req.EntityDcids[0]),
			},
			datacommons.PropertyTypeOf: {
				Predicates: makeSdmxPredicates(req.ChildEntityTypeDcid),
			},
		},
	}
}

// executeV2SdmxData calls SdmxData and translates rejections into actionable v2 errors without leaking raw
// SDMX or Spanner text through wrapV2BackendError.
func (s *Service) executeV2SdmxData(
	ctx context.Context,
	req *V2GetObservationsRequest,
	sdmxReq *sdmxpb.SdmxDataQuery,
) (*sdmxpb.SdmxDataResult, error) {
	res, err := s.mixer.SdmxData(ctx, sdmxReq)
	if err == nil {
		return res, nil
	}
	code := v2BackendStatus(err).Code()
	// SdmxData returns InvalidArgument for incompatible observationProperties, wrong dimension slots, or
	// missing Spanner tables (such as LinkedEdge), and returns Unimplemented when child-mode property
	// constraints target a slot absent from the variable's observationProperties. Inspect variable metadata
	// first so caller mistakes get exact guidance while internal InvalidArgument text never passes through.
	if code == codes.InvalidArgument || code == codes.Unimplemented {
		return nil, s.diagnoseV2SdmxRejection(ctx, req, err, code)
	}
	slog.Warn("V2GetObservations SdmxData failed", "error", err, "variablesCount", len(req.VariableDcids))
	return nil, errV2ObservationsFailed(err)
}

// diagnoseV2SdmxRejection inspects V2Node typeOf and observationProperties on req.VariableDcids to map an
// SdmxData InvalidArgument or Unimplemented rejection to a caller-actionable error.
func (s *Service) diagnoseV2SdmxRejection(
	ctx context.Context,
	req *V2GetObservationsRequest,
	sdmxErr error,
	sdmxCode codes.Code,
) error {
	graphs, err := s.fetchNodeGraphs(ctx, req.VariableDcids, v2VarDiagPropertiesQuery)
	if err != nil {
		slog.Warn("Failed to inspect variables after SdmxData rejection", "error", err, "sdmxError", sdmxErr)
		if sdmxCode == codes.InvalidArgument {
			return errV2ObservationsUnavailable()
		}
		return errV2ObservationsFailed(sdmxErr)
	}

	topics, unknown := classifyV2VariableNodes(req.VariableDcids, graphs)
	if len(topics) > 0 {
		return errV2TopicAsVariableDcids(topics)
	}
	if len(unknown) > 0 {
		return errV2UnknownVariableDcids(unknown)
	}

	propsByVar := make(map[string][]string, len(req.VariableDcids))
	var refProps []string
	mixed := false
	for i, varDcid := range req.VariableDcids {
		props := extractV2ObservationProperties(graphs[varDcid])
		propsByVar[varDcid] = props
		if i == 0 {
			refProps = props
		} else if !slices.Equal(refProps, props) {
			mixed = true
		}
	}
	if mixed {
		parts := make([]string, 0, len(req.VariableDcids))
		for _, varDcid := range req.VariableDcids {
			parts = append(parts, fmt.Sprintf("%s: %q", varDcid, propsByVar[varDcid]))
		}
		return errV2MixedObservationProperties(strings.Join(parts, "; "))
	}

	var badProps []string
	if !slices.Contains(refProps, req.EntityProperty) {
		badProps = append(badProps, req.EntityProperty)
	}
	for _, prop := range slices.Sorted(maps.Keys(req.AdditionalEntities)) {
		if !slices.Contains(refProps, prop) {
			badProps = append(badProps, prop)
		}
	}
	if len(badProps) > 0 {
		return errV2WrongObservationProperty(req.VariableDcids[0], badProps, refProps)
	}

	slog.Warn("V2GetObservations SdmxData rejected valid-looking request", "error", sdmxErr, "variables", req.VariableDcids)
	if sdmxCode == codes.InvalidArgument {
		return errV2ObservationsUnavailable()
	}
	return errV2ObservationsFailed(sdmxErr)
}

// classifyV2VariableNodes partitions variableDcids into topics (Topic) and unknown DCIDs (not typed as
// StatisticalVariable, including StatVarGroup).
func classifyV2VariableNodes(variableDcids []string, graphs map[string]*pbv2.LinkedGraph) (topics, unknown []string) {
	for _, dcid := range variableDcids {
		if isV2TopicPrefix(dcid) {
			topics = append(topics, dcid)
			continue
		}
		types := extractV2ArcStrings(graphs[dcid], datacommons.PropertyTypeOf)
		switch {
		case slices.Contains(types, DcidTypeTopic):
			topics = append(topics, dcid)
		case !slices.Contains(types, DcidTypeVariable):
			unknown = append(unknown, dcid)
		}
	}
	return topics, unknown
}

// extractV2ObservationProperties returns the sorted observation properties of a variable graph, defaulting
// to ["observationAbout"] when none are declared, matching resolveSdmxEntityShape in Spanner.
func extractV2ObservationProperties(graph *pbv2.LinkedGraph) []string {
	props := extractV2ArcStrings(graph, "observationProperties")
	if len(props) == 0 {
		return []string{datacommons.ComponentObservationAbout}
	}
	return props
}

// extractV2ArcStrings returns sorted unique string targets for prop in graph, preferring each node's Dcid
// and falling back to Value so custom graphs whose target nodes lack Node.types still resolve cleanly.
func extractV2ArcStrings(graph *pbv2.LinkedGraph, prop string) []string {
	if graph == nil || graph.GetArcs() == nil {
		return nil
	}
	arc := graph.GetArcs()[prop]
	if arc == nil {
		return nil
	}
	seen := make(map[string]struct{})
	for _, node := range arc.GetNodes() {
		val := strings.TrimSpace(node.GetDcid())
		if val == "" {
			val = strings.TrimSpace(node.GetValue())
		}
		if val != "" {
			seen[val] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(seen))
}

// partitionSeriesByVariable groups SdmxTimeSeries by variableMeasured.
func partitionSeriesByVariable(
	result *sdmxpb.SdmxDataResult,
	variableDcids []string,
) map[string][]*sdmxpb.SdmxTimeSeries {
	byVar := make(map[string][]*sdmxpb.SdmxTimeSeries, len(variableDcids))
	for _, series := range result.GetSeries() {
		varDcid := series.GetDimensions()[datacommons.ComponentVariableMeasured]
		if varDcid == "" && len(variableDcids) == 1 {
			varDcid = variableDcids[0]
		}
		byVar[varDcid] = append(byVar[varDcid], series)
	}
	return byVar
}

// evaluateAllV2Variables filters dates, ranks facets, and builds rows per variable in request order, while
// collecting the sorted union of active entity DCIDs and provenance DCIDs across all variables.
func evaluateAllV2Variables(
	req *V2GetObservationsRequest,
	fullResult *sdmxpb.SdmxDataResult,
	byVariable map[string][]*sdmxpb.SdmxTimeSeries,
	filter *dateFilter,
	scope string,
) ([]*v2EvaluatedVariable, []string, []string) {
	evaluated := make([]*v2EvaluatedVariable, 0, len(req.VariableDcids))
	entitySet := make(map[string]struct{})
	provSet := make(map[string]struct{})

	for _, varDcid := range req.VariableDcids {
		ev := evaluateV2VariableSeries(varDcid, byVariable[varDcid], fullResult, req, filter, scope)
		evaluated = append(evaluated, ev)
		for entityDcid := range ev.activeEntities {
			entitySet[entityDcid] = struct{}{}
		}
		for _, st := range ev.rankedFacets {
			if rawFacet := ev.facetsByID[st.facetID]; rawFacet != nil && rawFacet.provenanceDcid != "" {
				provSet[rawFacet.provenanceDcid] = struct{}{}
			}
		}
	}

	return evaluated, slices.Sorted(maps.Keys(entitySet)), slices.Sorted(maps.Keys(provSet))
}

// evaluateV2VariableSeries filters points, selects the primary facet via sortFacetStats, and extracts table
// rows or an empty-result note for a single variable.
func evaluateV2VariableSeries(
	varDcid string,
	varSeries []*sdmxpb.SdmxTimeSeries,
	fullResult *sdmxpb.SdmxDataResult,
	req *V2GetObservationsRequest,
	filter *dateFilter,
	scope string,
) *v2EvaluatedVariable {
	varResult := &sdmxpb.SdmxDataResult{Series: varSeries}
	dimSlots := extractV2DimensionSlots(varResult, fullResult, req)
	ev := &v2EvaluatedVariable{
		dcid:     varDcid,
		dimSlots: dimSlots,
	}

	if len(varSeries) == 0 {
		if req.SourceOverride != "" {
			ev.note = v2NoObsForSourceNote(varDcid, scope, req.SourceOverride)
		} else {
			ev.note = v2NoObsNote(varDcid, scope)
		}
		return ev
	}

	rankedFacets, facetsByID := rankV2VariableFacets(varSeries, dimSlots, filter)
	if len(rankedFacets) == 0 {
		minDate, maxDate := seriesDateBounds(varSeries)
		switch {
		case minDate != "" && maxDate != "" && req.SourceOverride != "":
			ev.note = v2NoObsForSourceInDateWindowNote(varDcid, scope, req.SourceOverride, minDate, maxDate)
		case minDate != "" && maxDate != "":
			ev.note = v2NoObsInDateWindowNote(varDcid, scope, minDate, maxDate)
		case req.SourceOverride != "":
			ev.note = v2NoObsForSourceNote(varDcid, scope, req.SourceOverride)
		default:
			ev.note = v2NoObsNote(varDcid, scope)
		}
		return ev
	}

	ev.primaryFacetID = rankedFacets[0].facetID
	ev.rankedFacets = rankedFacets
	ev.facetsByID = facetsByID
	ev.rows, ev.activeEntities = buildSdmxDataRows(varResult, ev.primaryFacetID, dimSlots, filter)
	return ev
}

// rankV2VariableFacets evaluates facets that have at least one point in the requested date window and orders
// them using the shared sortFacetStats heuristics, counting distinct entity tuples across dimSlots per facet.
func rankV2VariableFacets(
	varSeries []*sdmxpb.SdmxTimeSeries,
	dimSlots []string,
	filter *dateFilter,
) ([]*facetStats, map[string]*v2RawFacet) {
	facetsByID := make(map[string]*v2RawFacet)
	statsByID := make(map[string]*facetStats)
	tuplesByFacet := make(map[string]map[string]struct{})

	for _, series := range varSeries {
		facetID := series.GetAttributes()[attrFacetID]
		if facetID == "" {
			facetID = "unknown"
		}
		if _, ok := facetsByID[facetID]; !ok {
			facetsByID[facetID] = extractV2RawFacet(series, facetID)
		}

		targetPoints := filterPointsByDate(series.GetPoints(), filter)
		if len(targetPoints) == 0 {
			continue
		}
		if _, ok := statsByID[facetID]; !ok {
			statsByID[facetID] = &facetStats{facetID: facetID}
			tuplesByFacet[facetID] = make(map[string]struct{})
		}
		if tupleKey := seriesEntityTupleKey(series, dimSlots); tupleKey != "" {
			tuplesByFacet[facetID][tupleKey] = struct{}{}
		}
		statsByID[facetID].dateCount += len(targetPoints)
		for _, pt := range targetPoints {
			if t, _, err := parseDateStringToInterval(pt.GetTimePeriod()); err == nil && t.After(statsByID[facetID].latestDate) {
				statsByID[facetID].latestDate = t
			}
		}
	}

	statsList := make([]*facetStats, 0, len(statsByID))
	for facetID, st := range statsByID {
		st.placesFoundCount = len(tuplesByFacet[facetID])
		statsList = append(statsList, st)
	}
	sortFacetStats(statsList)
	return statsList, facetsByID
}

// seriesEntityTupleKey builds a NUL-separated tuple key from the series' dimension values across dimSlots,
// returning "" when every slot is empty or "NotApplicable".
func seriesEntityTupleKey(series *sdmxpb.SdmxTimeSeries, dimSlots []string) string {
	if len(dimSlots) == 0 {
		return ""
	}
	parts := make([]string, len(dimSlots))
	hasAny := false
	for i, slot := range dimSlots {
		val := cleanV2FacetComponent(series.GetDimensions()[slot])
		if val != "" {
			hasAny = true
		}
		parts[i] = val
	}
	if !hasAny {
		return ""
	}
	return strings.Join(parts, "\x00")
}

// extractV2RawFacet reads the facet metadata components from a single SdmxTimeSeries.
func extractV2RawFacet(series *sdmxpb.SdmxTimeSeries, facetID string) *v2RawFacet {
	unit := cleanV2FacetComponent(series.GetAttributes()[datacommons.ComponentUnit])
	if unit == "" {
		unit = cleanV2FacetComponent(series.GetDimensions()[datacommons.ComponentUnit])
	}
	scalingFactor := cleanV2FacetComponent(series.GetAttributes()[datacommons.ComponentScalingFactor])
	if scalingFactor == "" {
		scalingFactor = cleanV2FacetComponent(series.GetDimensions()[datacommons.ComponentScalingFactor])
	}
	return &v2RawFacet{
		sourceID:          facetID,
		provenanceDcid:    cleanV2FacetComponent(series.GetDimensions()[datacommons.ComponentProvenance]),
		unit:              unit,
		scalingFactor:     scalingFactor,
		measurementMethod: cleanV2FacetComponent(series.GetDimensions()[datacommons.ComponentMeasurementMethod]),
		observationPeriod: cleanV2FacetComponent(series.GetDimensions()[datacommons.ComponentObservationPeriod]),
	}
}

// cleanV2FacetComponent strips SDMX's "NotApplicable" sentinel if present.
func cleanV2FacetComponent(val string) string {
	if val == datacommons.FallbackNotAvailable {
		return ""
	}
	return val
}

// seriesDateBounds returns the earliest and latest observation date strings across all points in varSeries.
func seriesDateBounds(varSeries []*sdmxpb.SdmxTimeSeries) (string, string) {
	var minStr, maxStr string
	var minTime, maxTime time.Time
	for _, series := range varSeries {
		for _, pt := range series.GetPoints() {
			period := pt.GetTimePeriod()
			t, _, err := parseDateStringToInterval(period)
			if err != nil {
				continue
			}
			if minStr == "" || t.Before(minTime) || (t.Equal(minTime) && period < minStr) {
				minTime = t
				minStr = period
			}
			if maxStr == "" || t.After(maxTime) || (t.Equal(maxTime) && period > maxStr) {
				maxTime = t
				maxStr = period
			}
		}
	}
	return minStr, maxStr
}

// fetchV2ObservationMetadata resolves display names for variables and active entities in parallel with
// provenance names and URLs. Non-cancellation V2Node failures degrade gracefully so observation numbers are
// still returned even if metadata enrichment fails.
func (s *Service) fetchV2ObservationMetadata(
	ctx context.Context,
	variableDcids []string,
	activeEntities []string,
	provenanceDcids []string,
) (map[string]*nodeProperties, map[string]*v2ProvenanceMeta, error) {
	toFetchNames := append(slices.Clone(variableDcids), activeEntities...)

	var namesByDcid map[string]*nodeProperties
	var provByDcid map[string]*v2ProvenanceMeta

	g, gCtx := errgroup.WithContext(ctx)
	if len(toFetchNames) > 0 {
		g.Go(func() error {
			props, err := s.fetchEntityProperties(gCtx, toFetchNames)
			if err != nil {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}
				slog.Warn("V2GetObservations entity name enrichment failed", "error", err)
				return nil
			}
			namesByDcid = props
			return nil
		})
	}
	if len(provenanceDcids) > 0 {
		g.Go(func() error {
			props, err := s.fetchV2ProvenanceMetadata(gCtx, provenanceDcids)
			if err != nil {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}
				slog.Warn("V2GetObservations provenance enrichment failed", "error", err)
				return nil
			}
			provByDcid = props
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, errV2ObservationsFailed(err)
	}

	if namesByDcid == nil {
		namesByDcid = make(map[string]*nodeProperties)
	}
	if provByDcid == nil {
		provByDcid = make(map[string]*v2ProvenanceMeta)
	}
	return namesByDcid, provByDcid, nil
}

// fetchV2ProvenanceMetadata resolves ->[name, url] for provenance DCIDs.
func (s *Service) fetchV2ProvenanceMetadata(ctx context.Context, dcids []string) (map[string]*v2ProvenanceMeta, error) {
	graphs, err := s.fetchNodeGraphs(ctx, dcids, v2ProvenancePropertiesQuery)
	if err != nil {
		return nil, err
	}
	res := make(map[string]*v2ProvenanceMeta, len(dcids))
	for _, dcid := range dcids {
		graph := graphs[dcid]
		res[dcid] = &v2ProvenanceMeta{
			name: getPropValue(graph, arcName),
			url:  getPropValue(graph, "url"),
		}
	}
	return res, nil
}

// buildV2GetObservationsResponse assembles the top-level entity_metadata table and per-variable observations
// entries in request order.
func buildV2GetObservationsResponse(
	evaluated []*v2EvaluatedVariable,
	unionActiveEntities []string,
	namesByDcid map[string]*nodeProperties,
	provByDcid map[string]*v2ProvenanceMeta,
) *V2GetObservationsResponse {
	obsList := make([]*V2VariableObservations, 0, len(evaluated))
	for _, ev := range evaluated {
		var varName string
		if p, ok := namesByDcid[ev.dcid]; ok {
			varName = p.name
		}
		cols := make([]string, 0, len(ev.dimSlots)+2)
		cols = append(cols, ev.dimSlots...)
		cols = append(cols, colDate, colValue)

		entry := &V2VariableObservations{
			Variable: &ObservationNode{Dcid: ev.dcid, Name: varName},
			Data:     &Table{Columns: cols, Rows: ev.rows},
			Note:     ev.note,
		}
		if len(ev.rows) > 0 && ev.primaryFacetID != "" {
			entry.SourceMetadata, entry.AlternativeSources = buildV2SourceMetadata(ev, provByDcid)
		}
		obsList = append(obsList, entry)
	}

	return &V2GetObservationsResponse{
		EntityMetadata: buildEntityMetadataTable(unionActiveEntities, namesByDcid),
		Observations:   obsList,
	}
}

// buildV2SourceMetadata constructs the primary source_metadata and ranked alternative_sources for one
// variable.
func buildV2SourceMetadata(
	ev *v2EvaluatedVariable,
	provByDcid map[string]*v2ProvenanceMeta,
) (*V2ObservationSourceMeta, []*V2AlternativeSource) {
	primary := buildV2FacetMeta(ev.facetsByID[ev.primaryFacetID], provByDcid)
	if len(ev.rankedFacets) <= 1 {
		return primary, nil
	}
	includeEntityCounts := ev.rankedFacets[0].placesFoundCount > 1
	alts := make([]*V2AlternativeSource, 0, len(ev.rankedFacets)-1)
	for _, st := range ev.rankedFacets[1:] {
		alt := &V2AlternativeSource{
			SourceMetadata: buildV2FacetMeta(ev.facetsByID[st.facetID], provByDcid),
		}
		if includeEntityCounts {
			c := int32(st.placesFoundCount)
			alt.EntitiesFoundCount = &c
		}
		alts = append(alts, alt)
	}
	return primary, alts
}

// buildV2FacetMeta converts a v2RawFacet and resolved provenance properties into V2ObservationSourceMeta,
// falling back to the provenance DCID when the graph lacks a display name.
func buildV2FacetMeta(raw *v2RawFacet, provByDcid map[string]*v2ProvenanceMeta) *V2ObservationSourceMeta {
	if raw == nil {
		return nil
	}
	var importName, provURL string
	if raw.provenanceDcid != "" {
		if pm, ok := provByDcid[raw.provenanceDcid]; ok {
			importName = pm.name
			provURL = pm.url
		}
		if importName == "" {
			importName = raw.provenanceDcid
		}
	}
	return &V2ObservationSourceMeta{
		SourceID:          raw.sourceID,
		ImportName:        importName,
		ProvenanceURL:     provURL,
		Unit:              raw.unit,
		ScalingFactor:     raw.scalingFactor,
		MeasurementMethod: raw.measurementMethod,
		ObservationPeriod: raw.observationPeriod,
	}
}

// extractV2DimensionSlots returns the sorted non-reserved observation property dimension slots for a
// variable, falling back to fullResult's shape/series or the request's constrained properties when the
// variable returned zero series.
func extractV2DimensionSlots(
	varResult *sdmxpb.SdmxDataResult,
	fullResult *sdmxpb.SdmxDataResult,
	req *V2GetObservationsRequest,
) []string {
	if slots := extractDimensionSlots(varResult); len(slots) > 0 {
		return slots
	}
	if fullResult != nil {
		if shapeSlots := extractShapeDimensionSlots(fullResult.GetShape()); len(shapeSlots) > 0 {
			return shapeSlots
		}
		if slots := extractDimensionSlots(fullResult); len(slots) > 0 {
			return slots
		}
	}
	slotSet := map[string]struct{}{req.EntityProperty: {}}
	for prop := range req.AdditionalEntities {
		slotSet[prop] = struct{}{}
	}
	return slices.Sorted(maps.Keys(slotSet))
}

// extractShapeDimensionSlots extracts sorted non-reserved dimension IDs from an SdmxDataShape.
func extractShapeDimensionSlots(shape *sdmxpb.SdmxDataShape) []string {
	if shape == nil {
		return nil
	}
	slotSet := make(map[string]struct{})
	for _, comp := range shape.GetComponents() {
		if comp.GetKind() != sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION {
			continue
		}
		if _, reserved := reservedSlotDimensions[comp.GetId()]; !reserved && comp.GetId() != "" {
			slotSet[comp.GetId()] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(slotSet))
}

// v2ObservationNoteScope returns the entity scope description used in empty-result notes.
func v2ObservationNoteScope(req *V2GetObservationsRequest) string {
	hasAddl := len(req.AdditionalEntities) > 0
	if req.ChildEntityTypeDcid != "" && len(req.EntityDcids) == 1 {
		if hasAddl {
			return v2ChildAndAdditionalEntitiesScope(req.EntityDcids[0], req.ChildEntityTypeDcid)
		}
		return v2ChildEntitiesScope(req.EntityDcids[0], req.ChildEntityTypeDcid)
	}
	if hasAddl {
		return v2GivenEntitiesAndAdditionalScope
	}
	return v2GivenEntitiesScope
}

// isV2TopicPrefix reports whether dcid uses the canonical topic prefix.
func isV2TopicPrefix(dcid string) bool {
	return strings.HasPrefix(dcid, "dc/topic/")
}
