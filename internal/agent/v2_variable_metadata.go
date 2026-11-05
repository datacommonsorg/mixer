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
	"log/slog"
	"slices"
	"time"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

// v2MetadataSourceColumns are the columns of a variable's sources table (see V2VariableMetadata).
var v2MetadataSourceColumns = []string{
	colV2SourceID, colV2ImportName, colV2ProvenanceURL, colV2Unit, colV2ScalingFactor, colV2MeasurementMethod,
	colV2ObservationPeriod, colV2EarliestDate, colV2LatestDate, colV2ObservationCount, colEntitiesWithData,
}

// v2MetadataVariable holds a variable's sources in rank order, with each source's facet metadata and coverage.
// Every ranked facet has entries in facetsByID and coverage, since ranking needs a series with observations.
type v2MetadataVariable struct {
	dcid         string
	rankedFacets []*facetStats
	facetsByID   map[string]*v2RawFacet
	coverage     map[string]*v2FacetCoverage
}

// v2FacetCoverage summarizes a source's series for one variable: the span and number of its observations, and
// the values of the constrained entity property it has data for.
type v2FacetCoverage struct {
	earliestDate     string
	latestDate       string
	observationCount int
	entities         *dcidSet
}

// V2GetVariableMetadata returns, for 1 to 10 statistical variables sharing observation_properties, each
// variable's definition and the sources with data for the requested entities, ranked as V2GetObservations
// ranks them for the latest date, plus the details of the sources' datasets.
//
// Coverage comes from one SdmxData query over all dates, built and validated exactly as for
// V2GetObservations. In child mode the query runs on the sample of the parent's child entities that
// V2SearchIndicators checks (see requireChildEntitySample): coverage is reported for the parent, ranking
// reflects the sample, and observation counts are left empty since they would only count the sample. The
// sample is not exposed to callers.
func (s *Service) V2GetVariableMetadata(
	ctx context.Context,
	req *V2GetVariableMetadataRequest,
) (*V2GetVariableMetadataResponse, error) {
	obsReq := normalizeV2GetObservationsRequest(toV2ObservationsRequest(req))
	// The request has no dates, so validation parses the default date and only variable and entity checks can
	// fail.
	if _, err := validateV2GetObservationsRequest(obsReq); err != nil {
		return nil, err
	}
	defer util.TimeTrack(time.Now(), "Agent: V2GetVariableMetadata")
	slog.Info("V2GetVariableMetadata started",
		"variablesCount", len(obsReq.VariableDcids),
		"entitiesCount", len(obsReq.EntityDcids),
		"childEntityType", obsReq.ChildEntityTypeDcid,
		"entityProperty", obsReq.EntityProperty,
		"additionalPropsCount", len(obsReq.AdditionalEntities))

	queryReq, err := s.v2MetadataQueryRequest(ctx, obsReq)
	if err != nil {
		return nil, err
	}
	sdmxResult, definitions, err := s.fetchV2MetadataSeriesAndDefinitions(ctx, queryReq)
	if err != nil {
		return nil, err
	}
	variables := evaluateV2MetadataVariables(queryReq, sdmxResult)
	provByDcid, datasets := s.fetchV2DatasetMetadata(ctx, collectV2ProvenanceDcids(variables))
	// The lookups degrade on failure, so a cancelled request would otherwise return a partial response as a
	// success.
	if err := ctx.Err(); err != nil {
		return nil, errV2ObservationsFailed(err)
	}

	resp := buildV2GetVariableMetadataResponse(obsReq, variables, definitions, provByDcid, datasets)
	slog.Info("V2GetVariableMetadata completed",
		"variablesCount", len(resp.Variables),
		"datasetsCount", len(resp.Datasets))
	return resp, nil
}

// toV2ObservationsRequest returns a V2GetObservationsRequest with req's variables and entity scope and no dates
// or source_override, so V2GetObservations normalization, validation, query building and errors apply
// unchanged. A nil request stays nil, which normalization turns into an empty request.
func toV2ObservationsRequest(req *V2GetVariableMetadataRequest) *V2GetObservationsRequest {
	if req == nil {
		return nil
	}
	return &V2GetObservationsRequest{
		VariableDcids:       req.VariableDcids,
		EntityDcids:         req.EntityDcids,
		ChildEntityTypeDcid: req.ChildEntityTypeDcid,
		EntityProperty:      req.EntityProperty,
		AdditionalEntities:  req.AdditionalEntities,
	}
}

// v2MetadataQueryRequest returns the request whose entities the SdmxData query constrains: req itself for
// explicit entities, or in child mode a copy that constrains the sample of the parent's child entities instead
// of the child type. An empty sample gets the same errors as in V2SearchIndicators (see
// requireChildEntitySample).
func (s *Service) v2MetadataQueryRequest(ctx context.Context, req *V2GetObservationsRequest) (*V2GetObservationsRequest, error) {
	if req.ChildEntityTypeDcid == "" {
		return req, nil
	}
	children, err := s.requireChildEntitySample(ctx, req.EntityDcids[0], req.ChildEntityTypeDcid)
	if err != nil {
		return nil, err
	}
	sampled := *req
	sampled.EntityDcids = children
	sampled.ChildEntityTypeDcid = ""
	return &sampled, nil
}

// fetchV2MetadataSeriesAndDefinitions fetches, in parallel, every observation of the variables on the query
// request's entities (via SdmxData, with V2GetObservations' error handling) and the variables' outgoing
// properties. A failed property lookup is logged and returns no definitions, so sources are still returned.
func (s *Service) fetchV2MetadataSeriesAndDefinitions(
	ctx context.Context,
	queryReq *V2GetObservationsRequest,
) (*sdmxpb.SdmxDataResult, map[string]*pbv2.LinkedGraph, error) {
	var sdmxResult *sdmxpb.SdmxDataResult
	var definitions map[string]*pbv2.LinkedGraph
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		sdmxReq := buildV2SdmxDataQuery(queryReq, &dateFilter{dateType: dateTypeAll})
		var err error
		sdmxResult, err = s.executeV2SdmxData(gCtx, queryReq, sdmxReq)
		return err
	})
	g.Go(func() error {
		graphs, err := s.fetchNodeGraphs(gCtx, queryReq.VariableDcids, wildcardPropertyQuery)
		if err != nil {
			// Cancellation is reported by SdmxData or by the caller's context check, so it is not logged here.
			if gCtx.Err() == nil {
				slog.Warn("V2GetVariableMetadata variable property lookup failed", "error", err)
			}
			return nil
		}
		definitions = graphs
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	return sdmxResult, definitions, nil
}

// evaluateV2MetadataVariables returns each requested variable's ranked sources and coverage, in request order.
func evaluateV2MetadataVariables(queryReq *V2GetObservationsRequest, fullResult *sdmxpb.SdmxDataResult) []*v2MetadataVariable {
	byVariable := partitionSeriesByVariable(fullResult, queryReq.VariableDcids)
	variables := make([]*v2MetadataVariable, 0, len(queryReq.VariableDcids))
	for _, varDcid := range queryReq.VariableDcids {
		variables = append(variables, evaluateV2MetadataVariable(varDcid, byVariable[varDcid], fullResult, queryReq))
	}
	return variables
}

// evaluateV2MetadataVariable ranks a variable's sources as V2GetObservations does for the latest date, so the
// first is the one it uses by default, and summarizes each source's coverage over all dates.
func evaluateV2MetadataVariable(
	varDcid string,
	varSeries []*sdmxpb.SdmxTimeSeries,
	fullResult *sdmxpb.SdmxDataResult,
	queryReq *V2GetObservationsRequest,
) *v2MetadataVariable {
	dimSlots := extractV2DimensionSlots(&sdmxpb.SdmxDataResult{Series: varSeries}, fullResult, queryReq)
	rankedFacets, facetsByID := rankV2VariableFacets(varSeries, dimSlots, &dateFilter{dateType: dateTypeLatest})
	return &v2MetadataVariable{
		dcid:         varDcid,
		rankedFacets: rankedFacets,
		facetsByID:   facetsByID,
		coverage:     summarizeV2FacetCoverage(varSeries, queryReq.EntityProperty),
	}
}

// summarizeV2FacetCoverage returns, per source (facet ID), the earliest and latest observation dates, the number
// of observations, and the entityProperty values across the variable's series from that source. Series without
// observations are skipped, matching how V2GetObservations counts entities.
func summarizeV2FacetCoverage(varSeries []*sdmxpb.SdmxTimeSeries, entityProperty string) map[string]*v2FacetCoverage {
	seriesByFacet := make(map[string][]*sdmxpb.SdmxTimeSeries)
	for _, series := range varSeries {
		if len(series.GetPoints()) == 0 {
			continue
		}
		facetID := v2SeriesFacetID(series)
		seriesByFacet[facetID] = append(seriesByFacet[facetID], series)
	}
	coverage := make(map[string]*v2FacetCoverage, len(seriesByFacet))
	for facetID, facetSeries := range seriesByFacet {
		cov := &v2FacetCoverage{entities: newDcidSet()}
		cov.earliestDate, cov.latestDate = seriesDateBounds(facetSeries)
		for _, series := range facetSeries {
			cov.observationCount += len(series.GetPoints())
			if entity := cleanV2FacetComponent(series.GetDimensions()[entityProperty]); entity != "" {
				cov.entities.add(entity)
			}
		}
		coverage[facetID] = cov
	}
	return coverage
}

// collectV2ProvenanceDcids returns the provenance DCIDs of every ranked source, in first-appearance order
// (variables in request order, sources in rank order).
func collectV2ProvenanceDcids(variables []*v2MetadataVariable) []string {
	provenances := newDcidSet()
	for _, mv := range variables {
		for _, st := range mv.rankedFacets {
			if raw := mv.facetsByID[st.facetID]; raw != nil && raw.provenanceDcid != "" {
				provenances.add(raw.provenanceDcid)
			}
		}
	}
	return provenances.dcids
}

// fetchV2DatasetMetadata fetches the outgoing properties of the provenances and of their parent sources, and
// returns each provenance's name and URL (for source rows) and the datasets that have properties: v1's dataset
// properties, with provenance values overriding source values (see translateProvenance). Failures are logged:
// without the provenance lookup there are no names, so rows fall back to provenance DCIDs, and no datasets;
// without the source lookup, datasets have provenance properties only.
func (s *Service) fetchV2DatasetMetadata(
	ctx context.Context,
	provenanceDcids []string,
) (map[string]*v2ProvenanceMeta, []*V2DatasetMetadata) {
	provByDcid := make(map[string]*v2ProvenanceMeta, len(provenanceDcids))
	datasets := []*V2DatasetMetadata{}
	if len(provenanceDcids) == 0 {
		return provByDcid, datasets
	}
	provGraphs, err := s.fetchNodeGraphs(ctx, provenanceDcids, wildcardPropertyQuery)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("V2GetVariableMetadata provenance lookup failed", "error", err, "provenancesCount", len(provenanceDcids))
		}
		return provByDcid, datasets
	}
	sourceGraphs := s.fetchV2SourceGraphs(ctx, provenanceDcids, provGraphs)
	for _, provDcid := range provenanceDcids {
		provGraph := provGraphs[provDcid]
		provByDcid[provDcid] = newV2ProvenanceMeta(provGraph)
		dataset := &rawDatasetData{provenance: provGraph, source: sourceGraphs[extractSourceDcid(provGraph)]}
		if properties := translateProvenance(provDcid, dataset).Properties; len(properties) > 0 {
			datasets = append(datasets, &V2DatasetMetadata{ImportName: v2ImportName(provDcid, provByDcid), Properties: properties})
		}
	}
	return provByDcid, datasets
}

// fetchV2SourceGraphs fetches the outgoing properties of the provenances' parent sources, keyed by source DCID.
// A failed lookup is logged and returns nil.
func (s *Service) fetchV2SourceGraphs(
	ctx context.Context,
	provenanceDcids []string,
	provGraphs map[string]*pbv2.LinkedGraph,
) map[string]*pbv2.LinkedGraph {
	sources := newDcidSet()
	for _, provDcid := range provenanceDcids {
		if source := extractSourceDcid(provGraphs[provDcid]); source != "" {
			sources.add(source)
		}
	}
	if len(sources.dcids) == 0 {
		return nil
	}
	graphs, err := s.fetchNodeGraphs(ctx, sources.dcids, wildcardPropertyQuery)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("V2GetVariableMetadata source lookup failed", "error", err, "sourcesCount", len(sources.dcids))
		}
		return nil
	}
	return graphs
}

// buildV2GetVariableMetadataResponse assembles one entry per variable in request order, with its definition
// (when the property lookup succeeded), its sources table, and a note when it has no sources. Notes use the
// V2GetObservations scope of the original request, so child mode names the parent and child type.
func buildV2GetVariableMetadataResponse(
	req *V2GetObservationsRequest,
	variables []*v2MetadataVariable,
	definitions map[string]*pbv2.LinkedGraph,
	provByDcid map[string]*v2ProvenanceMeta,
	datasets []*V2DatasetMetadata,
) *V2GetVariableMetadataResponse {
	scope := v2ObservationNoteScope(req)
	entries := make([]*V2VariableMetadata, 0, len(variables))
	for _, mv := range variables {
		definition := translateProperties(definitions[mv.dcid])
		entry := &V2VariableMetadata{
			Dcid:        mv.dcid,
			Name:        definition.name,
			Description: definition.description,
			Properties:  definition.properties,
			Sources:     buildV2MetadataSourcesTable(mv, provByDcid, req),
		}
		if len(entry.Sources.Rows) == 0 {
			entry.Note = v2NoObsNote(mv.dcid, scope)
		}
		entries = append(entries, entry)
	}
	return &V2GetVariableMetadataResponse{Variables: entries, Datasets: datasets}
}

// buildV2MetadataSourcesTable returns a variable's sources table: one row per source in rank order, with the
// source_metadata fields V2GetObservations returns (see buildV2FacetMeta) followed by the source's coverage.
func buildV2MetadataSourcesTable(
	mv *v2MetadataVariable,
	provByDcid map[string]*v2ProvenanceMeta,
	req *V2GetObservationsRequest,
) *Table {
	table := &Table{Columns: slices.Clone(v2MetadataSourceColumns)}
	for _, st := range mv.rankedFacets {
		meta := buildV2FacetMeta(mv.facetsByID[st.facetID], provByDcid)
		cov := mv.coverage[st.facetID]
		table.Rows = append(table.Rows, []any{
			meta.SourceID, meta.ImportName, meta.ProvenanceURL, meta.Unit, meta.ScalingFactor,
			meta.MeasurementMethod, meta.ObservationPeriod, cov.earliestDate, cov.latestDate,
			v2MetadataObservationCount(cov, req), v2MetadataEntitiesWithData(cov, req),
		})
	}
	return table
}

// v2MetadataObservationCount returns observation_count for a source, or nil (an empty cell) in child mode, where
// only an internal sample of the parent's children is counted.
func v2MetadataObservationCount(cov *v2FacetCoverage, req *V2GetObservationsRequest) any {
	if req.ChildEntityTypeDcid != "" {
		return nil
	}
	return cov.observationCount
}

// v2MetadataEntitiesWithData returns entities_with_data for a source: the sorted entity_property values it has
// data for, or in child mode the parent, since the entities checked are an internal sample of its children.
func v2MetadataEntitiesWithData(cov *v2FacetCoverage, req *V2GetObservationsRequest) []string {
	if req.ChildEntityTypeDcid != "" {
		return []string{req.EntityDcids[0]}
	}
	return util.NonNilStringSlice(slices.Sorted(slices.Values(cov.entities.dcids)))
}
