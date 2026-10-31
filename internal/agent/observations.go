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

// Shared SDMX constraint, facet ranking, and table formatting helpers for the v1 and v2 observation tools.

package agent

import (
	"context"
	"sort"
	"strconv"
	"time"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"github.com/datacommonsorg/mixer/internal/util"
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

// facetStats tracks coverage and date metrics used to rank and select the primary SDMX facet.
type facetStats struct {
	facetID          string
	placesFoundCount int
	dateCount        int
	latestDate       time.Time
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
