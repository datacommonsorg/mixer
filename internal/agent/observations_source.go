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
	"sort"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

type sourceProcessingResult struct {
	primarySourceID         string
	alternativeSourceCounts map[string]int
	processedDataByPlace    map[string]*processedPlaceData
}

type processedPlaceData struct {
	facetID      string
	observations []*pb.PointStat
}

// sourceStats holds the aggregation metrics for ranking a single source facet.
type sourceStats struct {
	sourceID         string
	placesFoundCount int
	dateCount        int
	latestDate       time.Time
	avgIndex         float64
}

// selectPrimarySource analyzes variable observation facets and picks the primary source and counts alternatives.
func selectPrimarySource(
	variableData *pbv2.VariableObservation,
	sourceOverride string,
	filter *dateFilter,
) *sourceProcessingResult {
	if variableData == nil || len(variableData.GetByEntity()) == 0 {
		return &sourceProcessingResult{
			alternativeSourceCounts: make(map[string]int),
			processedDataByPlace:    make(map[string]*processedPlaceData),
		}
	}

	// Handle override mode
	if sourceOverride != "" {
		processed := make(map[string]*processedPlaceData)
		for entityDcid, entityObs := range variableData.GetByEntity() {
			for _, facetObs := range entityObs.GetOrderedFacets() {
				if facetObs.GetFacetId() == sourceOverride {
					filtered := filterObservationsByDate(facetObs.GetObservations(), filter)
					if len(filtered) > 0 {
						processed[entityDcid] = &processedPlaceData{
							facetID:      sourceOverride,
							observations: filtered,
						}
					}
					break
				}
			}
		}
		return &sourceProcessingResult{
			primarySourceID:         sourceOverride,
			alternativeSourceCounts: make(map[string]int),
			processedDataByPlace:    processed,
		}
	}

	// First pass: gather statistics for all available sources to rank them.
	placesFound := make(map[string]int)
	dateCounts := make(map[string]int)
	latestDates := make(map[string]time.Time)
	sourceIndices := make(map[string][]int)

	for _, entityObs := range variableData.GetByEntity() {
		for i, facetObs := range entityObs.GetOrderedFacets() {
			sourceID := facetObs.GetFacetId()
			filtered := filterObservationsByDate(facetObs.GetObservations(), filter)
			if len(filtered) > 0 {
				placesFound[sourceID]++
				dateCounts[sourceID] += len(filtered)

				// Calculate latest date
				var maxDate time.Time
				for _, obs := range filtered {
					t, _, err := parseDateStringToInterval(obs.GetDate())
					if err == nil && t.After(maxDate) {
						maxDate = t
					}
				}
				if maxDate.After(latestDates[sourceID]) {
					latestDates[sourceID] = maxDate
				}

				// Track original indices for average index calculation
				sourceIndices[sourceID] = append(sourceIndices[sourceID], i)
			}
		}
	}

	if len(placesFound) == 0 {
		return &sourceProcessingResult{
			alternativeSourceCounts: make(map[string]int),
			processedDataByPlace:    make(map[string]*processedPlaceData),
		}
	}

	// Build stats slice for sorting
	var statsList []sourceStats
	for sourceID, count := range placesFound {
		var sumIdx int
		for _, idx := range sourceIndices[sourceID] {
			sumIdx += idx
		}
		avgIdx := float64(sumIdx) / float64(len(sourceIndices[sourceID]))

		statsList = append(statsList, sourceStats{
			sourceID:         sourceID,
			placesFoundCount: count,
			dateCount:        dateCounts[sourceID],
			latestDate:       latestDates[sourceID],
			avgIndex:         avgIdx,
		})
	}

	// Sort stats according to 4 heuristics:
	// 1. Most places found (higher is better)
	// 2. Most observation points (higher is better)
	// 3. Most recent data (latest date, later is better)
	// 4. Original preference list position (average index, lower is better)
	// 5. Final tie-breaker: string comparison of source ID
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
		if si.avgIndex != sj.avgIndex {
			return si.avgIndex < sj.avgIndex
		}
		return si.sourceID < sj.sourceID
	})

	primary := statsList[0].sourceID

	altCounts := make(map[string]int)
	for _, stats := range statsList {
		if stats.sourceID != primary {
			altCounts[stats.sourceID] = stats.placesFoundCount
		}
	}

	// Second pass: build processed data using only the primary source
	processed := make(map[string]*processedPlaceData)
	for entityDcid, entityObs := range variableData.GetByEntity() {
		for _, facetObs := range entityObs.GetOrderedFacets() {
			if facetObs.GetFacetId() == primary {
				filtered := filterObservationsByDate(facetObs.GetObservations(), filter)
				if len(filtered) > 0 {
					processed[entityDcid] = &processedPlaceData{
						facetID:      primary,
						observations: filtered,
					}
				}
				break
			}
		}
	}

	return &sourceProcessingResult{
		primarySourceID:         primary,
		alternativeSourceCounts: altCounts,
		processedDataByPlace:    processed,
	}
}

// filterObservationsByDate filters a slice of observations by date filter.
func filterObservationsByDate(
	obs []*pb.PointStat,
	filter *dateFilter,
) []*pb.PointStat {
	var filtered []*pb.PointStat
	for _, o := range obs {
		if isDateInInterval(o.GetDate(), filter) {
			filtered = append(filtered, o)
		}
	}

	// If filter type is "latest" and we have observations, only return the latest one.
	if filter != nil && filter.dateType == dateTypeLatest && len(filtered) > 0 {
		var latest *pb.PointStat
		for _, o := range filtered {
			if latest == nil || o.GetDate() > latest.GetDate() {
				latest = o
			}
		}
		return []*pb.PointStat{latest}
	}

	return filtered
}
