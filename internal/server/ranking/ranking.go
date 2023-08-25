// Copyright 2022 Google LLC
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

// Source ranking is based on the following criteria in order:
// 1. More trusted source ranks higher
// 2. Latest data ranks higher
// 3. More data ranks higher

package ranking

import (
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
)

// RankKey represents keys used for ranking.
// Can use a nil pointer for wildcard match for any field that is a *string type.
type RankKey struct {
	MM   *string // MeasurementMethod
	OP   *string // ObservationPeriod
	Unit *string
}

// s returns a pointer to the string that is passed to it.
func s(str string) *string {
	return &str
}

// StatsRanking is used to rank multiple source series for the same
// StatisticalVariable, where lower value means higher ranking.
// Outer key is Import Name, inner key is Rank Key, value is score.
// When making changes to ranking, please also make sure to update golden files.
var StatsRanking = map[string]map[RankKey]int{
	// Population
	"USCensusPEP_Annual_Population": {
		{MM: s("CensusPEPSurvey"), OP: s("P1Y")}: 0,
	},

	// Population
	"CensusACS5YearSurvey": {
		{MM: s("CensusACS5yrSurvey")}: 1,
	},

	// Population
	"CensusACS5YearSurvey_AggCountry": {
		{MM: s("CensusACS5yrSurvey")}: 1,
	},

	// Population
	"CensusUSAMedianAgeIncome": {{MM: s("CensusACS5yrSurvey")}: 1},

	// Population
	"USDecennialCensus_RedistrictingRelease": {{MM: s("USDecennialCensus")}: 2},

	"EurostatData": {
		// Population
		{MM: s("EurostatRegionalPopulationData")}: 3,
		// Unemployment Rate
		{MM: s("")}: 2,
	},

	// Population
	"WorldDevelopmentIndicators": {{}: 4},

	// Prefer Indian Census population for Indian states, over something like
	// OECD.
	// Population
	"IndiaCensus_Primary": {{}: 5},

	// Population
	"WikipediaStatsData": {{MM: s("Wikipedia")}: 1001},

	// Population
	"HumanCuratedStats": {{MM: s("HumanCuratedStats")}: 1002},

	// Population
	"WikidataPopulation": {{MM: s("WikidataPopulation")}: 1003},

	// Unemployment Rate
	"BLS_LAUS": {{MM: s("BLSSeasonallyUnadjusted")}: 0},

	// Labor Force data ranked higher than WDI (above)}, or Eurostat
	"BLS_CPS": {{MM: s("BLSSeasonallyAdjusted")}: 1},

	// Covid
	"NYT_COVID19": {{MM: s("NYT_COVID19_GitHub")}: 0},

	// CDC500
	"CDC500": {{MM: s("AgeAdjustedPrevalence")}: 0},

	// Electricity
	"UNEnergy": {{MM: s("")}: 0},

	// Electricity
	"EIA_Electricity": {{}: 1},

	// Prefer observational weather over gridded over projections
	// Observational
	"NOAA_EPA_Observed_Historical_Weather": {{}: 0},

	// Gridded reanalysis
	"Copernicus_ECMWF_ERA5_Monthly": {{}: 1},

	// IPCC Projections
	"NASA_NEXDCP30": {{MM: s("NASA_Mean_CCSM4"), OP: s("P1M")}: 2},

	// IPCC Projections
	"NASA_NEXDCP30_AggrDiffStats": {{OP: s("P1M")}: 3},

	// TODO: Remove this once disppears from backend (replaced by
	// NASA_NEXDCP30_AggrDiffStats).
	// IPCC Projections
	"NASA_NEXDCP30_StatVarSeriesAggr": {{OP: s("P1M")}: 4},

	// Wet bulb year aggregation
	"NASA_WetBulbComputation_Aggregation": {
		{MM: s("NASA_Mean_HadGEM2-AO")}: 0,
		{}:                              1,
	},

	// Wet bulb
	"NASA_WetBulbComputation": {{MM: s("NASA_Mean_HadGEM2-AO")}: 2},

	"NASA_NEXGDDP_CMIP6_Subnational_AggrDiffStats_LongRangeProjections": {
		{MM: s("NASA_Mean_CMIP6_GFDL-ESM4")}: 0,
		{MM: s("NASA_Mean_CMIP6_GFDL-CM4")}:  1,
	},

	// Prefer FBI Hate Crime Publications over Data Commons Aggregate
	// Note: https://autopush.datacommons.org/tools/timeline#statsVar=Count_CriminalIncidents_IsHateCrime&place=country%2FUSA
	// Expected data 2010-2020: 6628, 6222, 5796, 5928, 5479, 5850, 6121, 7175, 7120, 7314, 8263
	// Note: https://autopush.datacommons.org/tools/timeline#place=geoId%2F06&statsVar=Count_CriminalIncidents_IsHateCrime
	// Expected data 2004-2010: 1393, 1379, 1297, 1400, 1381, 1015, 1092
	// FBI Hate Crime Publications
	"FBIHateCrimePublications": {{}: 0},

	// FBI Hate Crime Aggregations
	"FBIHateCrime": {{}: 1},

	// Prefer USDollar over Risk Score for Expected Annual Loss in FEMA National Risk Index (NRI)
	"USFEMA_NationalRiskIndex": {
		{Unit: s("USDollar")}:              0,
		{Unit: s("FemaNationalRiskScore")}: 1,
	},

	// Disaster
	"EarthquakeUSGS_Agg": {{OP: s("P1Y")}: 0},
}

// BaseRank is the base ranking score for sources. If a source is prefered, it
// should be given a score lower than BaseRank in StatsRanking. If a source is not
// prefered, it should be given a score higher than BaseRank in StatsRanking
const BaseRank = 100

// CohortByRank implements sort.Interface for []*SourceSeries based on
// the rank score. Each source series data is keyed by the place dcid.
//
// Note this has the same data type as SeriesByRank but is used to compare
// cohort instead of time series.
type CohortByRank []*pb.SourceSeries

// GetScoreRk derives the ranking score for a source series.
//
// The score depends on ImportName and other SVObs properties, by checking the
// StatsRanking dict. To get the score, ImportName is required, and a RankKey
// with these optional fields:
// - MM: MeasurementMethod
// - OP: ObservationPeriod
//
// When there are exact match of the properties in StatsRanking, then use that
// score, otherwise can also match to wildcard options (indicated by a nil
// pointer).
//
// If no entry is found, a BaseRank is assigned to the source series.
func GetScoreRk(importName string, rk RankKey) int {
	importNameStatsRanking, ok := StatsRanking[importName]
	if !ok {
		return BaseRank
	}
	rankScore := BaseRank
	mostMatches := -1
	for k, score := range importNameStatsRanking {
		matches := 0
		isMatch := true
		if k.MM != nil && rk.MM != nil {
			if *k.MM == *rk.MM {
				matches++
			} else {
				isMatch = false
			}
		}

		if k.OP != nil && rk.OP != nil {
			if *k.OP == *rk.OP {
				matches++
			} else {
				isMatch = false
			}
		}

		if k.Unit != nil && rk.Unit != nil {
			if *k.Unit == *rk.Unit {
				matches++
			} else {
				isMatch = false
			}
		}

		if !isMatch {
			continue
		}
		if matches < mostMatches {
			continue
		}
		if matches == mostMatches && score > rankScore {
			continue
		}

		rankScore = score
		mostMatches = matches
	}

	return rankScore
}

// GetScorePb is a GetScoreRk adapter for pb.SourceSeries
func GetScorePb(ss *pb.SourceSeries) int {
	rk := RankKey{
		MM:   s(ss.MeasurementMethod),
		OP:   s(ss.ObservationPeriod),
		Unit: s(ss.Unit),
	}
	return GetScoreRk(ss.ImportName, rk)
}

// GetFacetScore is a GetScoreRk adapter for pb.Facet
func GetFacetScore(m *pb.Facet) int {
	rk := RankKey{
		MM:   s(m.MeasurementMethod),
		OP:   s(m.ObservationPeriod),
		Unit: s(m.Unit),
	}
	return GetScoreRk(m.ImportName, rk)
}

func (a CohortByRank) Len() int {
	return len(a)
}

func (a CohortByRank) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a CohortByRank) Less(i, j int) bool {
	oi := a[i]
	scorei := GetScorePb(oi)
	oj := a[j]
	scorej := GetScorePb(oj)
	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}

	// Compare other fields to get consistent ranking.
	if oi.MeasurementMethod != oj.MeasurementMethod {
		return oi.MeasurementMethod < oj.MeasurementMethod
	}
	if oi.ObservationPeriod != oj.ObservationPeriod {
		return oi.ObservationPeriod < oj.ObservationPeriod
	}
	if oi.ScalingFactor != oj.ScalingFactor {
		return oi.ScalingFactor < oj.ScalingFactor
	}
	if oi.Unit != oj.Unit {
		return oi.Unit < oj.Unit
	}
	if oi.ProvenanceUrl != oj.ProvenanceUrl {
		return oi.ProvenanceUrl < oj.ProvenanceUrl
	}
	return true
}

// SeriesByRank implements sort.Interface for []*SourceSeries based on
// the rank score. Each source series data is keyed by the observation date.
//
// This is the protobuf version of byRank.
type SeriesByRank []*pb.SourceSeries

func (a SeriesByRank) Len() int { return len(a) }

func (a SeriesByRank) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a SeriesByRank) Less(i, j int) bool {
	oi := a[i]
	scorei := GetScorePb(oi)
	oj := a[j]
	scorej := GetScorePb(oj)

	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}

	latesti := ""
	for date := range a[i].Val {
		if date > latesti {
			latesti = date
		}
	}

	latestj := ""
	for date := range a[j].Val {
		if date > latestj {
			latestj = date
		}
	}

	// Series with latest data is ranked higher
	if latesti != latestj {
		return latesti > latestj
	}

	// Series with more data is ranked higher
	if len(a[i].Val) != len(a[j].Val) {
		return len(a[i].Val) > len(a[j].Val)
	}

	// Compare other fields to get consistent ranking.
	if oi.ImportName != oj.ImportName {
		return oi.ImportName < oj.ImportName
	}
	if oi.MeasurementMethod != oj.MeasurementMethod {
		return oi.MeasurementMethod < oj.MeasurementMethod
	}
	if oi.ObservationPeriod != oj.ObservationPeriod {
		return oi.ObservationPeriod < oj.ObservationPeriod
	}
	if oi.ScalingFactor != oj.ScalingFactor {
		return oi.ScalingFactor < oj.ScalingFactor
	}
	if oi.Unit != oj.Unit {
		return oi.Unit < oj.Unit
	}
	if oi.ProvenanceUrl != oj.ProvenanceUrl {
		return oi.ProvenanceUrl < oj.ProvenanceUrl
	}
	return true
}

// GetScore is a GetScoreRk adapter for model.SourceSeries
// TODO(shifucun): Remove `SourceSeries` and use pb.SourceSeries everywhere.
func GetScore(ss *model.SourceSeries) int {
	rk := RankKey{
		MM:   s(ss.MeasurementMethod),
		OP:   s(ss.ObservationPeriod),
		Unit: s(ss.Unit),
	}
	return GetScoreRk(ss.ImportName, rk)
}

// ByRank implements sort.Interface for []*SourceSeries based on
// the rank score.
type ByRank []*model.SourceSeries

func (a ByRank) Len() int { return len(a) }

func (a ByRank) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a ByRank) Less(i, j int) bool {
	oi := a[i]
	scorei := GetScore(oi)
	oj := a[j]
	scorej := GetScore(oj)
	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}

	latesti := ""
	for date := range a[i].Val {
		if date > latesti {
			latesti = date
		}
	}

	latestj := ""
	for date := range a[j].Val {
		if date > latestj {
			latestj = date
		}
	}

	// Series with latest data is ranked higher
	if latesti != latestj {
		return latesti > latestj
	}

	// Series with more data is ranked higher
	if len(a[i].Val) != len(a[j].Val) {
		return len(a[i].Val) > len(a[j].Val)
	}

	// Compare other fields to get consistent ranking.
	if oi.MeasurementMethod != oj.MeasurementMethod {
		return oi.MeasurementMethod < oj.MeasurementMethod
	}
	if oi.ObservationPeriod != oj.ObservationPeriod {
		return oi.ObservationPeriod < oj.ObservationPeriod
	}
	if oi.ScalingFactor != oj.ScalingFactor {
		return oi.ScalingFactor < oj.ScalingFactor
	}
	if oi.Unit != oj.Unit {
		return oi.Unit < oj.Unit
	}
	if oi.ProvenanceURL != oj.ProvenanceURL {
		return oi.ProvenanceURL < oj.ProvenanceURL
	}
	return true
}

// FacetByRank implements sort.Interface for []*Facet based on
// the rank score.
type FacetByRank []*pb.PlaceVariableFacet

func (a FacetByRank) Len() int {
	return len(a)
}

func (a FacetByRank) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a FacetByRank) Less(i, j int) bool {
	oi := a[i]
	scorei := GetFacetScore(oi.Facet)
	oj := a[j]
	scorej := GetFacetScore(oj.Facet)
	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}

	if oi.LatestDate != oj.LatestDate {
		return oi.LatestDate > oj.LatestDate
	}

	if oi.ObsCount != oj.ObsCount {
		return oi.ObsCount > oj.ObsCount
	}

	// Compare other fields to get consistent ranking.
	if oi.Facet.MeasurementMethod != oj.Facet.MeasurementMethod {
		return oi.Facet.MeasurementMethod < oj.Facet.MeasurementMethod
	}
	if oi.Facet.ObservationPeriod != oj.Facet.ObservationPeriod {
		return oi.Facet.ObservationPeriod < oj.Facet.ObservationPeriod
	}
	if oi.Facet.ScalingFactor != oj.Facet.ScalingFactor {
		return oi.Facet.ScalingFactor < oj.Facet.ScalingFactor
	}
	if oi.Facet.Unit != oj.Facet.Unit {
		return oi.Facet.Unit < oj.Facet.Unit
	}
	if oi.Facet.ProvenanceUrl != oj.Facet.ProvenanceUrl {
		return oi.Facet.ProvenanceUrl < oj.Facet.ProvenanceUrl
	}
	return true
}
