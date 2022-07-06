// Copyright 2020 Google LLC
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
// Import Name should be non-empty.
// Can use "*" for wildcard match for MeasurementMethod and ObservationPeriod
type RankKey struct {
    MM *string
    OP *string
}

// S returns a pointer to the string that is passed to it
func s(str string) *string {
	return &str
}

// StatsRanking is used to rank multiple source series for the same
// StatisticalVariable, where lower value means higher ranking.
// Outer key is Import Name, inner key is Rank Key, value is score
var StatsRanking = map[string]map[RankKey]int{
	"CensusPEP": {{MM: s("CensusPEPSurvey")}:                                0,}, // Population
	"CensusACS5YearSurvey": {{MM: s("CensusACS5yrSurvey")}:                  1,}, // Population
	"CensusACS5YearSurvey_AggCountry": {{MM: s("CensusACS5yrSurvey")}:       1,}, // Population
	"CensusUSAMedianAgeIncome": {{MM: s("CensusACS5yrSurvey")}:              1,}, // Population
	"USDecennialCensus_RedistrictingRelease": {{MM: s("USDecennialCensus")}: 2,}, // Population
    "EurostatData": {
		{MM: s("EurostatRegionalPopulationData")}:                        3, // Population
		{MM: s("")}:                                                      2,// Unemployment Rate
    },
    "WorldDevelopmentIndicators": {{}:                             4,}, // Population
	// Prefer Indian Census population for Indian states, over something like OECD.
    "IndiaCensus_Primary":{{}:                 5,},    // Population
	"WikipediaStatsData": {{MM: s("Wikipedia")}:          1001,}, // Population
	"HumanCuratedStats": {{MM: s("HumanCuratedStats")}:   1002,}, // Population
	"WikidataPopulation": {{MM: s("WikidataPopulation")}: 1003,}, // Population

	"BLS_LAUS": {{MM: s("BLSSeasonallyUnadjusted")}: 0, },// Unemployment Rate
	"BLS_CPS": {{MM: s("BLSSeasonallyAdjusted")}:    1, },// Labor Force data ranked higher than WDI (above)}, or Eurostat

	"NYT_COVID19": {{MM: s("NYT_COVID19_GitHub")}: 0,}, // Covid

	"CDC500": {{MM: s("AgeAdjustedPrevalence")}: 0,}, // CDC500

	"UNEnergy": {{MM: s("")}:         0,}, // Electricity
	"EIA_Electricity": {{}: 1,}, // Electricity

	// Prefer observational weather over gridded over projections
    "NOAA_EPA_Observed_Historical_Weather": {{}: 0,}, // Observational
    "Copernicus_ECMWF_ERA5_Monthly": {{}:        1, }, // Gridded reanalysis
	"NASA_NEXDCP30": {{MM: s("NASA_Mean_CCSM4"), OP: s("P1M")}:        2, }, // IPCC Projections
	"NASA_NEXDCP30_AggrDiffStats": {{OP: s("P1M")}:        3, }, // IPCC Projections
	// TODO: Remove this once disppears from backend (replaced by NASA_NEXDCP30_AggrDiffStats).
	"NASA_NEXDCP30_StatVarSeriesAggr": {{OP: s("P1M")}: 4,}, // IPCC Projections

    "NASA_WetBulbComputation_Aggregation": { // Wet bulb year aggregation
		{MM: s("NASA_Mean_HadGEM2-AO")}: 0,
        {}:                    1,
    },
	"NASA_WetBulbComputation": {{MM: s("NASA_Mean_HadGEM2-AO")}:             2,}, // Wet bulb

	// Prefer FBI Hate Crime Publications over Data Commons Aggregate
	// Note: https://autopush.datacommons.org/tools/timeline#statsVar=Count_CriminalIncidents_IsHateCrime&place=country%2FUSA
	// Expected data 2010-2020: 6628, 6222, 5796, 5928, 5479, 5850, 6121, 7175, 7120, 7314, 8263
	// Note: https://autopush.datacommons.org/tools/timeline#place=geoId%2F06&statsVar=Count_CriminalIncidents_IsHateCrime
	// Expected data 2004-2010: 1393, 1379, 1297, 1400, 1381, 1015, 1092
    "FBIHateCrimePublications": {{}: 0,}, // FBI Hate Crime Publications
    "FBIHateCrime": {{}:             1,}, // FBI Hate Crime Aggregations
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
// StatsRanking dict. To get the score, ImportName is required, with optional
// properties:
// - MM: MeasurementMethod
// - OP: ObservationPeriod
//
// When there are exact match of the properties in StatsRanking, then use that
// score, otherwise can also match to wildcard options (indicated by *).
//
// If no entry is found, a BaseRank is assigned to the source series.
func GetScoreRk(importName string, rk RankKey) int {
    importNameStatsRanking, ok := StatsRanking[importName]
    if ! ok {
        return BaseRank
    }
    rankScore := BaseRank
    mostMatches := -1
    for k, score := range importNameStatsRanking {
        matches := 0
        isMatch := true
        if k.MM != nil {
            if *k.MM == *rk.MM {
                matches++
            } else {
                isMatch = false
            }
        }

        if k.OP != nil {
            if *k.OP == *rk.OP {
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
func GetScorePb(s *pb.SourceSeries) int {
    rk := RankKey{
        MM: &(s.MeasurementMethod),
        OP: &(s.ObservationPeriod),
    }
	return GetScoreRk(s.ImportName, rk)
}

// GetMetadataScore is a GetScoreRk adapter for pb.StatMetadata
func GetMetadataScore(m *pb.StatMetadata) int {
    rk := RankKey{
        MM: &(m.MeasurementMethod),
        OP: &(m.ObservationPeriod),
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
func GetScore(s *model.SourceSeries) int {
    rk := RankKey{
        MM: &(s.MeasurementMethod),
        OP: &(s.ObservationPeriod),
    }
    return GetScoreRk(s.ImportName, rk)
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
