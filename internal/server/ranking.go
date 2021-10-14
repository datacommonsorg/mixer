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

package server

import (
	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// RankKey represents keys used for ranking.
type RankKey struct {
	Prov    string
	Mmethod string
}

// StatsRanking is used to rank multiple source series for the same
// StatisticalVariable, where lower value means higher ranking.
var StatsRanking = map[RankKey]int{
	{"CensusPEP", "CensusPEPSurvey"}:                                      0, // Population
	{"CensusACS5YearSurvey", "CensusACS5yrSurvey"}:                        1, // Population
	{"CensusACS5YearSurvey_AggCountry", "dcAggregate/CensusACS5yrSurvey"}: 1, // Population
	{"CensusUSAMedianAgeIncome", "CensusACS5yrSurvey"}:                    1, // Population
	{"EurostatData", "EurostatRegionalPopulationData"}:                    2, // Population
	{"WorldDevelopmentIndicators", ""}:                                    3, // Population
	{"BLS_LAUS", "BLSSeasonallyUnadjusted"}:                               0, // Unemployment Rate
	{"EurostatData", ""}:                                                  1, // Unemployment Rate
	{"NYT_COVID19", "NYT_COVID19_GitHub"}:                                 0, // Covid
	{"CDC500", "AgeAdjustedPrevalence"}:                                   0, // CDC500
	{"UNEnergy", ""}:                                                      0, // Electricity
	{"EIA_Electricity", ""}:                                               1, // Electricity
	{"WikipediaStatsData", "Wikipedia"}:                                   1001,
	{"HumanCuratedStats", "HumanCuratedStats"}:                            1002,
	{"WikidataPopulation", "WikidataPopulation"}:                          1003,
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

func (a CohortByRank) Len() int { return len(a) }

func (a CohortByRank) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a CohortByRank) Less(i, j int) bool {
	oi := a[i]
	keyi := RankKey{Prov: oi.ImportName, Mmethod: oi.MeasurementMethod}
	scorei, ok := StatsRanking[keyi]
	if !ok {
		scorei = BaseRank
	}
	oj := a[j]
	keyj := RankKey{Prov: oj.ImportName, Mmethod: oj.MeasurementMethod}
	scorej, ok := StatsRanking[keyj]
	if !ok {
		scorej = BaseRank
	}
	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}

	// Cohort with more place coverage is ranked higher
	if len(a[i].Val) != len(a[j].Val) {
		return len(a[i].Val) > len(a[j].Val)
	}

	// Compare other fields to get consistent ranking.
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
	keyi := RankKey{Prov: oi.ImportName, Mmethod: oi.MeasurementMethod}
	scorei, ok := StatsRanking[keyi]
	if !ok {
		scorei = BaseRank
	}
	oj := a[j]
	keyj := RankKey{Prov: oj.ImportName, Mmethod: oj.MeasurementMethod}
	scorej, ok := StatsRanking[keyj]
	if !ok {
		scorej = BaseRank
	}
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

// byRank implements sort.Interface for []*SourceSeries based on
// the rank score.
type byRank []*SourceSeries

func (a byRank) Len() int { return len(a) }

func (a byRank) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a byRank) Less(i, j int) bool {
	oi := a[i]
	keyi := RankKey{oi.ImportName, oi.MeasurementMethod}
	scorei, ok := StatsRanking[keyi]
	if !ok {
		scorei = BaseRank
	}
	oj := a[j]
	keyj := RankKey{oj.ImportName, oj.MeasurementMethod}
	scorej, ok := StatsRanking[keyj]
	if !ok {
		scorej = BaseRank
	}
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
