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

package server

import (
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// RankKey represents keys used for ranking.
type RankKey struct {
	Prov    string
	Mmethod string
}

// StatsRanking is used to rank multiple source series for the same
// StatisticalVariable, where lower value means higher ranking.
// The ranking score ranges from 0 to 100.
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
}

// LowestRank is the lowest ranking score.
const LowestRank = 100

// SeriesByRank implements sort.Interface for []*SourceSeries based on
// the rank score.
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
		scorei = LowestRank
	}
	oj := a[j]
	keyj := RankKey{Prov: oj.ImportName, Mmethod: oj.MeasurementMethod}
	scorej, ok := StatsRanking[keyj]
	if !ok {
		scorej = LowestRank
	}
	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}

	// Rank higher series with latest data
	datesi := []string{}
	for date := range a[i].Val {
		datesi = append(datesi, date)
	}
	sort.Strings(datesi)
	latesti := datesi[len(datesi)-1]

	datesj := []string{}
	for date := range a[j].Val {
		datesj = append(datesj, date)
	}
	sort.Strings(datesj)
	latestj := datesj[len(datesj)-1]

	if latesti != latestj {
		return latesti > latestj
	}

	if len(datesi) != len(datesj) {
		return len(datesi) > len(datesj)
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
		scorei = LowestRank
	}
	oj := a[j]
	keyj := RankKey{oj.ImportName, oj.MeasurementMethod}
	scorej, ok := StatsRanking[keyj]
	if !ok {
		scorej = LowestRank
	}
	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}

	// Rank higher series with latest data
	datesi := []string{}
	for date := range a[i].Val {
		datesi = append(datesi, date)
	}
	sort.Strings(datesi)
	latesti := datesi[len(datesi)-1]

	datesj := []string{}
	for date := range a[j].Val {
		datesj = append(datesj, date)
	}
	sort.Strings(datesj)
	latestj := datesj[len(datesj)-1]

	if latesti != latestj {
		return latesti > latestj
	}

	if len(datesi) != len(datesj) {
		return len(datesi) > len(datesj)
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
