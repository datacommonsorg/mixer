// Copyright 2021 Google LLC
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

package ranking

import (
	"sort"
	"strings"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetScorePb(t *testing.T) {
	for _, c := range []struct {
		series *pb.SourceSeries
		score  int
	}{
		{
			&pb.SourceSeries{ImportName: "CensusPEP", MeasurementMethod: "CensusPEPSurvey"},
			100,
		},
		{
			&pb.SourceSeries{ImportName: "USCensusPEP_Annual_Population", MeasurementMethod: "CensusPEPSurvey", ObservationPeriod: "P1Y"},
			0,
		},
		{
			&pb.SourceSeries{ImportName: "WorldDevelopmentIndicators", MeasurementMethod: "NewMM", ObservationPeriod: "P1Y"},
			4,
		},
		{
			&pb.SourceSeries{ImportName: "NASA_NEXDCP30", MeasurementMethod: "MM", ObservationPeriod: "P1Y"},
			100,
		},
		{
			&pb.SourceSeries{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_CCSM4", ObservationPeriod: "P1M"},
			2,
		},
		{ // test that an import name that does not exist returns the BaseRank
			&pb.SourceSeries{ImportName: "THIS_IMPORT_DOES_NOT_EXIST", MeasurementMethod: "DOES_NOT_EXIST", ObservationPeriod: "DOES_NOT_EXIST"},
			100,
		},
	} {
		score := GetScorePb(c.series)
		if diff := cmp.Diff(score, c.score); diff != "" {
			t.Errorf("getScorePb() got diff score %v", diff)
		}
	}
}

func TestSeriesByRank(t *testing.T) {
	for _, c := range []struct {
		series   []*pb.SourceSeries
		expected []*pb.SourceSeries
	}{
		{
			[]*pb.SourceSeries{
				{ImportName: "CensusACS5YearSurvey", MeasurementMethod: "CensusACS5yrSurvey"},
				{ImportName: "USCensusPEP_Annual_Population", MeasurementMethod: "CensusPEPSurvey", ObservationPeriod: "P1Y"},
			},
			[]*pb.SourceSeries{
				{ImportName: "USCensusPEP_Annual_Population", MeasurementMethod: "CensusPEPSurvey", ObservationPeriod: "P1Y"},
				{ImportName: "CensusACS5YearSurvey", MeasurementMethod: "CensusACS5yrSurvey"},
			},
		},
		{
			[]*pb.SourceSeries{
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_GISS-E2-R", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1Y"},
			},
			[]*pb.SourceSeries{
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_GISS-E2-R", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1Y"},
			},
		},
		{
			[]*pb.SourceSeries{
				{ImportName: "NASA_WetBulbComputation", MeasurementMethod: "NASA_Mean_CCSM4", ObservationPeriod: "P1Y"},
				{ImportName: "NASA_WetBulbComputation", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1Y"},
				{ImportName: "NASA_WetBulbComputation_Aggregation", MeasurementMethod: "NASA_Mean_ACCESS1-0", ObservationPeriod: "P78Y"},
				{ImportName: "NASA_WetBulbComputation_Aggregation", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P78Y"},
			},
			[]*pb.SourceSeries{
				{ImportName: "NASA_WetBulbComputation_Aggregation", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P78Y"},
				{ImportName: "NASA_WetBulbComputation_Aggregation", MeasurementMethod: "NASA_Mean_ACCESS1-0", ObservationPeriod: "P78Y"},
				{ImportName: "NASA_WetBulbComputation", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1Y"},
				{ImportName: "NASA_WetBulbComputation", MeasurementMethod: "NASA_Mean_CCSM4", ObservationPeriod: "P1Y"},
			},
		},
		{ // For FEMA NRI Expected Loss, prefer USDollar over FemaNationalRiskScore
			[]*pb.SourceSeries{
				{ImportName: "USFEMA_NationalRiskIndex", Unit: "FemaNationalRiskScore"},
				{ImportName: "USFEMA_NationalRiskIndex", Unit: "USDollar"},
			},
			[]*pb.SourceSeries{
				{ImportName: "USFEMA_NationalRiskIndex", Unit: "USDollar"},
				{ImportName: "USFEMA_NationalRiskIndex", Unit: "FemaNationalRiskScore"},
			},
		},
		{
			[]*pb.SourceSeries{
				{ImportName: "NASA_WetBulbComputation", MeasurementMethod: "NASA_Mean_CCSM4", ObservationPeriod: "P1Y"},
				{ImportName: "EarthquakeUSGS_Agg", MeasurementMethod: "GridWeightedPearson", ObservationPeriod: "P1M"},
				{ImportName: "EarthquakeUSGS_Agg", MeasurementMethod: "GridWeightedPearson", ObservationPeriod: "P1D"},
				{ImportName: "EarthquakeUSGS_Agg", MeasurementMethod: "GridWeightedPearson", ObservationPeriod: "P1Y"},
			},
			[]*pb.SourceSeries{
				{ImportName: "EarthquakeUSGS_Agg", MeasurementMethod: "GridWeightedPearson", ObservationPeriod: "P1Y"},
				{ImportName: "EarthquakeUSGS_Agg", MeasurementMethod: "GridWeightedPearson", ObservationPeriod: "P1D"},
				{ImportName: "EarthquakeUSGS_Agg", MeasurementMethod: "GridWeightedPearson", ObservationPeriod: "P1M"},
				{ImportName: "NASA_WetBulbComputation", MeasurementMethod: "NASA_Mean_CCSM4", ObservationPeriod: "P1Y"},
			},
		},
	} {
		sort.Sort(SeriesByRank(c.series))
		if diff := cmp.Diff(c.expected, c.series, protocmp.Transform()); diff != "" {
			t.Errorf("SeriesByRank() got diff result %v", diff)
		}
	}
}

func TestGetFacetScoreProvenanceAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		facet *pb.Facet
		want  int
	}{
		{
			name: "provenanceId only",
			facet: &pb.Facet{
				ProvenanceId:      "dc/base/USCensusPEP_Annual_Population",
				MeasurementMethod: "CensusPEPSurvey",
				ObservationPeriod: "P1Y",
			},
			want: 0,
		},
		{
			name: "provenanceId takes precedence when importName is human-readable",
			facet: &pb.Facet{
				ProvenanceId:      "dc/base/CensusACS5YearSurvey",
				ImportName:        "U.S. Census American Community Survey 5-Year",
				MeasurementMethod: "CensusACS5yrSurvey",
			},
			want: 1,
		},
		{
			name: "fallback to importName when provenanceId is empty (legacy Bigtable)",
			facet: &pb.Facet{
				ImportName:        "WorldDevelopmentIndicators",
				ObservationPeriod: "P1Y",
			},
			want: 4,
		},
		{
			name: "fallback to importName when provenanceId is unknown",
			facet: &pb.Facet{
				ProvenanceId:      "custom/unranked_prov",
				ImportName:        "IndiaCensus_Primary",
				ObservationPeriod: "P1Y",
			},
			want: 5,
		},
		{
			name: "inferior facet via provenanceId only",
			facet: &pb.Facet{
				ProvenanceId:      "dc/base/WikidataPopulation",
				MeasurementMethod: "WikidataPopulation",
			},
			want: 1003,
		},
		{
			name: "neither provenanceId nor importName matches returns BaseRank",
			facet: &pb.Facet{
				ProvenanceId: "dc/base/UnrankedImport",
			},
			want: BaseRank,
		},
		{
			name:  "nil facet returns BaseRank",
			facet: nil,
			want:  BaseRank,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := GetFacetScore(tc.facet); got != tc.want {
				t.Errorf("GetFacetScore() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestProvenanceAndStatsRankingParity(t *testing.T) {
	for provID, rkMap := range StatsRanking {
		importName := strings.TrimPrefix(provID, dcBasePrefix)
		for rk := range rkMap {
			mm, op, unit := "", "", ""
			if rk.MM != nil {
				mm = *rk.MM
			}
			if rk.OP != nil {
				op = *rk.OP
			}
			if rk.Unit != nil {
				unit = *rk.Unit
			}
			importScore := GetFacetScore(&pb.Facet{
				ImportName:        importName,
				MeasurementMethod: mm,
				ObservationPeriod: op,
				Unit:              unit,
			})
			provScore := GetFacetScore(&pb.Facet{
				ProvenanceId:      provID,
				MeasurementMethod: mm,
				ObservationPeriod: op,
				Unit:              unit,
			})
			if provScore != importScore {
				t.Errorf("score mismatch for %q vs %q (rk=%+v): provScore=%d, importScore=%d", provID, importName, rk, provScore, importScore)
			}
		}
	}
}

func TestFacetByRankWithProvenanceId(t *testing.T) {
	facets := []*pb.PlaceVariableFacet{
		{
			FacetId:    "wikidata",
			LatestDate: "2025",
			ObsCount:   50,
			Facet: &pb.Facet{
				ProvenanceId:      "dc/base/WikidataPopulation",
				MeasurementMethod: "WikidataPopulation",
			},
		},
		{
			FacetId:    "unranked_b",
			LatestDate: "2024",
			ObsCount:   10,
			Facet: &pb.Facet{
				ProvenanceId: "dc/base/Unranked_B",
			},
		},
		{
			FacetId:    "unranked_a",
			LatestDate: "2024",
			ObsCount:   10,
			Facet: &pb.Facet{
				ProvenanceId: "dc/base/Unranked_A",
			},
		},
		{
			FacetId:    "acs5yr",
			LatestDate: "2024",
			ObsCount:   14,
			Facet: &pb.Facet{
				ProvenanceId:      "dc/base/CensusACS5YearSurvey",
				MeasurementMethod: "CensusACS5yrSurvey",
			},
		},
		{
			FacetId:    "pep",
			LatestDate: "2023",
			ObsCount:   10,
			Facet: &pb.Facet{
				ProvenanceId:      "dc/base/USCensusPEP_Annual_Population",
				MeasurementMethod: "CensusPEPSurvey",
				ObservationPeriod: "P1Y",
			},
		},
	}

	sort.Sort(FacetByRank(facets))

	var gotIDs []string
	for _, f := range facets {
		gotIDs = append(gotIDs, f.FacetId)
	}
	wantIDs := []string{"pep", "acs5yr", "unranked_a", "unranked_b", "wikidata"}
	if diff := cmp.Diff(wantIDs, gotIDs); diff != "" {
		t.Errorf("FacetByRank order mismatch (-want +got):\n%s", diff)
	}
}
