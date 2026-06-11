// Copyright 2024 Google LLC
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

package sqlquery

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/go-test/deep"
	"google.golang.org/protobuf/proto"
)

func TestGetStatVarSummaries(t *testing.T) {
	sqlClient, err := sqldb.NewSQLiteClient("../../../test/sqlquery/statvar_summary/datacommons.db")
	if err != nil {
		t.Fatalf("Could not open testing database: %v", err)
	}
	err = sqlClient.ValidateDatabase()
	if err != nil {
		t.Fatalf("SQL database validation failed: %v", err)
	}

	want := map[string]*pb.StatVarSummary{
		"var1": {
			PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
				"Country": {
					PlaceCount: 2,
					MinValue:   proto.Float64(5),
					MaxValue:   proto.Float64(7),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "country/USA", Name: "country/USA"},
						1: {Dcid: "country/CHN", Name: "country/CHN"},
					},
				},
				"State": {
					PlaceCount: 2,
					MinValue:   proto.Float64(1),
					MaxValue:   proto.Float64(4),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "geoId/01", Name: "geoId/01"},
						1: {Dcid: "geoId/02", Name: "geoId/02"},
					},
				},
			},
			ProvenanceSummary: map[string]*pb.StatVarSummary_ProvenanceSummary{
				"custom": {
					ImportName: "custom",
					SeriesSummary: []*pb.StatVarSummary_SeriesSummary{
						{
							SeriesKey:    &pb.StatVarSummary_SeriesSummary_SeriesKey{},
							EarliestDate: "2022",
							LatestDate:   "2024",
							MinValue:     proto.Float64(1),
							MaxValue:     proto.Float64(7),
							PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
								"Country": {
									PlaceCount: 2,
									MinValue:   proto.Float64(5),
									MaxValue:   proto.Float64(7),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "country/USA", Name: "country/USA"},
										{Dcid: "country/CHN", Name: "country/CHN"},
									},
								},
								"State": {
									PlaceCount: 2,
									MinValue:   proto.Float64(1),
									MaxValue:   proto.Float64(4),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "geoId/01", Name: "geoId/01"},
										{Dcid: "geoId/02", Name: "geoId/02"},
									},
								},
							},
							ObservationCount: 8,
							TimeSeriesCount:  4,
						},
					},
					ObservationCount: 8,
					TimeSeriesCount:  4,
				},
			},
		},
		"var2": {
			PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
				"Country": {
					PlaceCount: 3,
					MinValue:   proto.Float64(15),
					MaxValue:   proto.Float64(17),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "country/USA", Name: "country/USA"},
						1: {Dcid: "country/CHN", Name: "country/CHN"},
						2: {Dcid: "country/JPN", Name: "country/JPN"},
					},
				},
				"State": {
					PlaceCount: 2,
					MinValue:   proto.Float64(11),
					MaxValue:   proto.Float64(13),
					TopPlaces: []*pb.StatVarSummary_Place{
						0: {Dcid: "geoId/01", Name: "geoId/01"},
						1: {Dcid: "geoId/03", Name: "geoId/03"},
					},
				},
			},
			ProvenanceSummary: map[string]*pb.StatVarSummary_ProvenanceSummary{
				"custom": {
					ImportName: "custom",
					SeriesSummary: []*pb.StatVarSummary_SeriesSummary{
						{
							SeriesKey:    &pb.StatVarSummary_SeriesSummary_SeriesKey{},
							EarliestDate: "2022",
							LatestDate:   "2024",
							MinValue:     proto.Float64(11),
							MaxValue:     proto.Float64(17),
							PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
								"Country": {
									PlaceCount: 3,
									MinValue:   proto.Float64(15),
									MaxValue:   proto.Float64(17),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "country/USA", Name: "country/USA"},
										{Dcid: "country/CHN", Name: "country/CHN"},
										{Dcid: "country/JPN", Name: "country/JPN"},
									},
								},
								"State": {
									PlaceCount: 2,
									MinValue:   proto.Float64(11),
									MaxValue:   proto.Float64(13),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "geoId/01", Name: "geoId/01"},
										{Dcid: "geoId/03", Name: "geoId/03"},
									},
								},
							},
							ObservationCount: 7,
							TimeSeriesCount:  5,
						},
					},
					ObservationCount: 7,
					TimeSeriesCount:  5,
				},
			},
		},
		"var3": {
			PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
				"Country": {
					PlaceCount: 1,
					MinValue:   proto.Float64(150),
					MaxValue:   proto.Float64(400),
					TopPlaces: []*pb.StatVarSummary_Place{
						{Dcid: "country/USA", Name: "country/USA"},
					},
				},
				"State": {
					PlaceCount: 2,
					MinValue:   proto.Float64(100),
					MaxValue:   proto.Float64(300),
					TopPlaces: []*pb.StatVarSummary_Place{
						{Dcid: "geoId/01", Name: "geoId/01"},
						{Dcid: "geoId/02", Name: "geoId/02"},
					},
				},
			},
			ProvenanceSummary: map[string]*pb.StatVarSummary_ProvenanceSummary{
				"prov_a": {
					ImportName: "Provenance A",
					SeriesSummary: []*pb.StatVarSummary_SeriesSummary{
						{
							SeriesKey: &pb.StatVarSummary_SeriesSummary_SeriesKey{
								MeasurementMethod: "cm",
								ObservationPeriod: "P1Y",
								ScalingFactor:     "1",
								Unit:              "USD",
							},
							EarliestDate: "2023",
							LatestDate:   "2024",
							MinValue:     proto.Float64(100),
							MaxValue:     proto.Float64(150),
							PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
								"Country": {
									PlaceCount: 1,
									MinValue:   proto.Float64(150),
									MaxValue:   proto.Float64(150),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "country/USA", Name: "country/USA"},
									},
								},
								"State": {
									PlaceCount: 2,
									MinValue:   proto.Float64(100),
									MaxValue:   proto.Float64(120),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "geoId/01", Name: "geoId/01"},
										{Dcid: "geoId/02", Name: "geoId/02"},
									},
								},
							},
							ObservationCount: 4,
							TimeSeriesCount:  3,
						},
						{
							SeriesKey: &pb.StatVarSummary_SeriesSummary_SeriesKey{
								MeasurementMethod: "cm",
								ObservationPeriod: "P5Y",
								ScalingFactor:     "1",
								Unit:              "USD",
							},
							EarliestDate: "2023",
							LatestDate:   "2024",
							MinValue:     proto.Float64(200),
							MaxValue:     proto.Float64(210),
							PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
								"Country": {
									PlaceCount: 1,
									MinValue:   proto.Float64(200),
									MaxValue:   proto.Float64(210),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "country/USA", Name: "country/USA"},
									},
								},
							},
							ObservationCount: 2,
							TimeSeriesCount:  1,
						},
					},
					ObservationCount: 6,
					TimeSeriesCount:  4,
				},
				"prov_b": {
					ImportName: "Provenance B",
					SeriesSummary: []*pb.StatVarSummary_SeriesSummary{
						{
							SeriesKey:    &pb.StatVarSummary_SeriesSummary_SeriesKey{},
							EarliestDate: "2023",
							LatestDate:   "2024",
							MinValue:     proto.Float64(300),
							MaxValue:     proto.Float64(400),
							PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{
								"Country": {
									PlaceCount: 1,
									MinValue:   proto.Float64(400),
									MaxValue:   proto.Float64(400),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "country/USA", Name: "country/USA"},
									},
								},
								"State": {
									PlaceCount: 1,
									MinValue:   proto.Float64(300),
									MaxValue:   proto.Float64(300),
									TopPlaces: []*pb.StatVarSummary_Place{
										{Dcid: "geoId/01", Name: "geoId/01"},
									},
								},
							},
							ObservationCount: 2,
							TimeSeriesCount:  2,
						},
					},
					ObservationCount: 2,
					TimeSeriesCount:  2,
				},
			},
		},
	}

	got, err := GetStatVarSummaries(context.Background(), sqlClient, []string{"var1", "var2", "var3"})
	if err != nil {
		t.Fatalf("Error getting stat var summaries: %v", err)
	}

	if diff := deep.Equal(got, want); diff != nil {
		t.Errorf("Unexpected diff: %v", diff)
	}
}
