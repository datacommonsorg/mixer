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

package memdb

import (
	"testing"

	"github.com/datacommonsorg/mixer/internal/parser/tmcf"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

var (
	ts = &tmcf.TableSchema{
		ColumnInfo: map[string][]*tmcf.Column{
			"CumulativeCount_Vaccine_COVID_19_Administered": {
				{Node: "E0", Property: "value"},
			},
			"IncrementalCount_Vaccine_COVID_19_Administered": {
				{Node: "E1", Property: "value"},
			},
			"GeoId": {
				{Node: "E0", Property: "observationAbout"},
				{Node: "E1", Property: "observationAbout"},
			},
			"Date": {
				{Node: "E0", Property: "observationDate"},
				{Node: "E1", Property: "observationDate"},
			},
		},
		NodeSchema: map[string]map[string]string{
			"E0": {
				"measurementMethod": "OurWorldInData_COVID19",
				"typeOf":            "StatVarObservation",
				"variableMeasured":  "CumulativeCount_Vaccine_COVID_19_Administered",
			},
			"E1": {
				"measurementMethod": "OurWorldInData_COVID19",
				"typeOf":            "StatVarObservation",
				"variableMeasured":  "IncrementalCount_Vaccine_COVID_19_Administered",
			},
		},
	}

	config = &pb.MemdbConfig{
		ImportName:    "Private Import",
		ProvenanceUrl: "private.domain",
	}
)

func TestAddRow(t *testing.T) {
	for _, c := range []struct {
		header []string
		rows   [][]string
		want   map[string]map[string][]*pb.Series
	}{
		{
			[]string{
				"Date",
				"GeoId",
				"CumulativeCount_Vaccine_COVID_19_Administered",
				"IncrementalCount_Vaccine_COVID_19_Administered",
			},
			[][]string{{"2020-03-22", "country/AFG", "100", "10"}},
			map[string]map[string][]*pb.Series{
				"CumulativeCount_Vaccine_COVID_19_Administered": {
					"country/AFG": []*pb.Series{
						{
							Val: map[string]float64{"2020-03-22": 100},
							Metadata: &pb.StatMetadata{
								MeasurementMethod: "OurWorldInData_COVID19",
								ImportName:        "Private Import",
								ProvenanceUrl:     "private.domain",
							},
						},
					},
				},
				"IncrementalCount_Vaccine_COVID_19_Administered": {
					"country/AFG": []*pb.Series{
						{
							Val: map[string]float64{"2020-03-22": 10},
							Metadata: &pb.StatMetadata{
								MeasurementMethod: "OurWorldInData_COVID19",
								ImportName:        "Private Import",
								ProvenanceUrl:     "private.domain",
							},
						},
					},
				},
			},
		},
		{
			[]string{
				"Date",
				"GeoId",
				"CumulativeCount_Vaccine_COVID_19_Administered",
				"IncrementalCount_Vaccine_COVID_19_Administered",
			},
			[][]string{
				{"2020-03-22", "country/AFG", "100", "10"},
				{"2020-03-22", "country/USA", "200", "20"},
				{"2020-03-23", "country/USA", "250", "25"},
				{"2020-03-22", "country/ALB", "30", ""},
			},
			map[string]map[string][]*pb.Series{
				"CumulativeCount_Vaccine_COVID_19_Administered": {
					"country/AFG": []*pb.Series{
						{
							Val: map[string]float64{"2020-03-22": 100},
							Metadata: &pb.StatMetadata{
								MeasurementMethod: "OurWorldInData_COVID19",
								ImportName:        "Private Import",
								ProvenanceUrl:     "private.domain",
							},
						},
					},
					"country/USA": []*pb.Series{
						{
							Val: map[string]float64{"2020-03-22": 200, "2020-03-23": 250},
							Metadata: &pb.StatMetadata{
								MeasurementMethod: "OurWorldInData_COVID19",
								ImportName:        "Private Import",
								ProvenanceUrl:     "private.domain",
							},
						},
					},
					"country/ALB": []*pb.Series{
						{
							Val: map[string]float64{"2020-03-22": 30},
							Metadata: &pb.StatMetadata{
								MeasurementMethod: "OurWorldInData_COVID19",
								ImportName:        "Private Import",
								ProvenanceUrl:     "private.domain",
							},
						},
					},
				},
				"IncrementalCount_Vaccine_COVID_19_Administered": {
					"country/AFG": []*pb.Series{
						{
							Val: map[string]float64{"2020-03-22": 10},
							Metadata: &pb.StatMetadata{
								MeasurementMethod: "OurWorldInData_COVID19",
								ImportName:        "Private Import",
								ProvenanceUrl:     "private.domain",
							},
						},
					},
					"country/USA": []*pb.Series{
						{
							Val: map[string]float64{"2020-03-22": 20, "2020-03-23": 25},
							Metadata: &pb.StatMetadata{
								MeasurementMethod: "OurWorldInData_COVID19",
								ImportName:        "Private Import",
								ProvenanceUrl:     "private.domain",
							},
						},
					},
					"country/ALB": []*pb.Series{},
				},
			},
		},
	} {
		memDb := NewMemDb()
		for _, row := range c.rows {
			memDb.config = config
			err := memDb.addRow(c.header, row, ts)
			if err != nil {
				t.Fail()
			}
		}
		if diff := cmp.Diff(memDb.statSeries, c.want, protocmp.Transform()); diff != "" {
			t.Errorf("ParseTmcf got diff: %v", diff)
			continue
		}
	}
}
