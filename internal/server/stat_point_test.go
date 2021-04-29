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

package server

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStatCollection(t *testing.T) {
	keys := make([]string, 5)
	rows := make([][]byte, 5)
	// Count_Person, 2018, with mutliple sources and large place coverage
	keys[0] = "d/f/country/USA^State^Count_Person^2018"
	rows[0] = []byte(`{
		"obsCollection": {
			"sourceCohorts": [
				{
					"importName": "source1",
					"val": {
						"geoId/37": 10383620
					}
				},
				{
					"importName": "source2",
					"measurementMethod": "mmethod2",
					"observationPeriod": "P1Y",
					"val": {
						"geoId/01": 4887680,
						"geoId/02": 735139,
						"geoId/04": 7158020,
						"geoId/05": 3009730
					}
				},
				{
					"importName": "source3",
					"measurementMethod": "mmethod3",
					"val": {
						"geoId/02": 735139,
						"geoId/04": 7158024,
						"geoId/05": 3009733,
						"geoId/06": 39461588,
						"geoId/08": 5691287
					}
				}
			]
		}
	}`)

	// Count_Person, 2018, with single source of limited places
	keys[1] = "d/f/country/USA^State^Count_Person^2019"
	rows[1] = []byte(`{
		"obsCollection": {
			"sourceCohorts": [
				{
					"importName": "source1",
					"val": {
						"geoId/37": 11383620
					}
				}
			]
		}
	}`)

	// Count_Person date cache
	keys[2] = "d/f/country/USA^State^Count_Person^"
	rows[2] = []byte(`{
		"obsCollection": {
			"sourceCohorts": [
				{
					"importName": "source1",
					"val": {
						"2017": 3,
						"2018": 1,
						"2019": 1
					}
				},
				{
					"importName": "source2",
					"val": {
						"2016": 4,
						"2017": 4,
						"2018": 4
					}
				},
				{
					"importName": "source3",
					"val": {
						"2016": 5,
						"2017": 5,
						"2018": 5
					}
				}
			]
		}
	}`)

	// Median_Age_Person, 2018, with single source
	keys[3] = "d/f/country/USA^State^Median_Age_Person^2018"
	rows[3] = []byte(`{
		"obsCollection": {
			"sourceCohorts": [
				{
					"importName": "source1",
					"val": {
						"geoId/01": 38.9,
						"geoId/02": 34,
						"geoId/04": 37.4,
						"geoId/05": 37.9,
						"geoId/06": 36.3,
						"geoId/08": 36.6
					}
				}
			]
		}
	}`)
	// Median_Age_Person date cache
	keys[4] = "d/f/country/USA^State^Median_Age_Person^"
	rows[4] = []byte(`{
		"obsCollection": {
			"sourceCohorts": [
				{
					"importName": "source1",
					"val": {
						"2015": 6,
						"2016": 6,
						"2017": 6,
						"2018": 6
					}
				}
			]
		}
	}`)

	// Response
	respByte := []byte(`{
		"data": {
			"Count_Person": {
				"stat": {
					"geoId/02": {
						"date": "2018",
						"value": 735139
					},
					"geoId/04": {
						"date": "2018",
						"value": 7158024
					},
					"geoId/05": {
						"date": "2018",
						"value": 3009733
					},
					"geoId/06": {
						"date": "2018",
						"value": 39461588
					},
					"geoId/08": {
						"date": "2018",
						"value": 5691287
					},
					"geoId/37": {
						"date": "2019",
						"value": 11383620
					}
				},
				"metadata": [
					{
						"import_name": "source1"
					},
					{
						"import_name": "source3",
						"measurement_method": "mmethod3"
					}
				]
			},
			"Median_Age_Person": {
				"stat": {
					"geoId/01": {
						"date": "2018",
						"value": 38.9
					},
					"geoId/02": {
						"date": "2018",
						"value": 34
					},
					"geoId/04": {
						"date": "2018",
						"value": 37.4
					},
					"geoId/05": {
						"date": "2018",
						"value": 37.9
					},
					"geoId/06": {
						"date": "2018",
						"value": 36.3
					},
					"geoId/08": {
						"date": "2018",
						"value": 36.6
					}
				},
				"metadata": [
					{
						"import_name": "source1"
					}
				]
			}
		}
	}`)

	data := map[string]string{}
	for i, key := range keys {
		rowStr, err := util.ZipAndEncode(rows[i])
		if err != nil {
			t.Errorf("util.ZipAndEncode(%+v) = %v", rows[i], err)
		}
		data[key] = rowStr
	}
	// Setup bigtable
	btTable, err := SetupBigtable(context.Background(), data)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	s := NewServer(nil, btTable, nil, nil)
	req := &pb.GetStatCollectionRequest{
		ParentPlace: "country/USA",
		ChildType:   "State",
		StatVars:    []string{"Count_Person", "Median_Age_Person"},
	}
	resp, err := s.GetStatCollection(context.Background(), req)
	if err != nil {
		t.Errorf("GetStatCollection() got err: %v", err)
	}

	expected := &pb.GetStatCollectionResponse{}
	err = protojson.Unmarshal(respByte, expected)
	if err != nil {
		t.Errorf(("Unmarshal respByte raw failed"))
	}
	if diff := cmp.Diff(resp, expected, protocmp.Transform()); diff != "" {
		t.Errorf("GetStatCollection() got diff: %v", diff)
	}
}
