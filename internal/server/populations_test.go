// Copyright 2019 Google LLC
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

func TestGetPlaceObs(t *testing.T) {
	data := map[string]string{}
	key := util.BtPlaceObsPrefix + "City^Count_Person_Male^2013"
	btRow := []byte(`{
		"places": [
			{
				"name": "Webster County",
				"dcid": "geoId/12345",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 4199
					}
				]
			},
			{
				"name": "Nobles County",
				"dcid": "geoId/22222",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 11236
					}
				]
			}
		]
	}`)

	tableValue, err := util.ZipAndEncode(btRow)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", btRow, err)
	}
	data[key] = tableValue
	// Setup bigtable
	btTable1, err := SetupBigtable(context.Background(), data)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}
	btTable2, err := SetupBigtable(context.Background(), map[string]string{})
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	s := NewServer(nil, btTable1, btTable2, nil)

	// Base cache only.
	req := &pb.GetPlaceObsRequest{
		PlaceType: "City",
		StatVar:   "Count_Person_Male",
		Date:      "2013",
	}
	result, err := s.GetPlaceObs(context.Background(), req)
	if err != nil {
		t.Errorf("GetPlaceObs get error %v", err)
	}

	var expected pb.SVOCollection
	err = protojson.Unmarshal(btRow, &expected)
	if err != nil {
		t.Errorf("Unmarshell expected got error %v", err)
		return
	}

	if diff := cmp.Diff(result, &expected, protocmp.Transform()); diff != "" {
		t.Errorf("GetPlaceObs() got diff %+v", diff)
	}
}

func TestGetPlaceObsCacheMerge(t *testing.T) {
	key := util.BtPlaceObsPrefix + "City^Count_Person_Male^2013"

	// No base data
	baseData := map[string]string{}
	baseRow := []byte(`{
		"places": [
			{
				"name": "Webster County",
				"dcid": "geoId/12345",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 4199
					}
				]
			},
			{
				"name": "Nobles County",
				"dcid": "geoId/20000",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 11236
					}
				]
			}
		]
	}`)
	baseValue, err := util.ZipAndEncode(baseRow)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", baseRow, err)
	}
	baseData[key] = baseValue

	baseTable, err := SetupBigtable(context.Background(), baseData)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	// branch cache data. Have observation on more entries.
	branchData := map[string]string{}
	branchRow := []byte(`{
		"places": [
			{
				"name": "Lincoln Parish",
				"dcid": "geoId/22049",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 23041
					}
				]
			},
			{
				"name": "Jackson Parish",
				"dcid": "geoId/22061",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 8080
					}
				]
			}
		]
	}`)
	branchCacheValue, err := util.ZipAndEncode(branchRow)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", branchRow, err)
	}
	branchData[key] = branchCacheValue

	branchTable, err := SetupBigtable(context.Background(), branchData)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	// Test
	s := NewServer(nil, baseTable, branchTable, nil)
	req := &pb.GetPlaceObsRequest{
		PlaceType: "City",
		StatVar:   "Count_Person_Male",
		Date:      "2013",
	}
	result, err := s.GetPlaceObs(context.Background(), req)
	if err != nil {
		t.Errorf("GetPlaceObs got err %v", err)
		return
	}

	expectedByte := []byte(`{
  	"places": [
			{
				"name": "Webster County",
				"dcid": "geoId/12345",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 4199
					}
				]
			},
			{
				"name": "Nobles County",
				"dcid": "geoId/20000",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 11236
					}
				]
			},
			{
				"name": "Lincoln Parish",
				"dcid": "geoId/22049",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 23041
					}
				]
			},
			{
				"name": "Jackson Parish",
				"dcid": "geoId/22061",
				"observations": [
					{
						"measurementMethod": "CensusACS5yrSurvey",
						"dblValue": 8080
					}
				]
			}
		]
	}`)

	var expected pb.SVOCollection
	err = protojson.Unmarshal(expectedByte, &expected)
	if err != nil {
		t.Errorf("Unmarshell expected got error %v", err)
		return
	}

	if diff := cmp.Diff(result, &expected, protocmp.Transform()); diff != "" {
		t.Errorf("GetPlaceObs() got diff %+v", diff)
	}
}
