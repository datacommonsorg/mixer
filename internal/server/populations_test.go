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
	key := util.BtPlaceObsPrefix + "City^2013^Person^gender^Male"
	btRow := []byte(`{
		"places":[
			{
				"name":"Stony Prairie CDP",
				"observations":[
					{
						"measuredProp":"count",
						"measuredValue":5000,
						"measurementMethod":"CensusACS5yrSurvey"
					}
				],
				"place":"geoId/3974832"
			},
			{
				"name":"Americus",
				"observations":[
					{
						"measuredProp":"count",
						"measuredValue":6000,
						"measurementMethod":"CensusACS5yrSurvey"
					}
				],
				"place":"geoId/2001675"
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

	var (
		resultProto, expectProto pb.PopObsCollection
	)
	s := NewServer(nil, btTable1, btTable2, nil)

	// Base cache only.
	in := &pb.GetPlaceObsRequest{
		PlaceType:       "City",
		StatVar:         "Count_Person_Male",
		ObservationDate: "2013",
	}
	out, err := s.GetPlaceObs(context.Background(), in)
	if err != nil {
		t.Errorf("GetPlaceObs get error %v", err)
	}

	if diff := cmp.Diff(out.GetPayload(), tableValue); diff != "" {
		t.Errorf("GetPlaceObs() got diff: %v", diff)
	}
	tmp, err := util.UnzipAndDecode(out.GetPayload())
	if err != nil {
		t.Errorf("UnzipAndDecode got error %v", err)
	}
	if err = protojson.Unmarshal(tmp, &resultProto); err != nil {
		t.Errorf("Unmarshal result proto got error %v", err)
	}
	if err = protojson.Unmarshal(btRow, &expectProto); err != nil {
		t.Errorf("Unmarshal expected proto got error %v", err)
	}

	if diff := cmp.Diff(&resultProto, &expectProto, protocmp.Transform()); diff != "" {
		t.Errorf("GetPlaceObs() got diff %+v", diff)
	}
}

func TestGetPlaceObsCacheMerge(t *testing.T) {
	key := util.BtPlaceObsPrefix + "City^2013^Person^gender^Male"

	// No base data
	baseData := map[string]string{}
	baseTable, err := SetupBigtable(context.Background(), baseData)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	// branch cache data. Have observation on newer date.
	branchData := map[string]string{}
	branchRow := []byte(`{
		"places":[
			{
				"name":"Stony Prairie CDP",
				"observations":[
					{
						"measuredProp":"count",
						"measuredValue":5000,
						"measurementMethod":"CensusACS5yrSurvey"
					}
				],
				"place":"geoId/3974832"
			},
			{
				"name":"Americus",
				"observations":[
					{
						"measuredProp":"count",
						"measuredValue":6000,
						"measurementMethod":"CensusACS5yrSurvey"
					}
				],
				"place":"geoId/2001675"
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
	in := &pb.GetPlaceObsRequest{
		PlaceType:       "City",
		StatVar:         "Count_Person_Male",
		ObservationDate: "2013",
	}
	out, err := s.GetPlaceObs(context.Background(), in)
	if err != nil {
		t.Errorf("GetPlaceObs got err %v", err)
		return
	}

	var resultProto, expectProto pb.PopObsCollection
	tmp, err := util.UnzipAndDecode(out.GetPayload())
	if err != nil {
		t.Errorf("UnzipAndDecode got err %v", err)
	}
	if err = protojson.Unmarshal(tmp, &resultProto); err != nil {
		t.Errorf("Unmarshal result proto got error %v", err)
	}
	if err = protojson.Unmarshal(branchRow, &expectProto); err != nil {
		t.Errorf("Unmarshal expected proto got error %v", err)
	}

	if diff := cmp.Diff(&resultProto, &expectProto, protocmp.Transform()); diff != "" {
		t.Errorf("GetPlaceObs() got diff %+v", diff)
	}
}
