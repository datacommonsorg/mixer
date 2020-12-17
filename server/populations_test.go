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

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetPopObs(t *testing.T) {
	data := map[string]string{}
	dcid := "geoId/06"
	key := util.BtPopObsPrefix + dcid
	btRow := []byte(`{
		"name": "Santa Clara",
		"populations": {
			"dc/p/zzlmxxtp1el87": {
				"popType": "Household",
				"numConstraints": 3,
				"propertyValues": {
					"householderAge": "Years45To64",
					"householderRace": "AsianAlone",
					"income": "USDollar35000To39999"
				},
				"observations": [
					{
						"marginOfError": 274,
						"measuredProp": "count",
						"measuredValue": 1352,
						"measurementMethod": "CensusACS5yrSurvey",
						"observationDate": "2017"
					},
					{
						"marginOfError": 226,
						"measuredProp": "count",
						"measuredValue": 1388,
						"measurementMethod": "CensusACS5yrSurvey",
						"observationDate": "2013"
					}
				]
			}
		},
		"observations": [
			{
				"meanValue": 4.1583,
				"measuredProp": "particulateMatter25",
				"measurementMethod": "CDCHealthTracking",
				"observationDate": "2014-04-04",
				"observedNode": "geoId/06085"
			},
			{
				"meanValue": 9.4461,
				"measuredProp": "particulateMatter25",
				"measurementMethod": "CDCHealthTracking",
				"observationDate": "2014-03-20",
				"observedNode": "geoId/06085"
			}
		]
	}`)

	tableValue, err := util.ZipAndEncode(btRow)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", btRow, err)
	}
	data[key] = tableValue
	// Setup bigtable
	btTable, err := setupBigtable(context.Background(), data)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}
	// Test
	s := NewServer(nil, btTable, NewMemcache(map[string][]byte{}), nil)
	in := &pb.GetPopObsRequest{
		Dcid: dcid,
	}
	out, err := s.GetPopObs(context.Background(), in)
	if err != nil {
		t.Errorf("GetPopObs() got err: %v", err)
	}
	if diff := cmp.Diff(out.GetPayload(), tableValue); diff != "" {
		t.Errorf("GetPopObs() got diff: %v", diff)
	}
}

func TestGetPopObsCacheMerge(t *testing.T) {
	dcid := "geoId/06"
	key := util.BtPopObsPrefix + dcid

	// Setup bigtable
	baseData := map[string]string{}
	btRow := []byte(`{
		"name": "Santa Clara",
		"populations": {
			"dc/p/abcxyz": {
				"popType": "Household",
				"numConstraints": 3,
				"propertyValues": {
					"householderAge": "Years45To64",
					"householderRace": "AsianAlone",
					"income": "USDollar35000To39999"
				},
				"observations": [
					{
						"marginOfError": 226,
						"measuredProp": "count",
						"measuredValue": 1388,
						"measurementMethod": "CensusACS5yrSurvey",
						"observationDate": "2013"
					}
				]
			}
		}
	}`)
	tableValue, err := util.ZipAndEncode(btRow)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", btRow, err)
	}
	baseData[key] = tableValue
	btTable, err := setupBigtable(context.Background(), baseData)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	// branch cache data. Have observation on newer date.
	branchData := map[string][]byte{}
	branchCache := []byte(`{
		"name": "Santa Clara",
		"populations": {
			"dc/p/abcxyz": {
				"popType": "Household",
				"numConstraints": 3,
				"propertyValues": {
					"householderAge": "Years45To64",
					"householderRace": "AsianAlone",
					"income": "USDollar35000To39999"
				},
				"observations": [
					{
						"marginOfError": 274,
						"measuredProp": "count",
						"measuredValue": 1352,
						"measurementMethod": "CensusACS5yrSurvey",
						"observationDate": "2017"
					},
					{
						"marginOfError": 226,
						"measuredProp": "count",
						"measuredValue": 1388,
						"measurementMethod": "CensusACS5yrSurvey",
						"observationDate": "2013"
					}
				]
			}
		}
	}`)
	branchCacheValue, err := util.ZipAndEncode(branchCache)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", branchCache, err)
	}
	branchData[key] = []byte(branchCacheValue)
	// Test
	memcache := Memcache{}
	memcache.Update(branchData)
	s := NewServer(nil, btTable, &memcache, nil)

	var (
		resultProto, expectProto pb.PopObsPlace
	)

	// Merge base cache and branch cache.
	in := &pb.GetPopObsRequest{
		Dcid: dcid,
	}
	out, err := s.GetPopObs(context.Background(), in)
	if err != nil {
		t.Errorf("GetPopObs() got error %v", err)
	}

	if tmp, err := util.UnzipAndDecode(out.GetPayload()); err == nil {
		err = protojson.Unmarshal(tmp, &resultProto)
		if err != nil {
			t.Errorf("Unmarshal result got error %v", err)
			return
		}
	}
	err = protojson.Unmarshal(branchCache, &expectProto)
	if err != nil {
		t.Errorf("Unmarshal branchCache got err %v", err)
	}
	if diff := cmp.Diff(&resultProto, &expectProto, protocmp.Transform()); diff != "" {
		t.Errorf("GetPopObs() got diff %+v", diff)
	}
}

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
	btTable, err := setupBigtable(context.Background(), data)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	var (
		resultProto, expectProto pb.PopObsCollection
	)
	s := NewServer(nil, btTable, NewMemcache(map[string][]byte{}), nil)

	// Base cache only.
	in := &pb.GetPlaceObsRequest{
		PlaceType:       "City",
		PopulationType:  "Person",
		ObservationDate: "2013",
		Pvs: []*pb.PropertyValue{
			{Property: "gender", Value: "Male"},
		},
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
	btTable, err := setupBigtable(context.Background(), baseData)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}

	// branch cache data. Have observation on newer date.
	branchData := map[string][]byte{}
	branchCache := []byte(`{
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
	branchCacheValue, err := util.ZipAndEncode(branchCache)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", branchCache, err)
	}
	branchData[key] = []byte(branchCacheValue)
	// Test
	memcache := Memcache{}
	memcache.Update(branchData)
	s := NewServer(nil, btTable, &memcache, nil)
	in := &pb.GetPlaceObsRequest{
		PlaceType:       "City",
		PopulationType:  "Person",
		ObservationDate: "2013",
		Pvs: []*pb.PropertyValue{
			{Property: "gender", Value: "Male"},
		},
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
	if err = protojson.Unmarshal(branchCache, &expectProto); err != nil {
		t.Errorf("Unmarshal expected proto got error %v", err)
	}

	if diff := cmp.Diff(&resultProto, &expectProto, protocmp.Transform()); diff != "" {
		t.Errorf("GetPlaceObs() got diff %+v", diff)
	}
}

func TestIsterateSortPVs(t *testing.T) {
	var pvs = []*pb.PropertyValue{
		{
			Property: "gender",
			Value:    "Male",
		},
		{
			Property: "age",
			Value:    "Years85Onwards",
		},
	}
	got := "^populationType"
	if len(pvs) > 0 {
		iterateSortPVs(pvs, func(i int, p, v string) {
			got += ("^" + p + "^" + v)
		})
	}

	want := "^populationType^age^Years85Onwards^gender^Male"
	if got != want {
		t.Errorf("iterateSortPVs() = %s, want %s", got, want)
	}
}
