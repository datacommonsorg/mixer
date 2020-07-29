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

package translator

import (
	"testing"

	"github.com/datacommonsorg/mixer/base"

	"github.com/go-test/deep"
)

func TestGetNodeType(t *testing.T) {
	for _, c := range []struct {
		datalog      string
		wantNodeType map[string]string
		wantErr      bool
	}{
		{
			`
			SELECT ?dcid,
    		typeOf ?parent_node Place,
    		typeOf ?node Place,
    		subType ?node City,
			dcid ?node ?dcid
			`,
			map[string]string{
				"?parent_node": "Place",
				"?node":        "Place",
			},
			false,
		},
		{
			`
			SELECT ?dcid,
    		typeOf ?parent_node Place,
    		typeOf ?parent_node State,
			dcid ?node ?dcid
			`,
			nil,
			true,
		},
	} {
		_, queries, _ := ParseQuery(c.datalog)
		gotNodeType, err := GetNodeType(queries)
		if c.wantErr {
			if err == nil {
				t.Errorf("GetNodeType(%s) = nil, want error", c.datalog)
			}
			continue
		}
		if diff := deep.Equal(c.wantNodeType, gotNodeType); diff != nil {
			t.Errorf("GetNodeType from query %s, unexpected result diff %v", c.datalog, diff)
		}
	}
}

func TestGetEntityType(t *testing.T) {
	mappingStr := `
		Node: E:Observation->E2
		typeOf: StatisticalPopulation
		typeOf: Place
		typeOf: Tract
		typeOf: CensusTract
		typeOf: CommutingZone
		dcid: C:Observation->observed_node_key
		functionalDeps: dcid

		Node: E:Observation->E3
		typeOf: Provenance
		dcid: C:Observation->prov_id
		functionalDeps: dcid`

	mappings, _ := ParseMapping(mappingStr, "dc")
	gotEntityType := GetEntityType(mappings)
	wantEntityType := map[string][]string{
		"`dc.Observation`->E2": []string{
			"StatisticalPopulation", "Place", "Tract", "CensusTract", "CommutingZone"},
		"`dc.Observation`->E3": []string{"Provenance"},
	}

	if diff := deep.Equal(wantEntityType, gotEntityType); diff != nil {
		t.Errorf("GetEntityType, unexpected result diff %v", diff)
	}
}

func TestGetExplicitTypeProp(t *testing.T) {
	mappingStr := `
		Node: E:Observation->E1
		typeOf: Observation
		dcid: C:Observation->id
		provenance: E:Observation->E3
		isPublic: C:Observation->is_public
		measuredProperty: C:Observation->measured_prop
		startTime: C:Observation->start_time_us
		endTime: C:Observation->end_time_us

		Node: E:Observation->E2
		typeOf: Place
		typeOf: CommutingZone
		dcid: C:Observation->observed_node_key
		functionalDeps: dcid`

	mappings, _ := ParseMapping(mappingStr, "dc")
	mappings = PruneMapping(mappings)

	gotTypeProp := GetExplicitTypeProp(mappings)
	wantTypeProp := map[string][]string{
		"Observation": []string{
			"typeOf", "dcid", "provenance", "isPublic", "measuredProperty", "startTime", "endTime"},
	}

	if diff := deep.Equal(wantTypeProp, gotTypeProp); diff != nil {
		t.Errorf("GetExplicitTypeProp(%s), unexpected result diff %v", mappingStr, diff)
	}
}

func TestGetQueryID(t *testing.T) {
	queryStr := `
		SELECT ?dcid ?count_value,
		typeOf ?node Place,
		typeOf ?pop StatisticalPopulation,
		typeOf ?o Observation,
		dcid ?node X1234,
		childhoodLocation ?pop ?node,
		observedNode ?o ?pop,
		measuredValue ?o ?count_value
		`

	_, queries, err := ParseQuery(queryStr)
	if err != nil {
		t.Errorf("ParseQuery(%s) got %s", queryStr, err)
	}
	matchTriple := map[*base.Query]bool{
		queries[0]: false,
		queries[1]: false,
		queries[2]: false,
		queries[3]: false,
		queries[4]: true,
		queries[5]: false,
		queries[6]: false,
	}

	gotQueryID := GetQueryID(queries, matchTriple)
	wantQueryID := map[*base.Query]int{
		queries[0]: 0,
		queries[1]: 1,
		queries[2]: 2,
		queries[3]: 0,
		queries[4]: 0,
		queries[5]: 2,
		queries[6]: 2,
	}

	if diff := deep.Equal(wantQueryID, gotQueryID); diff != nil {
		t.Errorf("GetQueryID(%s), unexpected result diff %v", queryStr, diff)
	}
}

func TestMatchTriple(t *testing.T) {
	mappings := readTestMapping(t, []string{"test_mapping.mcf"})
	queryStr := `
		SELECT ?dcid,
		typeOf ?parent_node Place,
		typeOf ?node Place,
		subType ?node City,
		countryAlpha2Code ?node "country-code",
		geoId ?node geo-id,
		containedInPlace ?node ?parent_node,
		dcid ?parent_node dc/x333,
		dcid ?node ?dcid`

	_, queries, err := ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("parsing query string %s: %s", queryStr, err)
	}

	wantResult := map[*base.Query]bool{
		queries[0]: false,
		queries[1]: false,
		queries[2]: false,
		queries[3]: false,
		queries[4]: true,
		queries[5]: true,
		queries[6]: false,
		queries[7]: false,
	}
	gotResult, err := MatchTriple(mappings, queries)
	if err != nil {
		t.Fatalf("MatchTriple(%s) error: %s", queryStr, err)
	}
	if diff := deep.Equal(wantResult, gotResult); diff != nil {
		t.Errorf("MatchTriple(%s), unexpected result diff %v", queryStr, diff)
	}
}

func TestGetFuncDeps(t *testing.T) {
	db := "dc_v3"
	mappings := readTestMapping(t, []string{"test_mapping.mcf"})

	funcDeps, err := GetFuncDeps(mappings)
	if err != nil {
		t.Fatalf("GetFuncDeps error: %s", err)
	}

	wantStr := map[string]map[string]string{
		"E:ACLGroup->E1": map[string]string{
			"dcid": "C:ACLGroup->id",
		},
		"E:ACLGroup->E2": map[string]string{
			"dcid": "C:ACLGroup->prov_id",
		},
	}
	for e, p2c := range wantStr {
		entity, err := base.NewEntity(e, db)
		if err != nil {
			t.Errorf("Bad input entity string: %v, %v", e, err)
			continue
		}
		gotCs, ok := funcDeps[*entity]
		if !ok {
			t.Errorf("GetFuncDeps should contains value for %v", entity)
			continue
		}
		wantCs := map[string]interface{}{}
		for p, c := range p2c {
			col, err := base.NewColumn(c, db)
			if err != nil {
				t.Errorf("Bad input column string: %v, %v", c, err)
				continue
			}
			wantCs[p] = *col
		}
		if diff := deep.Equal(wantCs, gotCs); diff != nil {
			t.Errorf("GetFuncDeps unexpected result diff %v", diff)
		}
	}
}

func TestGetFuncDepsWithEntity(t *testing.T) {
	db := "dc_v3"
	mappings := readTestMapping(t, []string{"oi_county_mapping.mcf"})

	funcDeps, err := GetFuncDeps(mappings)
	if err != nil {
		t.Fatalf("GetFuncDeps error: %s", err)
	}

	wantStr := map[string]map[string]string{
		"E:bq_county_outcomes->E1": map[string]string{
			"geoId": "C:bq_county_outcomes->geo_id",
		},
		"E:bq_county_outcomes->E2": map[string]string{
			"location":   "E:bq_county_outcomes->E1",
			"outcome":    "C:bq_county_outcomes->outcome",
			"race":       "C:bq_county_outcomes->race",
			"gender":     "C:bq_county_outcomes->gender",
			"percentile": "C:bq_county_outcomes->percentile",
		},
		"E:bq_county_outcomes->E3": map[string]string{
			"observedNode":     "E:bq_county_outcomes->E2",
			"measuredProperty": "C:bq_county_outcomes->measured_property",
		},
	}
	for e, p2i := range wantStr {
		entity, err := base.NewEntity(e, db)
		if err != nil {
			t.Errorf("Bad input entity string: %v, %v", e, err)
			continue
		}
		gotCs, ok := funcDeps[*entity]
		if !ok {
			t.Errorf("GetFuncDeps should contains value for %v", entity)
			continue
		}
		wantCs := map[string]interface{}{}
		for p, i := range p2i {
			col, err := base.NewColumn(i, db)
			if err != nil {
				ent, err := base.NewEntity(i, db)
				if err != nil {
					t.Errorf("Bad input string: %v, %v", i, err)
					continue
				} else {
					wantCs[p] = *ent
				}
			} else {
				wantCs[p] = *col
			}
		}
		if diff := deep.Equal(wantCs, gotCs); diff != nil {
			t.Errorf("GetFuncDeps unexpected result diff %v", diff)
		}
	}
}

func TestGetProvColumn(t *testing.T) {
	mappings := readTestMapping(t, []string{"test_mapping.mcf"})
	wantInfo := map[string]string{
		"`dc_v3.ACLGroup`":              "C:ACLGroup->prov_id",
		"`dc_v3.Curator`":               "C:Curator->prov_id",
		"`dc_v3.Instance`":              "C:Instance->prov_id",
		"`dc_v3.Observation`":           "C:Observation->prov_id",
		"`dc_v3.Place`":                 "C:Place->prov_id",
		"`dc_v3.Source`":                "C:Source->prov_id",
		"`dc_v3.StatisticalPopulation`": "C:StatisticalPopulation->prov_id",
		"`dc_v3.Triple`":                "C:Triple->prov_id",
		"`dc_v3.Provenance`":            "C:Provenance->prov_id",
		"`dc_v3.MonthlyWeather`":        "C:MonthlyWeather->prov_id",
		"`dc_v3.PlaceExt`":              "C:PlaceExt->prov_id",
	}

	wantResult := map[string]base.Column{}
	for key, value := range wantInfo {
		col, _ := base.NewColumn(value, "dc_v3")
		wantResult[key] = *col
	}

	gotResult, err := GetProvColumn(mappings)
	if err != nil {
		t.Fatalf("GetProvColumn got error: %v", err)
	}
	if diff := deep.Equal(wantResult, gotResult); diff != nil {
		t.Errorf("GetProvColumn unexpected result diff %v", diff)
	}
}

func TestRewriteQuery(t *testing.T) {
	subTypeMap, err := GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	a1 := "SELECT ?node, typeOf ?node Observation, name ?node ?name"
	b1 := "SELECT ?node, typeOf ?node Observation, name ?node ?name"
	a2 := "SELECT ?node, typeOf ?node City, name ?node ?name"
	b2 := "SELECT ?node, typeOf ?node Place, name ?node ?name, subType ?node City"
	a3 := "SELECT ?node, typeOf ?node City, typeOf ?state Place, " +
		"subType ?state State,name ?node ?name"
	b3 := "SELECT ?node, typeOf ?node Place, typeOf ?state Place, " +
		"subType ?state State,name ?node ?name, subType ?node City"
	queryInfo := map[string]string{
		a1: b1,
		a2: b2,
		a3: b3,
	}
	for a, b := range queryInfo {
		_, query, err := ParseQuery(a)
		if err != nil {
			t.Errorf("ParseQuery got error: %v", err)
			continue
		}
		_, wantResult, err := ParseQuery(b)
		if err != nil {
			t.Errorf("ParseQuery got error: %v", err)
			continue
		}
		gotResult := RewriteQuery(query, subTypeMap)
		if diff := deep.Equal(wantResult, gotResult); diff != nil {
			t.Errorf("RewriteQuery unexpected result diff %v", diff)
		}
	}
}

func TestGetOutArcInfo(t *testing.T) {
	mappings := readTestMapping(t, []string{"test_mapping.mcf"})
	wantOutArcInfo := map[string][]OutArcInfo{
		"`dc_v3.Place`": []OutArcInfo{
			OutArcInfo{Pred: "name", Column: "name"},
			OutArcInfo{Pred: "alternateName", Column: "alternate_name"},
			OutArcInfo{Pred: "timezone", Column: "timezone"},
			OutArcInfo{Pred: "landArea", Column: "land_area", IsNode: true},
			OutArcInfo{Pred: "waterArea", Column: "water_area", IsNode: true},
			OutArcInfo{Pred: "latitude", Column: "latitude"},
			OutArcInfo{Pred: "longitude", Column: "longitude"},
			OutArcInfo{Pred: "elevation", Column: "elevation"},
			OutArcInfo{Pred: "stateCode", Column: "state_code"},
			OutArcInfo{Pred: "countryAlpha2Code", Column: "country_alpha_2_code"},
			OutArcInfo{Pred: "countryAlpha3Code", Column: "country_alpha_3_code"},
			OutArcInfo{Pred: "countryNumericCode", Column: "country_numeric_code"},
		},
		"`dc_v3.PlaceExt`": []OutArcInfo{
			OutArcInfo{Pred: "kmlCoordinates", Column: "kml_coordinates"},
		},
	}
	gotOutArcInfo, err := GetOutArcInfo(mappings, "Place")
	if err != nil {
		t.Errorf("GetOutArcInfo got error: %v", err)
	}
	if diff := deep.Equal(wantOutArcInfo, gotOutArcInfo); diff != nil {
		t.Errorf("GetOutArcInfo, unexpected result diff %v", diff)
	}
}
