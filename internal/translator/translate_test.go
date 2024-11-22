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
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/datacommonsorg/mixer/internal/translator/datalog"
	"github.com/datacommonsorg/mixer/internal/translator/solver"
	"github.com/datacommonsorg/mixer/internal/translator/sparql"
	"github.com/datacommonsorg/mixer/internal/translator/testutil"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/google/go-cmp/cmp"
)

func TestBind(t *testing.T) {
	db := "dc_v3"
	mappings := testutil.ReadTestMapping(t, []string{"testdata/test_mapping.mcf"})
	mappings = solver.PruneMapping(mappings)
	queryStr := `
		SELECT ?name ?timezone ?landArea ?parent_dcid ?parent_name,
		typeOf ?parent Place,
		typeOf ?node Place,
		subType ?node City,
		dcid ?node dc/1234 dc/4321,
		timezone ?node ?timezone,
		containedInPlace ?node ?parent,
		dcid ?parent ?parent_dcid,
		name ?parent ?parent_name,
		name ?node ?name,
		landArea ?node ?landArea
	`
	_, queries, err := datalog.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("parsing query string %s: %s", queryStr, err)
	}

	wantResult := map[string][]*types.Mapping{}
	v, _ := types.NewMapping("typeOf", "E:Place->E1", "Place", db)
	wantResult[fmt.Sprintf("%s", queries[0])] = []*types.Mapping{v}
	wantResult[fmt.Sprintf("%s", queries[1])] = []*types.Mapping{v}
	v, _ = types.NewMapping("subType", "E:Place->E1", "C:Place->type", db)
	wantResult[fmt.Sprintf("%s", queries[2])] = []*types.Mapping{v}
	v, _ = types.NewMapping("dcid", "E:Place->E1", "C:Place->id", db)
	wantResult[fmt.Sprintf("%s", queries[3])] = []*types.Mapping{v}
	v, _ = types.NewMapping("timezone", "E:Place->E1", "C:Place->timezone", db)
	wantResult[fmt.Sprintf("%s", queries[4])] = []*types.Mapping{v}
	v, _ = types.NewMapping("C:Triple->predicate", "E:Triple->E1", "C:Triple->object_value", db)
	wantResult[fmt.Sprintf("%s", queries[5])] = []*types.Mapping{v}
	v, _ = types.NewMapping("dcid", "E:Place->E1", "C:Place->id", db)
	wantResult[fmt.Sprintf("%s", queries[6])] = []*types.Mapping{v}
	v, _ = types.NewMapping("name", "E:Place->E1", "C:Place->name", db)
	wantResult[fmt.Sprintf("%s", queries[7])] = []*types.Mapping{v}
	wantResult[fmt.Sprintf("%s", queries[8])] = []*types.Mapping{v}
	v, _ = types.NewMapping("landArea", "E:Place->E1", "E:Place->E3", db)
	wantResult[fmt.Sprintf("%s", queries[9])] = []*types.Mapping{v}

	bindingMap, err := Bind(mappings, queries)
	gotResult := map[string][]*types.Mapping{}
	for q, ms := range bindingMap {
		gotResult[fmt.Sprintf("%s", q)] = ms
	}
	if err != nil {
		t.Fatalf("bind datalog query %s: %s", queryStr, err)
	}
	if diff := cmp.Diff(wantResult, gotResult); diff != "" {
		t.Errorf("Bind(%s), unexpected result diff %v", queryStr, diff)
	}
}

func TestGetBindings(t *testing.T) {
	qToM := map[string][]string{
		"typeOf ?a City": {"typeOf E:Place->E1 Place"},
		"name ?a MTV": {
			"name E:Place->E1 C:Place->name",
			"name E:Instance->E1 C:Instance->name",
		},
		"dcid ?a x123": {
			"name E:Place->E1 C:Place->id",
			"name E:Instance->E1 C:Instance->id",
		},
	}
	bindingMap := make(map[*types.Query][]*types.Mapping)
	for qStr, mStrList := range qToM {
		qTokens := strings.Split(qStr, " ")
		q := types.NewQuery(qTokens[0], qTokens[1], qTokens[2])
		for _, mStr := range mStrList {
			mTokens := strings.Split(mStr, " ")
			m, err := types.NewMapping(mTokens[0], mTokens[1], mTokens[2], "dc")
			if err != nil {
				t.Fatalf("Invalid input %s: %s", mStr, err)
			}
			bindingMap[q] = append(bindingMap[q], m)
		}
	}
	bindings := getBindingSets(bindingMap)
	if len(bindings) != 4 {
		t.Errorf("getBindingSets expects 4 bindings, got %v instead", len(bindings))
	}
}

func TestGetGraph(t *testing.T) {
	q2m := map[[3]string][3]string{
		{"dcid", "?p", "?dcid"}: {
			"dcid", "E:Place->E1", "C:Place->id"},
		{"typeOf", "?p", "Place"}: {
			"typeOf", "E:Place->E1", "Place"},
		{"subType", "?p", "City"}: {
			"subType", "E:Place->E1", "C:Place->type"},
		{"name", "?p", "San Jose"}: {
			"name", "E:Place->E1", "C:Place->name"},
	}
	bindings := []Binding{}
	for q, m := range q2m {
		query := types.NewQuery(q[0], q[1], q[2])
		mapping, err := types.NewMapping(m[0], m[1], m[2], "dc")
		if err != nil {
			t.Fatalf("Invalid mapping input %s: %s", m, err)
		}
		bindings = append(bindings, Binding{query, mapping})
	}

	queryID := map[*types.Query]int{
		bindings[0].Query: 0,
		bindings[1].Query: 0,
		bindings[2].Query: 0,
		bindings[3].Query: 0,
	}

	gotResult := getGraph(bindings, queryID, map[types.Node]struct{}{})
	if len(gotResult) != 8 {
		t.Errorf("gotResult expects 8 keys, got %d instead", len(gotResult))
	}
}

func TestGetConstraint(t *testing.T) {
	db := "dc_v3"
	mappings := testutil.ReadTestMapping(t, []string{"testdata/test_mapping.mcf"})
	graph := Graph{}
	e, err := types.NewEntity("E:Place->E1", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	n1 := types.NewNode("?p")
	graph[*e] = map[interface{}]struct{}{n1: {}}
	graph[n1] = map[interface{}]struct{}{*e: {}}

	c1, err := types.NewColumn("C:Place->name", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	graph[*c1] = map[interface{}]struct{}{"MTV": {}}
	graph["MTV"] = map[interface{}]struct{}{*c1: {}}

	c2, err := types.NewColumn("C:Place->type", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	graph[*c2] = map[interface{}]struct{}{"City": {}}
	graph["City"] = map[interface{}]struct{}{*c2: {}}

	n2 := types.NewNode("?dcid")
	c3, err := types.NewColumn("C:Place->id", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	graph[n2] = map[interface{}]struct{}{*c3: {}}
	graph[*c3] = map[interface{}]struct{}{n2: {}}

	funcDeps, err := solver.GetFuncDeps(mappings)
	if err != nil {
		t.Fatalf("GetFuncDeps error: %s", err)
	}

	gotConstraints, _, _ := GetConstraint(graph, funcDeps)
	wantConstraints := map[Constraint]struct{}{
		{*c1, "MTV"}:  {},
		{*c2, "City"}: {},
		{*c3, n1}:     {},
		{*c3, n2}:     {},
	}
	for _, con := range gotConstraints {
		if _, ok := wantConstraints[con]; !ok {
			t.Errorf("getConstraint unexpected constraint %v", con)
			continue
		}
	}
}

func TestGetSQL(t *testing.T) {
	db := "dc_v3"
	p, err := types.NewColumn("C:Place->prov_id", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	tableProv := map[string]types.Column{"`dc_v3.Place`": *p}

	n1 := types.NewNode("?p")
	c1, err := types.NewColumn("C:Place->name", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	c2, err := types.NewColumn("C:Place->type", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	n2 := types.NewNode("?dcid")
	c3, err := types.NewColumn("C:Place->id", db)
	if err != nil {
		t.Fatalf("Invalid input %s", err)
	}
	constraints := []Constraint{
		{*c3, n2}, {*c3, n1}, {*c2, "City"}, {*c1, "MTV"},
	}
	got, _, err := getSQL(
		[]types.Node{n2},
		constraints,
		map[types.Node]string{},
		ProvInfo{true, tableProv},
		&types.QueryOptions{Limit: 20, Distinct: true, Orderby: "?dcid", ASC: true},
	)
	if err != nil {
		t.Fatalf("getSQL error: %s", err)
	}
	wantSQL :=
		"SELECT DISTINCT _dc_v3_Place_.id AS dcid,\n" +
			"_dc_v3_Place_.prov_id AS prov0\n" +
			"FROM `dc_v3.Place` AS _dc_v3_Place_\n" +
			"WHERE _dc_v3_Place_.name = @param0\n" +
			"AND _dc_v3_Place_.type = @param1\n" +
			"ORDER BY dcid ASC\n" +
			"LIMIT 20\n"
	wantParams := []bigquery.QueryParameter{
		{Name: "param0", Value: "MTV"},
		{Name: "param1", Value: "City"},
	}
	if diff := cmp.Diff(wantSQL, got.SQL); diff != "" {
		t.Errorf("getSQL unexpected got sql diff %v", diff)
	}
	if diff := cmp.Diff(wantParams, got.Params); diff != "" {
		t.Errorf("getSQL unexpected got params diff %v", diff)
	}
}

func TestTranslate(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	emptyProv := map[int][]int{}
	mappings := testutil.ReadTestMapping(t, []string{"testdata/test_mapping.mcf"})
	for _, c := range []struct {
		name     string
		askProv  bool
		queryStr string
		wantSQL  string
		wantProv map[int][]int
	}{
		{
			"OneVar",
			false,
			`SELECT ?dcid/test,
		    typeOf ?p Place,
		    subType ?p "City",
		    name ?p "San Jose",
		    dcid ?p ?dcid/test`,

			"SELECT _dc_v3_Place_0.id AS dcid_test\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"WHERE _dc_v3_Place_0.name = \"San Jose\"\n" +
				"AND _dc_v3_Place_0.type = \"City\"\n",
			emptyProv,
		},
		{
			"InstanceQueryFipsIdContainedIn",
			false,
			`SELECT ?dcid,
				typeOf ?parent_node Place,
				typeOf ?node Place,
				subType ?node City,
				countryAlpha2Code ?node "country-code",
				containedInPlace ?node ?parent_node,
				dcid ?parent_node "dc/x333",
				dcid ?node ?dcid`,

			"SELECT _dc_v3_Place_1.id AS dcid\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_1\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"ON _dc_v3_Place_1.id = _dc_v3_Triple_0.subject_id\n" +
				"WHERE _dc_v3_Place_1.country_alpha_2_code = \"country-code\"\n" +
				"AND _dc_v3_Place_1.type = \"City\"\n" +
				"AND _dc_v3_Triple_0.object_id = \"dc/x333\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"containedInPlace\"\n",
			emptyProv,
		},

		{
			"InstanceQueryType",
			true,
			`SELECT ?node_type,
			  typeOf ?node Thing,
			  dcid ?node "dc/m1rl3k",
			  subType ?node ?node_type`,

			"SELECT _dc_v3_Instance_0.type AS node_type,\n" +
				"_dc_v3_Instance_0.prov_id AS prov0\n" +
				"FROM `dc_v3.Instance` AS _dc_v3_Instance_0\n" +
				"WHERE _dc_v3_Instance_0.id = \"dc/m1rl3k\"\n",
			map[int][]int{1: {0}},
		},
		{
			"InstanceQueryByType",
			false,
			`SELECT ?dcid,
				typeOf ?node Place,
				subType ?node City,
				dcid ?node ?dcid`,
			"SELECT _dc_v3_Place_0.id AS dcid\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"WHERE _dc_v3_Place_0.type = \"City\"\n",
			emptyProv,
		},
		{
			"InstanceQueryField",
			false,
			`SELECT ?name,
				typeOf ?node Place,
				subType ?node City,
				dcid ?node dc/qp620l2,
				name ?node ?name`,

			"SELECT _dc_v3_Place_0.name AS name\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"WHERE _dc_v3_Place_0.id = \"dc/qp620l2\"\n" +
				"AND _dc_v3_Place_0.type = \"City\"\n",
			emptyProv,
		},
		{
			"ContainedInPlace",
			false,
			`SELECT ?name ?dcid,
				typeOf ?node Place,
				typeOf ?city_or_county Place,
				subType ?node State,
				containedInPlace ?city_or_county ?node,
				dcid ?node dc/b72vdv,
				name ?city_or_county ?name,
				dcid ?city_or_county ?dcid`,

			"SELECT _dc_v3_Place_1.name AS name,\n" +
				"_dc_v3_Place_1.id AS dcid\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Place` AS _dc_v3_Place_1\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Place_1.id\n" +
				"WHERE _dc_v3_Triple_0.object_id = \"dc/b72vdv\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"containedInPlace\"\n",
			emptyProv,
		},
		{
			"ComplexQuery",
			true,
			`SELECT ?name ?timezone ?landArea ?parent_dcid ?parent_name,
				typeOf ?parent Place,
				typeOf ?node Place,
				subType ?node City,
				dcid ?node dc/1234 dc/4321,
				timezone ?node ?timezone,
				containedInPlace ?node ?parent,
				dcid ?parent ?parent_dcid,
				name ?parent ?parent_name,
				name ?node ?name,
				landArea ?node ?landArea`,

			"SELECT _dc_v3_Place_1.name AS name,\n" +
				"_dc_v3_Place_1.timezone AS timezone,\n" +
				"_dc_v3_Place_1.land_area AS landArea,\n" +
				"_dc_v3_Place_0.id AS parent_dcid,\n" +
				"_dc_v3_Place_0.name AS parent_name,\n" +
				"_dc_v3_Place_1.prov_id AS prov0,\n" +
				"_dc_v3_Place_0.prov_id AS prov1\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_1\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"ON _dc_v3_Place_1.id = _dc_v3_Triple_0.subject_id\n" +
				"JOIN `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"ON _dc_v3_Triple_0.object_id = _dc_v3_Place_0.id\n" +
				"WHERE _dc_v3_Place_1.id IN (\"dc/1234\", \"dc/4321\")\n" +
				"AND _dc_v3_Place_1.type = \"City\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"containedInPlace\"\n",
			map[int][]int{5: {0, 1, 2}, 6: {3, 4}},
		},

		{
			"MultipleTypeQuery",
			false,
			`SELECT ?dcid ?population_dcid,
				typeOf ?state Place,
				subType ?state State,
				typeOf ?population StatisticalPopulation,
				dcid ?state dc/p/x1234,
				dcid ?state ?dcid,
				location ?population ?state,
				dcid ?population ?population_dcid`,

			"SELECT _dc_v3_StatisticalPopulation_1.place_key AS dcid,\n" +
				"_dc_v3_StatisticalPopulation_1.id AS population_dcid\n" +
				"FROM `dc_v3.StatisticalPopulation` AS _dc_v3_StatisticalPopulation_1\n" +
				"WHERE _dc_v3_StatisticalPopulation_1.place_key = \"dc/p/x1234\"\n",
			emptyProv,
		},
		{
			"FavorInnerJoinBinding",
			false,
			`SELECT ?dcid ?count_value,
			  typeOf ?node Place,
			  subType ?node County,
				typeOf ?pop StatisticalPopulation,
				typeOf ?o Observation,
				dcid ?node X1234,
				dcid ?node ?dcid,
				location ?pop ?node,
				populationType ?pop Person,
				observedNode ?o ?pop,
				measuredValue ?o ?count_value`,

			"SELECT _dc_v3_StatisticalPopulation_1.place_key AS dcid,\n" +
				"_dc_v3_Observation_2.measured_value AS count_value\n" +
				"FROM `dc_v3.StatisticalPopulation` AS _dc_v3_StatisticalPopulation_1\n" +
				"JOIN `dc_v3.Observation` AS _dc_v3_Observation_2\n" +
				"ON _dc_v3_StatisticalPopulation_1.id = _dc_v3_Observation_2.observed_node_key\n" +
				"WHERE _dc_v3_StatisticalPopulation_1.place_key = \"X1234\"\n" +
				"AND _dc_v3_StatisticalPopulation_1.population_type = \"Person\"\n",
			emptyProv,
		},
		{
			"BrowserPopulationQuery",
			false,
			`SELECT ?dcid,
				typeOf ?child Place,
				subType ?child City,
				typeOf ?parent StatisticalPopulation,
				dcid ?child dc/m1rl3k,
				dcid ?parent ?dcid,
				location ?parent ?child`,

			"SELECT _dc_v3_StatisticalPopulation_1.id AS dcid\n" +
				"FROM `dc_v3.StatisticalPopulation` AS _dc_v3_StatisticalPopulation_1\n" +
				"WHERE _dc_v3_StatisticalPopulation_1.place_key = \"dc/m1rl3k\"\n",
			emptyProv,
		},
		{
			"CollegeObservation",
			false,
			`SELECT ?dcid,
				typeOf ?o StatVarObservation,
				dcid ?place dc/zkelys3,
				dcid ?o ?dcid,
				observationAbout ?o ?place`,

			"SELECT _dc_v3_StatVarObservation_0.id AS dcid\n" +
				"FROM `dc_v3.StatVarObservation` AS _dc_v3_StatVarObservation_0\n" +
				"WHERE _dc_v3_StatVarObservation_0.observation_about = \"dc/zkelys3\"\n",
			emptyProv,
		},

		{
			"Triple",
			false,
			`SELECT ?datePublished ?author_name,
				typeOf ?node ClaimReview,
				typeOf ?author Organization,
				datePublished ?node ?datePublished,
				author ?node ?author,
				name ?author ?author_name,
				dcid ?node dc/4568bbd63cjdg`,

			"SELECT _dc_v3_Triple_2.object_value AS datePublished,\n" +
				"_dc_v3_Triple_4.object_value AS author_name\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_2\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_2.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_3\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_3.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"ON _dc_v3_Triple_3.object_id = _dc_v3_Triple_1.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_4\n" +
				"ON _dc_v3_Triple_1.subject_id = _dc_v3_Triple_4.subject_id\n" +
				"WHERE _dc_v3_Triple_0.object_id = \"ClaimReview\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"typeOf\"\n" +
				"AND _dc_v3_Triple_0.subject_id = \"dc/4568bbd63cjdg\"\n" +
				"AND _dc_v3_Triple_1.object_id = \"Organization\"\n" +
				"AND _dc_v3_Triple_1.predicate = \"typeOf\"\n" +
				"AND _dc_v3_Triple_2.predicate = \"datePublished\"\n" +
				"AND _dc_v3_Triple_3.predicate = \"author\"\n" +
				"AND _dc_v3_Triple_4.predicate = \"name\"\n",
			emptyProv,
		},
		{
			"ResolutionQuery",
			false,
			`SELECT ?dcid,
				typeOf ?parent Place,
				typeOf ?node Place,
				subType ?node City,
				dcid ?node ?dcid,
				containedInPlace ?node ?parent,
				dcid ?parent dc/zxvc6e2,
				geoId ?node 12345`,

			"SELECT _dc_v3_Place_1.id AS dcid\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Place` AS _dc_v3_Place_1\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Place_1.id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"ON _dc_v3_Place_1.id = _dc_v3_Triple_1.subject_id\n" +
				"WHERE _dc_v3_Place_1.type = \"City\"\n" +
				"AND _dc_v3_Triple_0.object_id = \"dc/zxvc6e2\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"containedInPlace\"\n" +
				"AND _dc_v3_Triple_1.object_value = \"12345\"\n" +
				"AND _dc_v3_Triple_1.predicate = \"geoId\"\n",
			emptyProv,
		},
		{
			"ClassLabel",
			false,
			`SELECT ?v,
				typeOf ?o Class,
				dcid ?o ListenAction,
				label ?o ?v`,
			"SELECT _dc_v3_Triple_2.object_value AS v\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_2\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_2.subject_id\n" +
				"WHERE _dc_v3_Triple_0.object_id = \"Class\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"typeOf\"\n" +
				"AND _dc_v3_Triple_0.subject_id = \"ListenAction\"\n" +
				"AND _dc_v3_Triple_2.predicate = \"label\"\n",
			emptyProv,
		},
		{
			"LocalCuratorIdSet",
			false,
			`SELECT ?dcid ?local_id,
				dcid ?node ?dcid,
				localCuratorLevelId ?node B01001 B022202,
				localCuratorLevelId ?node ?local_id`,

			"SELECT _dc_v3_Triple_0.subject_id AS dcid,\n" +
				"_dc_v3_Triple_1.object_value AS local_id\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"ON _dc_v3_Triple_1.subject_id = _dc_v3_Triple_0.subject_id\n" +
				"WHERE _dc_v3_Triple_1.object_value IN (\"B01001\", \"B022202\")\n" +
				"AND _dc_v3_Triple_1.predicate = \"localCuratorLevelId\"\n",
			emptyProv,
		},
		{
			"CollegeOrUniversity",
			false,
			`SELECT ?dcid,
				typeOf ?child Place,
				subType ?child City,
				typeOf ?parent CollegeOrUniversity,
				dcid ?child dc/m1rl3k,
				dcid ?parent ?dcid,
				location ?parent ?child`,

			"SELECT _dc_v3_Triple_1.subject_id AS dcid\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_2\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_2.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_1.subject_id\n" +
				"WHERE _dc_v3_Triple_0.object_id = \"CollegeOrUniversity\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"typeOf\"\n" +
				"AND _dc_v3_Triple_2.object_id = \"dc/m1rl3k\"\n" +
				"AND _dc_v3_Triple_2.predicate = \"location\"\n",

			emptyProv,
		},
		{
			"ObservationStatVarProp",
			false,
			`SELECT ?measuredProperty ?statType,
				typeOf ?o StatVarObservation,
				typeOf ?sv StatisticalVariable,
				dcid ?o dc/o/xyz,
				variableMeasured ?o ?sv,
				measuredProperty ?sv ?measuredProperty,
				statType ?sv ?statType`,

			"SELECT _dc_v3_StatisticalVariable_1.measured_prop AS measuredProperty,\n" +
				"_dc_v3_StatisticalVariable_1.stat_type AS statType\n" +
				"FROM `dc_v3.StatVarObservation` AS _dc_v3_StatVarObservation_0\n" +
				"JOIN `dc_v3.StatisticalVariable` AS _dc_v3_StatisticalVariable_1\n" +
				"ON _dc_v3_StatVarObservation_0.variable_measured = _dc_v3_StatisticalVariable_1.id\n" +
				"WHERE _dc_v3_StatVarObservation_0.id = \"dc/o/xyz\"\n",
			emptyProv,
		},
		{
			"ObservationCommuteZone",
			false,
			`SELECT ?parentDcid ?dcid ?measuredProperty,
				typeOf ?node CommutingZone,
				dcid ?node dc/p/zcerrzm76y0bh,
				dcid ?node ?parentDcid,
				typeOf ?o Observation,
				observedNode ?o ?node,
				dcid ?o ?dcid,
				measuredProperty ?o ?measuredProperty`,

			"SELECT _dc_v3_Observation_1.observed_node_key AS parentDcid,\n" +
				"_dc_v3_Observation_1.id AS dcid,\n" +
				"_dc_v3_Observation_1.measured_prop AS measuredProperty\n" +
				"FROM `dc_v3.Observation` AS _dc_v3_Observation_1\n" +
				"WHERE _dc_v3_Observation_1.observed_node_key = \"dc/p/zcerrzm76y0bh\"\n",
			emptyProv,
		},
		{
			"StateCountyPopObs",
			false,
			`SELECT ?countyDcid ?countyName ?hasMom,
        typeOf ?state State,
				typeOf ?county County,
				dcid ?state dc/y5gtcw1,
				containedInPlace ?county ?state,
				dcid ?county ?countyDcid,
				name ?county ?countyName,
				typeOf ?sv StatisticalVariable,
				observationAbout ?sv ?county,
				p1 ?sv gender,
				v1 ?sv Male,
				p2 ?sv parentIncome,
				v2 ?sv Percentile10,
				numConstraints ?sv 2,
				measuredProperty ?sv opportunity_atlas_has_mom,
				typeOf ?obs StatVarObservation,
				value ?obs ?hasMom,
				observationPeriod ?obs P1Y`,

			"SELECT _dc_v3_Place_1.id AS countyDcid,\n" +
				"_dc_v3_Place_1.name AS countyName,\n" +
				"_dc_v3_StatVarObservation_3.value AS hasMom\n" +
				"FROM `dc_v3.StatisticalVariable` AS _dc_v3_StatisticalVariable_2\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"ON _dc_v3_StatisticalVariable_2.id = _dc_v3_Triple_1.subject_id\n" +
				"JOIN `dc_v3.Place` AS _dc_v3_Place_1\n" +
				"ON _dc_v3_Triple_1.object_id = _dc_v3_Place_1.id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"ON _dc_v3_Place_1.id = _dc_v3_Triple_0.subject_id\n" +
				"WHERE _dc_v3_Place_1.type = \"County\"\n" +
				"AND _dc_v3_StatVarObservation_3.observation_period = \"P1Y\"\n" +
				"AND _dc_v3_StatisticalVariable_2.measured_prop = \"opportunity_atlas_has_mom\"\n" +
				"AND _dc_v3_StatisticalVariable_2.num_constraints = 2\n" +
				"AND _dc_v3_StatisticalVariable_2.p1 = \"gender\"\n" +
				"AND _dc_v3_StatisticalVariable_2.p2 = \"parentIncome\"\n" +
				"AND _dc_v3_StatisticalVariable_2.v1 = \"Male\"\n" +
				"AND _dc_v3_StatisticalVariable_2.v2 = \"Percentile10\"\n" +
				"AND _dc_v3_Triple_0.object_id = \"dc/y5gtcw1\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"containedInPlace\"\n" +
				"AND _dc_v3_Triple_1.predicate = \"observationAbout\"\n",
			emptyProv,
		},
		{
			"ProvenancePointQuery",
			true,
			`SELECT ?dcid ?name ?curator ?aclGroup ?source ?url ?importUrl ?importTime ?importDuration,
				typeOf ?node Provenance,
				dcid ?node dc/8eednm2,
				dcid ?node ?dcid,
				name ?node ?name,
				curator ?node ?curator,
				aclGroup ?node ?aclGroup,
				source ?node ?source,
				url ?node ?url,
				importUrl ?node ?importUrl,
				importTime ?node ?importTime,
				importDuration ?node ?importDuration`,

			"SELECT _dc_v3_Provenance_0.id AS dcid,\n" +
				"_dc_v3_Provenance_0.name AS name,\n" +
				"_dc_v3_Provenance_0.curator AS curator,\n" +
				"_dc_v3_Provenance_0.acl_group AS aclGroup,\n" +
				"_dc_v3_Provenance_0.source AS source,\n" +
				"_dc_v3_Provenance_0.provenance_url AS url,\n" +
				"_dc_v3_Provenance_0.mcf_url AS importUrl,\n" +
				"_dc_v3_Provenance_0.timestamp_secs AS importTime,\n" +
				"_dc_v3_Provenance_0.duration_secs AS importDuration,\n" +
				"_dc_v3_Provenance_0.prov_id AS prov0\n" +
				"FROM `dc_v3.Provenance` AS _dc_v3_Provenance_0\n" +
				"WHERE _dc_v3_Provenance_0.id = \"dc/8eednm2\"\n",
			map[int][]int{9: {0, 1, 2, 3, 4, 5, 6, 7, 8}},
		},
		{
			"Encode",
			false,
			`SELECT ?experimentDcid,
				typeOf ?experiment EncodeExperiment,
				typeOf ?biosample BiosampleType,
				biosampleOntology ?experiment ?biosample,
				classification ?biosample "primary cell",
				termName ?biosample "keratinocyte",
				dcid ?experiment ?experimentDcid`,

			"SELECT _dc_v3_Triple_5.subject_id AS experimentDcid\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_2\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_2.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_5\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_5.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"ON _dc_v3_Triple_2.object_id = _dc_v3_Triple_1.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_4\n" +
				"ON _dc_v3_Triple_1.subject_id = _dc_v3_Triple_4.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_3\n" +
				"ON _dc_v3_Triple_1.subject_id = _dc_v3_Triple_3.subject_id\n" +
				"WHERE _dc_v3_Triple_0.object_id = \"EncodeExperiment\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"typeOf\"\n" +
				"AND _dc_v3_Triple_1.object_id = \"BiosampleType\"\n" +
				"AND _dc_v3_Triple_1.predicate = \"typeOf\"\n" +
				"AND _dc_v3_Triple_2.predicate = \"biosampleOntology\"\n" +
				"AND _dc_v3_Triple_3.object_value = \"primary cell\"\n" +
				"AND _dc_v3_Triple_3.predicate = \"classification\"\n" +
				"AND _dc_v3_Triple_4.object_value = \"keratinocyte\"\n" +
				"AND _dc_v3_Triple_4.predicate = \"termName\"\n",
			emptyProv,
		},
		{
			"MultipleObjs",
			false,
			`SELECT ?experiment ?bedFileNode,
				dcid ?experimentNode dc/abc dc/xyz,
				dcid ?experimentNode ?experiment,
				experiment ?bedFileNode ?experimentNode`,

			"SELECT _dc_v3_Triple_1.object_id AS experiment,\n" +
				"_dc_v3_Triple_1.subject_id AS bedFileNode\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"WHERE _dc_v3_Triple_1.object_id IN (\"dc/abc\", \"dc/xyz\")\n" +
				"AND _dc_v3_Triple_1.predicate = \"experiment\"\n",
			emptyProv,
		},
	} {
		options := types.QueryOptions{Prov: c.askProv}
		nodes, queries, err := datalog.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap, &options)
		if err != nil {
			t.Errorf("getSQL error: %s", err)
			continue
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
		if diff := cmp.Diff(c.wantProv, translation.Prov); diff != "" {
			t.Errorf("getSQL unexpected prov diff for test %s, %v", c.name, diff)
		}
	}
}

func TestDcidSimplified(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	emptyProv := map[int][]int{}
	mappings := testutil.ReadTestMapping(t, []string{"testdata/test_mapping.mcf"})
	for _, c := range []struct {
		name     string
		askProv  bool
		queryStr string
		wantSQL  string
		wantProv map[int][]int
	}{
		{
			"OneVar",
			false,
			`SELECT ?p,
		    typeOf ?p Place,
		    subType ?p "City",
		    name ?p "San Jose"`,

			"SELECT _dc_v3_Place_0.id AS p\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"WHERE _dc_v3_Place_0.name = \"San Jose\"\n" +
				"AND _dc_v3_Place_0.type = \"City\"\n",
			emptyProv,
		},
		{
			"InstanceQueryFipsIdContainedIn",
			false,
			`SELECT ?node,
				typeOf ?node Place,
				subType ?node City,
				countryAlpha2Code ?node "alpha-code",
				containedInPlace ?node ?parent_node,
				dcid ?parent_node "dc/x333"`,

			"SELECT _dc_v3_Place_0.id AS node\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"ON _dc_v3_Place_0.id = _dc_v3_Triple_0.subject_id\n" +
				"WHERE _dc_v3_Place_0.country_alpha_2_code = \"alpha-code\"\n" +
				"AND _dc_v3_Place_0.type = \"City\"\n" +
				"AND _dc_v3_Triple_0.object_id = \"dc/x333\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"containedInPlace\"\n",
			emptyProv,
		},
		{
			"InstanceQueryByType",
			false,
			`SELECT ?node,
				typeOf ?node Place,
				subType ?node City`,
			"SELECT _dc_v3_Place_0.id AS node\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"WHERE _dc_v3_Place_0.type = \"City\"\n",
			emptyProv,
		},
		{
			"ContainedInPlace",
			false,
			`SELECT ?name ?city_or_county,
				typeOf ?city_or_county Place,
				containedInPlace ?city_or_county dc/b72vdv,
				name ?city_or_county ?name`,

			"SELECT _dc_v3_Place_0.name AS name,\n" +
				"_dc_v3_Place_0.id AS city_or_county\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Place_0.id\n" +
				"WHERE _dc_v3_Triple_0.object_value = \"dc/b72vdv\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"containedInPlace\"\n",
			emptyProv,
		},
	} {
		nodes, queries, err := datalog.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap)
		if err != nil {
			t.Errorf("Translate(%s) = %s", c.name, err)
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
	}
}

func TestTranslateIOCountyBQ(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	mappings := testutil.ReadTestMapping(t, []string{"testdata/oi_county_mapping.mcf"})
	for _, c := range []struct {
		name     string
		queryStr string
		wantSQL  string
	}{
		{
			"all_pops_of_a_place",
			`SELECT ?race,
				typeOf ?place Place,
				typeOf ?pop StatisticalPopulation,
				race ?pop ?race,
				location ?pop ?place,
				geoId ?place "40005"`,
			"SELECT _dc_v3_bq_county_outcomes_1.race AS race\n" +
				"FROM `dc_v3.bq_county_outcomes` AS _dc_v3_bq_county_outcomes_1\n" +
				"WHERE _dc_v3_bq_county_outcomes_1.geo_id = \"40005\"\n",
		},
	} {
		nodes, queries, err := datalog.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap)
		if err != nil {
			t.Errorf("Translate(%s) = %s", c.name, err)
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
	}
}

func TestTranslateWeather(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	mappings := testutil.ReadTestMapping(t, []string{"testdata/test_mapping.mcf"})
	for _, c := range []struct {
		name     string
		queryStr string
		wantSQL  string
	}{
		{
			"weather",
			`SELECT ?min ?max ?unit,
				typeOf ?o WeatherObservation,
				observedNode ?o geoId/06,
				measuredProperty ?o temperature,
				unit ?o ?unit,
				minValue ?o ?min,
				maxValue ?o ?max`,

			"SELECT _dc_v3_MonthlyWeather_0.temp_c_min AS min,\n" +
				"_dc_v3_MonthlyWeather_0.temp_c_max AS max,\n" +
				"\"Celsius\"\n" +
				"FROM `dc_v3.MonthlyWeather` AS _dc_v3_MonthlyWeather_0\n" +
				"WHERE _dc_v3_MonthlyWeather_0.place_id = \"geoId/06\"\n",
		},
		{
			"weather_multipleCity",
			`SELECT ?place ?MeanTemp,
				typeOf ?o WeatherObservation,
				measuredProperty ?o temperature,
				meanValue ?o ?MeanTemp,
				observedNode ?o ?place,
				dcid ?place geoId/4261000 geoId/0649670 geoId/4805000,
				observationDate ?o "2019-05-09"`,
			"SELECT _dc_v3_MonthlyWeather_0.place_id AS place,\n" +
				"_dc_v3_MonthlyWeather_0.temp_c_mean AS MeanTemp\n" +
				"FROM `dc_v3.MonthlyWeather` AS _dc_v3_MonthlyWeather_0\n" +
				"WHERE _dc_v3_MonthlyWeather_0.observation_date = \"2019-05-09\"\n" +
				"AND _dc_v3_MonthlyWeather_0.place_id IN (\"geoId/4261000\", \"geoId/0649670\", \"geoId/4805000\")\n",
		},
	} {
		nodes, queries, err := datalog.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap)
		if err != nil {
			t.Errorf("Translate(%s) = %s", c.name, err)
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
	}
}

func TestTranslateWeatherSparql(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	mappings := testutil.ReadTestMapping(t, []string{"testdata/test_mapping.mcf"})
	for _, c := range []struct {
		name     string
		queryStr string
		wantSQL  string
	}{
		{
			"weather",
			`
			SELECT ?MeanTemp
			WHERE {
				?o typeOf WeatherObservation .
				?o measuredProperty temperature .
				?o meanValue ?MeanTemp .
				?o observationDate "2018-01" .
				?o observedNode ?place .
				?place dcid geoId/4261000
			}`,

			"SELECT _dc_v3_MonthlyWeather_0.temp_c_mean AS MeanTemp\n" +
				"FROM `dc_v3.MonthlyWeather` AS _dc_v3_MonthlyWeather_0\n" +
				"WHERE _dc_v3_MonthlyWeather_0.observation_date = \"2018-01\"\n" +
				"AND _dc_v3_MonthlyWeather_0.place_id = \"geoId/4261000\"\n",
		},
	} {
		nodes, queries, _, err := sparql.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap)
		if err != nil {
			t.Errorf("Translate(%s) = %s", c.name, err)
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
	}
}

func TestTranslatePew(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	mappings := testutil.ReadTestMapping(t, []string{
		"testdata/PewReligiousLandscapeSurvey2007Items.mcf",
		"testdata/PewReligiousLandscapeSurvey2007ItemsMetadata.mcf",
		"testdata/PewReligiousLandscapeSurvey2007Response.mcf",
	})
	for _, c := range []struct {
		name     string
		queryStr string
		wantSQL  string
	}{
		{
			"name",
			`
			SELECT ?name
			WHERE {
				?unit typeOf SampleUnit .
				?unit name ?name .
				?response typeOf SurveyResponse .
				?response respondent ?unit .
				?response inLanguage "Spanish"
			}
			`,
			"SELECT _dc_v3_PewReligiousLandscapeSurvey2007Response_0.SampleUnit_Name AS name\n" +
				"FROM `dc_v3.PewReligiousLandscapeSurvey2007Response` AS _dc_v3_PewReligiousLandscapeSurvey2007Response_1\n" +
				"JOIN `dc_v3.PewReligiousLandscapeSurvey2007Response` AS _dc_v3_PewReligiousLandscapeSurvey2007Response_0\n" +
				"ON _dc_v3_PewReligiousLandscapeSurvey2007Response_1.SampleUnit_Dcid = _dc_v3_PewReligiousLandscapeSurvey2007Response_0.SampleUnit_Dcid\n" +
				"WHERE _dc_v3_PewReligiousLandscapeSurvey2007Response_1.SurveyResponse_InLanguage = \"Spanish\"\n",
		},
		{
			"option name",
			`
			SELECT ?roptionname
			WHERE {
				?question typeOf SurveyItem .
				?question dcid "SurveyItem/Pew_ContinentalUS_ReligiousLandscapeSurvey_2007_protfam" .
				?roption typeOf ResponseOption .
				?question hasResponseOption ?roption .
				?roption name ?roptionname
			}
			`,
			"SELECT _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_1.ResponseOption_Name AS roptionname\n" +
				"FROM `dc_v3.PewReligiousLandscapeSurvey2007ItemsMetadata` AS _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_0\n" +
				"JOIN `dc_v3.PewReligiousLandscapeSurvey2007ItemsMetadata` AS _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_1\n" +
				"ON _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_0.ResponseOption_Dcid = _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_1.ResponseOption_Dcid\n" +
				"WHERE _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_0.SurveyItem_Dcid = \"SurveyItem/Pew_ContinentalUS_ReligiousLandscapeSurvey_2007_protfam\"\n",
		},
		{
			"qcode",
			`
			SELECT ?qcode
			WHERE {
				?question typeOf SurveyItem .
				?question name ?qcode .
				?question hasResponseOption ?roption .
				?roption typeOf ResponseOption .
				?roption identifier "0"
			}
			`,
			"SELECT _dc_v3_PewReligiousLandscapeSurvey2007Items_0.SurveyItem_Name AS qcode\n" +
				"FROM `dc_v3.PewReligiousLandscapeSurvey2007ItemsMetadata` AS _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_1\n" +
				"JOIN `dc_v3.PewReligiousLandscapeSurvey2007ItemsMetadata` AS _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_0\n" +
				"ON _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_1.ResponseOption_Dcid = _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_0.ResponseOption_Dcid\n" +
				"JOIN `dc_v3.PewReligiousLandscapeSurvey2007Items` AS _dc_v3_PewReligiousLandscapeSurvey2007Items_0\n" +
				"ON _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_0.SurveyItem_Dcid = _dc_v3_PewReligiousLandscapeSurvey2007Items_0.SurveyItem_Dcid\n" +
				"WHERE _dc_v3_PewReligiousLandscapeSurvey2007ItemsMetadata_1.ResponseOption_Identifier = \"0\"\n",
		},
	} {
		nodes, queries, _, err := sparql.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap)
		if err != nil {
			t.Errorf("Translate(%s) = %s", c.name, err)
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
	}
}

func TestSparql(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	mappings := testutil.ReadTestMapping(t, []string{
		"testdata/test_mapping.mcf",
	})
	for _, c := range []struct {
		name     string
		queryStr string
		wantSQL  string
	}{
		{
			"popobs",
			`
			SELECT  ?name ?a
			WHERE {
			  ?a typeOf State .
			  ?a name ?name .
			  ?b location ?a .
			  ?b numConstraints 0 .
			  ?b typeOf StatisticalPopulation .
			  ?b populationType Person .
			  ?c observedNode ?b .
			  ?c typeOf Observation .
			  ?c measuredProperty count .
			  ?c measuredValue ?population .
			}
			`,
			"SELECT _dc_v3_Place_0.name AS name,\n" +
				"_dc_v3_Place_0.id AS a\n" +
				"FROM `dc_v3.StatisticalPopulation` AS _dc_v3_StatisticalPopulation_1\n" +
				"JOIN `dc_v3.Observation` AS _dc_v3_Observation_2\n" +
				"ON _dc_v3_StatisticalPopulation_1.id = _dc_v3_Observation_2.observed_node_key\n" +
				"JOIN `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"ON _dc_v3_StatisticalPopulation_1.place_key = _dc_v3_Place_0.id\n" +
				"WHERE _dc_v3_Observation_2.measured_prop = \"count\"\n" +
				"AND _dc_v3_Place_0.type = \"State\"\n" +
				"AND _dc_v3_StatisticalPopulation_1.num_constraints = 0\n" +
				"AND _dc_v3_StatisticalPopulation_1.population_type = \"Person\"\n",
		},
		{
			"adminarea1",
			`
			SELECT ?name
		  WHERE {
		  	?state typeOf AdministrativeArea1 .
		    ?state name ?name .
		  }
			`,
			"SELECT _dc_v3_Place_0.name AS name\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_0\n" +
				"WHERE _dc_v3_Place_0.type = \"AdministrativeArea1\"\n",
		},
		{
			"bio",
			`
			SELECT distinct ?d
			WHERE {
				?ccdt typeOf ChemicalCompoundDiseaseTreatment .
				?ccdt compoundID ?c .
				?ccdt diseaseID ?dis .
				?dis commonName ?d .
				?c drugName "Prednisone" .
			}
			LIMIT 100
			`,
			"SELECT _dc_v3_Triple_3.object_value AS d\n" +
				"FROM `dc_v3.Triple` AS _dc_v3_Triple_0\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_1\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_1.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_2\n" +
				"ON _dc_v3_Triple_0.subject_id = _dc_v3_Triple_2.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_4\n" +
				"ON _dc_v3_Triple_1.object_id = _dc_v3_Triple_4.subject_id\n" +
				"JOIN `dc_v3.Triple` AS _dc_v3_Triple_3\n" +
				"ON _dc_v3_Triple_2.object_id = _dc_v3_Triple_3.subject_id\n" +
				"WHERE _dc_v3_Triple_0.object_id = \"ChemicalCompoundDiseaseTreatment\"\n" +
				"AND _dc_v3_Triple_0.predicate = \"typeOf\"\n" +
				"AND _dc_v3_Triple_1.predicate = \"compoundID\"\n" +
				"AND _dc_v3_Triple_2.predicate = \"diseaseID\"\n" +
				"AND _dc_v3_Triple_3.predicate = \"commonName\"\n" +
				"AND _dc_v3_Triple_4.object_value = \"Prednisone\"\n" +
				"AND _dc_v3_Triple_4.predicate = \"drugName\"\n",
		},
	} {
		nodes, queries, _, err := sparql.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap)
		if err != nil {
			t.Errorf("Translate(%s) = %s", c.name, err)
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
	}
}

func TestStatVarObs(t *testing.T) {
	subTypeMap, err := solver.GetSubTypeMap("table_types.json")
	if err != nil {
		t.Fatalf("GetSubTypeMap() = %v", err)
	}

	mappings := testutil.ReadTestMapping(t, []string{
		"testdata/test_mapping.mcf",
	})
	for _, c := range []struct {
		name     string
		queryStr string
		wantSQL  string
	}{
		{
			"country-gdp-place",
			`
				SELECT ?observation ?place
				WHERE {
				 ?observation typeOf StatVarObservation .
				 ?observation variableMeasured Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita .
				 ?observation observationAbout ?place .
				 ?place typeOf Country .
				}
				`,
			"SELECT _dc_v3_StatVarObservation_0.id AS observation,\n" +
				"_dc_v3_Place_1.id AS place\n" +
				"FROM `dc_v3.Place` AS _dc_v3_Place_1\n" +
				"JOIN `dc_v3.StatVarObservation` AS _dc_v3_StatVarObservation_0\n" +
				"ON _dc_v3_Place_1.id = _dc_v3_StatVarObservation_0.observation_about\n" +
				"WHERE _dc_v3_StatVarObservation_0.variable_measured = \"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita\"\n" +
				"AND _dc_v3_Place_1.type = \"Country\"\n",
		},
		{
			"browser-observation",
			`
			SELECT ?dcid ?mmethod ?obsPeriod ?obsDate
				WHERE {
					?svObservation typeOf StatVarObservation .
					?svObservation variableMeasured Count_Person .
					?svObservation observationAbout country/USA .
					?svObservation dcid ?dcid .
					?svObservation measurementMethod ?mmethod .
					?svObservation observationPeriod ?obsPeriod .
				}
				`,
			"SELECT _dc_v3_StatVarObservation_0.id AS dcid,\n" +
				"_dc_v3_StatVarObservation_0.measurement_method AS mmethod,\n" +
				"_dc_v3_StatVarObservation_0.observation_period AS obsPeriod,\n\n" +
				"FROM `dc_v3.StatVarObservation` AS _dc_v3_StatVarObservation_0\n" +
				"WHERE _dc_v3_StatVarObservation_0.variable_measured = \"Count_Person\"\n" +
				"AND _dc_v3_StatVarObservation_0.observation_about = \"country/USA\"\n",
		},
	} {
		nodes, queries, _, err := sparql.ParseQuery(c.queryStr)
		if err != nil {
			t.Errorf("ParseQuery error: %s", err)
			continue
		}
		translation, err := Translate(mappings, nodes, queries, subTypeMap)
		if err != nil {
			t.Errorf("Translate(%s) = %s", c.name, err)
		}
		if diff := cmp.Diff(c.wantSQL, translation.SQL); diff != "" {
			t.Errorf("getSQL unexpected sql diff for test %s, %v", c.name, diff)
			continue
		}
	}
}
