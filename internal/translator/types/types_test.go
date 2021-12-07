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

package types

import (
	"testing"

	"github.com/go-test/deep"
)

func TestQuery(t *testing.T) {
	q := NewQuery("typeOf", "?a", "City")
	if !q.IsTypeOf() {
		t.Errorf("%v should be a typeOf query", q)
	}
}

func TestEntity(t *testing.T) {
	db := "dc"
	for _, c := range []struct {
		input   string
		want    *Entity
		wantErr bool
	}{
		{
			"E:Observation->E1",
			&Entity{ID: "E1", Table: Table{Name: "`dc.Observation`"}},
			false,
		},
		{
			"E:Observation-E1",
			nil,
			true,
		},
	} {
		gotResult, err := NewEntity(c.input, db)
		if c.wantErr {
			if err == nil {
				t.Errorf("NewEntity(%s) = nil, want error", c.input)
			}
			continue
		}
		if diff := deep.Equal(c.want, gotResult); diff != nil {
			t.Errorf("Input: %s; unexpected diff %v", c.input, diff)
		}
	}
}

func TestColumn(t *testing.T) {
	db := "dc"
	for _, c := range []struct {
		input   string
		want    *Column
		wantErr bool
	}{
		{
			"C:Observation->id",
			&Column{Name: "id", Table: Table{Name: "`dc.Observation`"}},
			false,
		},
		{
			"E:Observation",
			nil,
			true,
		},
	} {
		result, err := NewColumn(c.input, db)
		if c.wantErr {
			if err == nil {
				t.Errorf("NewColumn(%s) = nil, want error", c.input)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("Input: %s; unexpected diff %v", c.input, diff)
		}
	}
}

func TestMapping(t *testing.T) {
	db := "dc"
	for _, c := range []struct {
		pred    string
		sub     string
		obj     string
		want    *Mapping
		wantErr bool
	}{
		{
			"typeOf",
			"E:Place->E1",
			"C:Place->id",
			&Mapping{
				Pred: "typeOf",
				Sub:  Entity{ID: "E1", Table: Table{Name: "`dc.Place`"}},
				Obj:  Column{Name: "id", Table: Table{Name: "`dc.Place`"}},
			},
			false,
		},
		{
			"functionalDeps",
			"E:Place->E1",
			"dcid",
			&Mapping{
				Pred: FuncDeps{},
				Sub:  Entity{ID: "E1", Table: Table{Name: "`dc.Place`"}},
				Obj:  []string{"dcid"},
			},
			false,
		},
		{
			"location",
			"E:Place->E1",
			"E:Place->E2",
			&Mapping{
				Pred: "location",
				Sub:  Entity{ID: "E1", Table: Table{Name: "`dc.Place`"}},
				Obj:  Entity{ID: "E2", Table: Table{Name: "`dc.Place`"}},
			},
			false,
		},
		{
			"C:Triple->predicate",
			"E:Triple->E1",
			"E:Triple->E2",
			&Mapping{
				Pred: Column{Name: "predicate", Table: Table{Name: "`dc.Triple`"}},
				Sub:  Entity{ID: "E1", Table: Table{Name: "`dc.Triple`"}},
				Obj:  Entity{ID: "E2", Table: Table{Name: "`dc.Triple`"}},
			},
			false,
		},
		{
			"name",
			"subject",
			"E:Triple->E2",
			nil,
			true,
		},
	} {
		result, err := NewMapping(c.pred, c.sub, c.obj, db)
		if c.wantErr {
			if err == nil {
				t.Errorf("NewMapping(%s, %s, %s) = nil, want error", c.pred, c.sub, c.obj)
			}
			continue
		}
		if diff := deep.Equal(c.want, result); diff != nil {
			t.Errorf("pred, sub, obj: %s, %s, %s; unexpected diff %v", c.pred, c.sub, c.obj, diff)
		}
	}
}

func TestTableAlias(t *testing.T) {
	for _, c := range []struct {
		table Table
		want  string
	}{
		{
			Table{Name: "Place", ID: "0"},
			"Place0",
		},
		{
			Table{Name: "`dc_store.Place`", ID: "0"},
			"_dc_store_Place_0",
		},
		{
			Table{Name: "`datcom-store.dc_kg_latest.Place`", ID: "1"},
			"_datcom_store_dc_kg_latest_Place_1",
		},
	} {
		if diff := deep.Equal(c.want, c.table.Alias()); diff != nil {
			t.Errorf("table %s; unexpected diff %v", c.table, diff)
		}
	}
}
