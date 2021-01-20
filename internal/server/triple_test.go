// Copyright 2020 Google LLC
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

	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
)

func TestReadTriple(t *testing.T) {
	ctx := context.Background()
	data := map[string]string{}
	dcid := "City"
	key := util.BtTriplesPrefix + dcid
	btRow := []byte(`{
		"triples":[
			{
				"subjectId": "wikidataId/Q9879",
				"subjectName": "Waalwijk",
				"subjectTypes": ["City"],
				"predicate": "typeOf",
				"objectId": "City",
				"objectName": "City",
				"objectTypes" :["Class"]
			}
		]
	}`)

	tableValue, err := util.ZipAndEncode(btRow)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", btRow, err)
	}
	data[key] = tableValue
	// Setup bigtable
	btTable, err := SetupBigtable(ctx, data)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}
	// Test
	want := &TriplesCache{
		[]*Triple{
			{
				SubjectID:    "wikidataId/Q9879",
				SubjectName:  "Waalwijk",
				SubjectTypes: []string{"City"},
				Predicate:    "typeOf",
				ObjectID:     "City",
				ObjectName:   "City",
				ObjectTypes:  []string{"Class"},
			},
		},
	}
	got, err := readTriples(
		ctx, store.NewStore(nil, btTable, nil), buildTriplesKey([]string{"City"}))
	if err != nil {
		t.Errorf("ReadTriple get err: %v", err)
	}
	if diff := cmp.Diff(want, got["City"]); diff != "" {
		t.Errorf("ReadTriple() got diff: %v", diff)
	}
}
