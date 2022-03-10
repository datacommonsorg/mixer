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

package node

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestReadTriple(t *testing.T) {
	ctx := context.Background()
	data := map[string]string{}
	dcid := "City"
	key := bigtable.BtTriplesPrefix + dcid
	btRow := &pb.Triples{
		Triples: []*pb.Triple{
			{
				SubjectId:    "wikidataId/Q9879",
				SubjectName:  "Waalwijk",
				SubjectTypes: []string{"City"},
				Predicate:    "typeOf",
				ObjectId:     "City",
				ObjectName:   "City",
				ObjectTypes:  []string{"Class"},
			},
		},
	}
	raw, err := proto.Marshal(btRow)
	if err != nil {
		t.Errorf("proto.Marshal(%v) = %v", btRow, err)
	}
	tableValue, err := util.ZipAndEncode(raw)
	if err != nil {
		t.Errorf("util.ZipAndEncode(%+v) = %v", btRow, err)
	}
	data[key] = tableValue
	// Setup bigtable
	btTable, err := bigtable.SetupBigtable(ctx, data)
	if err != nil {
		t.Errorf("SetupBigtable(...) = %v", err)
	}
	// Test
	want := &pb.Triples{
		Triples: []*pb.Triple{
			{
				SubjectId:    "wikidataId/Q9879",
				SubjectName:  "Waalwijk",
				SubjectTypes: []string{"City"},
				Predicate:    "typeOf",
				ObjectId:     "City",
				ObjectName:   "City",
				ObjectTypes:  []string{"Class"},
			},
		},
	}
	got, err := ReadTriples(
		ctx,
		bigtable.NewGroup([]*bigtable.Table{bigtable.NewTable("base", btTable)}, ""),
		bigtable.BuildTriplesKey([]string{"City"}),
	)
	if err != nil {
		t.Errorf("ReadTriple get err: %v", err)
	}
	if diff := cmp.Diff(want, got.Triples["City"], protocmp.Transform()); diff != "" {
		t.Errorf("ReadTriple() got diff: %v", diff)
	}
}
