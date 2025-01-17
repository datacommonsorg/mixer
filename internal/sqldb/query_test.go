// Copyright 2025 Google LLC
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

package sqldb

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/go-test/deep"
	"github.com/google/go-cmp/cmp"
)

func TestGetKeyValue(t *testing.T) {
	sqlClient, err := NewSQLiteClient("../../test/sqlquery/key_value/datacommons.db")
	if err != nil {
		t.Fatalf("Could not open test database: %v", err)
	}

	want := &pb.StatVarGroups{
		StatVarGroups: map[string]*pb.StatVarGroupNode{
			"svg1": {AbsoluteName: "SVG1"},
		},
	}

	var got pb.StatVarGroups

	found, _ := sqlClient.GetKeyValue(context.Background(), StatVarGroupsKey, &got)
	if !found {
		t.Errorf("Key value data not found: %s", StatVarGroupsKey)
	}
	if diff := deep.Equal(want, &got); diff != nil {
		t.Errorf("Unexpected diff %v", diff)
	}

}

func TestGenerateCTESelectStatement(t *testing.T) {
	for _, tc := range []struct {
		name        string
		paramPrefix string
		values      []string
		want        statement
	}{
		{
			name:        "multiple values",
			paramPrefix: "node",
			values:      []string{"n1", "n2"},
			want: statement{
				query: "SELECT :node1 UNION ALL SELECT :node2",
				args: map[string]interface{}{
					"node1": "n1",
					"node2": "n2",
				},
			},
		},
		{
			name:        "single value",
			paramPrefix: "node",
			values:      []string{"n1"},
			want: statement{
				query: "SELECT :node1",
				args: map[string]interface{}{
					"node1": "n1",
				},
			},
		},
	} {
		got := generateCTESelectStatement(tc.paramPrefix, tc.values)

		if diff := cmp.Diff(got, tc.want, cmp.AllowUnexported(statement{})); diff != "" {
			t.Errorf("Unexpected diff (%s) %v", tc.name, diff)
		}
	}
}
func TestChunkSlice(t *testing.T) {
	for _, tc := range []struct {
		name      string
		items     []string
		chunkSize int
		want      [][]string
	}{
		{
			name:      "2 chunks",
			items:     []string{"1", "2", "3"},
			chunkSize: 2,
			want: [][]string{
				{"1", "2"},
				{"3"},
			},
		},
		{
			name:      "1 chunk",
			items:     []string{"1", "2", "3"},
			chunkSize: 3,
			want: [][]string{
				{"1", "2", "3"},
			},
		},
	} {
		got := chunkSlice(tc.items, tc.chunkSize)

		if diff := cmp.Diff(got, tc.want); diff != "" {
			t.Errorf("Unexpected diff (%s) %v", tc.name, diff)
		}
	}
}
