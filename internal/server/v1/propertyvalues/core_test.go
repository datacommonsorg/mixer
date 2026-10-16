// Copyright 2026 Google LLC
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

package propertyvalues

import (
	"testing"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBuildValidatedCursorGroupMap(t *testing.T) {
	validCursors := []*pbv1.Cursor{
		{ImportGroup: 0, Page: 0, Item: 5},
		{ImportGroup: 1, Page: 1, Item: 2},
	}

	for _, tc := range []struct {
		name         string
		cursorGroups []*pbv1.CursorGroup
		nodes        []string
		properties   []string
		numTables    int
		wantCode     codes.Code
	}{
		{
			name: "valid_cursor_groups",
			cursorGroups: []*pbv1.CursorGroup{
				{
					Keys:    []string{"geoId/06", "name", "Text"},
					Cursors: validCursors,
				},
			},
			nodes:      []string{"geoId/06"},
			properties: []string{"name"},
			numTables:  2,
			wantCode:   codes.OK,
		},
		{
			name: "invalid_key_count",
			cursorGroups: []*pbv1.CursorGroup{
				{
					Keys:    []string{"geoId/06", "name"},
					Cursors: validCursors,
				},
			},
			nodes:      []string{"geoId/06"},
			properties: []string{"name"},
			numTables:  2,
			wantCode:   codes.InvalidArgument,
		},
		{
			name: "unrequested_node_key",
			cursorGroups: []*pbv1.CursorGroup{
				{
					Keys:    []string{"geoId/99999", "name", "Text"},
					Cursors: validCursors,
				},
			},
			nodes:      []string{"geoId/06"},
			properties: []string{"name"},
			numTables:  2,
			wantCode:   codes.InvalidArgument,
		},
		{
			name: "unrequested_property_key",
			cursorGroups: []*pbv1.CursorGroup{
				{
					Keys:    []string{"geoId/06", "containedInPlace", "Place"},
					Cursors: validCursors,
				},
			},
			nodes:      []string{"geoId/06"},
			properties: []string{"name"},
			numTables:  2,
			wantCode:   codes.InvalidArgument,
		},
		{
			name: "fewer_cursors_than_tables",
			cursorGroups: []*pbv1.CursorGroup{
				{
					Keys:    []string{"geoId/06", "name", "Text"},
					Cursors: validCursors[:1],
				},
			},
			nodes:      []string{"geoId/06"},
			properties: []string{"name"},
			numTables:  2,
			wantCode:   codes.InvalidArgument,
		},
		{
			name: "mismatched_import_group_index",
			cursorGroups: []*pbv1.CursorGroup{
				{
					Keys: []string{"geoId/06", "name", "Text"},
					Cursors: []*pbv1.Cursor{
						{ImportGroup: 0, Page: 0, Item: 0},
						{ImportGroup: 5, Page: 0, Item: 0},
					},
				},
			},
			nodes:      []string{"geoId/06"},
			properties: []string{"name"},
			numTables:  2,
			wantCode:   codes.InvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildValidatedCursorGroupMap(tc.cursorGroups, tc.nodes, tc.properties, tc.numTables)
			if gotCode := status.Code(err); gotCode != tc.wantCode {
				t.Errorf("buildValidatedCursorGroupMap(%s) status code = %v, want %v (err: %v)", tc.name, gotCode, tc.wantCode, err)
			}
		})
	}
}
