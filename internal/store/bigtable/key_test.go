// Copyright 2022 Google LLC
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

package bigtable

import (
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/google/go-cmp/cmp"
)

func TestBuildRowList(t *testing.T) {
	for _, c := range []struct {
		prefix   string
		body     [][]string
		expected bigtable.RowList
	}{
		{
			prefix:   "d/a/",
			body:     [][]string{{"dc1", "dc2"}},
			expected: []string{"d/a/dc1", "d/a/dc2"},
		},
		{
			prefix:   "d/b/",
			body:     [][]string{{"dc1", "dc2"}, {""}},
			expected: []string{"d/b/dc1^", "d/b/dc2^"},
		},
		{
			prefix:   "d/c/",
			body:     [][]string{{"dc1"}, {"name", "address"}},
			expected: []string{"d/c/dc1^name", "d/c/dc1^address"},
		},
		{
			prefix:   "d/c/",
			body:     [][]string{{"dc1", "dc2"}, {"name", "address"}},
			expected: []string{"d/c/dc1^name", "d/c/dc2^name", "d/c/dc1^address", "d/c/dc2^address"},
		},
	} {
		result := BuildRowList(c.prefix, c.body)
		if diff := cmp.Diff(c.expected, result); diff != "" {
			t.Errorf("BuildRowList() got diff: %v", diff)
		}
	}
}
