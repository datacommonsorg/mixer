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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestSortTables(t *testing.T) {
	for _, c := range []struct {
		tables   []*Table
		expected []*Table
	}{
		{
			tables: []*Table{
				{name: "random_2022", table: nil},
				{name: "infrequent_2022_01_31_23_15_14", table: nil},
				{name: "frequent_2022_02_01_14_20_47", table: nil},
				{name: "dcbranch_2022_02_01_14_00_49", table: nil},
				{name: "ipcc_2022_01_31_20_56_49", table: nil},
				{name: "private_01_02", table: nil, is_custom: true},
			},
			expected: []*Table{
				{name: "private_01_02", table: nil, is_custom: true},
				{name: "dcbranch_2022_02_01_14_00_49", table: nil},
				{name: "frequent_2022_02_01_14_20_47", table: nil},
				{name: "ipcc_2022_01_31_20_56_49", table: nil},
				{name: "random_2022", table: nil},
				{name: "infrequent_2022_01_31_23_15_14", table: nil},
			},
		},
		{
			tables: []*Table{
				{name: "borgcron_2022_02_15_01_02_51", table: nil},
				{name: "branch_dcbranch_2022_02_16_14_18_02", table: nil},
			},
			expected: []*Table{
				{name: "branch_dcbranch_2022_02_16_14_18_02", table: nil},
				{name: "borgcron_2022_02_15_01_02_51", table: nil},
			},
		},
	} {
		SortTables(c.tables)
		tableNames := []string{}
		for _, t := range c.tables {
			tableNames = append(tableNames, t.name)
		}
		expectNames := []string{}
		for _, t := range c.expected {
			expectNames = append(expectNames, t.name)
		}
		if diff := cmp.Diff(tableNames, expectNames, cmpopts.IgnoreUnexported()); diff != "" {
			t.Errorf("SortTables() got diff: %v", diff)
		}
	}
}
