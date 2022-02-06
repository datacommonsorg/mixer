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
	"encoding/json"
	"io/ioutil"
	"log"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSortTables(t *testing.T) {
	for _, c := range []struct {
		tables   []string
		expected []string
	}{
		{
			tables: []string{
				"borgcron_random",
				"borgcron_infrequent_2022_01_31_23_15_14",
				"borgcron_frequent_2022_02_01_14_20_47",
				"borgcron_dc_branch_2022_02_01_14_00_49",
				"borgcron_ipcc_2022_01_31_20_56_49",
			},
			expected: []string{
				"borgcron_frequent_2022_02_01_14_20_47",
				"borgcron_dc_branch_2022_02_01_14_00_49",
				"borgcron_ipcc_2022_01_31_20_56_49",
				"borgcron_random",
				"borgcron_infrequent_2022_01_31_23_15_14",
			},
		},
	} {
		SortTables(c.tables)
		if diff := cmp.Diff(c.tables, c.expected); diff != "" {
			t.Errorf("SortTables() got diff: %v", diff)
		}
	}
}

func TestFileFormat(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	jsonByte, _ := ioutil.ReadFile(
		path.Join(path.Dir(filename), "../../../deploy/storage/bigtable_import_groups.json"))

	var c TableConfig
	if err := json.Unmarshal(jsonByte, &c); err != nil {
		log.Fatalf("Failed to load import group tables config")
	}
	for _, table := range c.Tables {
		if !strings.HasPrefix(table, "borgcron_") {
			t.Errorf("Table name should start with borgcron_, but got %s", table)
		}
	}
}
