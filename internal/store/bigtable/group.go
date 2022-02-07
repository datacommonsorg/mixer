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

// Defines struct and util functions of bigtable table and groups.

package bigtable

import (
	"context"
	"sort"
	"strings"
	"sync"

	cbt "cloud.google.com/go/bigtable"
)

var groupRank = map[string]int{
	"frequent":   0,
	"dc":         1,
	"ipcc":       2,
	"infrequent": 10000,
}

const defaultRank = 9999

// Table holds the bigtable name and client stub.
type Table struct {
	name  string
	table *cbt.Table
}

// NewTable creates a new Table struct.
func NewTable(name string, table *cbt.Table) *Table {
	return &Table{name: name, table: table}
}

// Name access the name of a table
func (t *Table) Name() string {
	return t.name
}

// Group represents all the cloud bigtables that mixer talks to.
type Group struct {
	tables          []*Table
	lock            sync.RWMutex
	branchTableName string
	isProto         bool
}

//  TableConfig represents the config for a list of Bigtable.
type TableConfig struct {
	Tables []string `json:"tables,omitempty"`
}

// NewGroup creates a BigtableGroup
func NewGroup(
	tables []*Table,
	branchTableName string,
	useImportGroup bool,
) *Group {
	if useImportGroup {
		SortTables(tables)
	}
	return &Group{
		tables:  tables,
		isProto: useImportGroup,
	}
}

// Tables is the accessor for all the Bigtable client stubs.
func (g *Group) Tables() []*cbt.Table {
	g.lock.RLock()
	defer g.lock.RUnlock()
	result := []*cbt.Table{}
	for _, t := range g.tables {
		result = append(result, t.table)
	}
	return result
}

// TableNames is the accesser to get all the Bigtable names.
func (g *Group) TableNames() []string {
	g.lock.RLock()
	defer g.lock.RUnlock()
	result := []string{}
	for _, t := range g.tables {
		result = append(result, t.name)
	}
	return result
}

// UpdateBranchTable updates the branch Bigtable.
func (g *Group) UpdateBranchTable(branchTable *Table) {
	g.lock.Lock()
	defer g.lock.Unlock()
	tables := []*Table{}
	for _, t := range g.tables {
		if t.name != g.branchTableName {
			tables = append(tables, t)
		}
	}
	tables = append(tables, branchTable)
	g.branchTableName = branchTable.name
	g.tables = tables
	if g.isProto {
		// To place branch table in the right place
		SortTables(g.tables)
	}
}

// NewGroupWithPreferredBase creates a new group with only one base table.
// The base table is the preferred Bigtable, which is used for data that needs
// not be merged.
func NewGroupWithPreferredBase(g *Group) *Group {
	return &Group{
		tables:          g.tables[:1],
		branchTableName: "",
		isProto:         g.isProto,
	}
}

// NewTable creates a new cbt.Table instance.
func NewBtTable(ctx context.Context, projectID, instanceID, tableID string) (
	*cbt.Table, error) {
	btClient, err := cbt.NewClient(ctx, projectID, instanceID)
	if err != nil {
		return nil, err
	}
	return btClient.Open(tableID), nil
}

// SortTables sorts the bigtable by import group preferences
// - frequent should always be the highest rank
// - infrequent should always be the lowest rank
// - if a group is not in ranking list, put it right before "infrequent" and
//   after other groups with ranking.
func SortTables(tables []*Table) {
	sort.SliceStable(tables, func(i, j int) bool {
		// ranking for i
		// This is to parse the table name like "borgcron_frequent_2022_02_01_14_20_47"
		// and get the actual import group name.
		// TODO: Update this if table format changes.
		ni := strings.Split(tables[i].name, "_")[1]
		ri, ok := groupRank[ni]
		if !ok {
			ri = defaultRank
		}
		// ranking for j
		nj := strings.Split(tables[j].name, "_")[1]
		rj, ok := groupRank[nj]
		if !ok {
			rj = defaultRank
		}
		return ri < rj
	})
}
