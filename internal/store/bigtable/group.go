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
	"github.com/datacommonsorg/mixer/internal/server/resource"
)

// ContextKey is used to store addtional values in the context.
type ContextKey int

const (
	// CustomImportGroups is the context key for custom import groups.
	CustomImportGroups ContextKey = iota
)

var groupRank = map[string]int{
	"dcbranch":   0, // Used for the latest proto branch cache
	"branch":     0, // Used for legacy branch cache
	"frequent":   1,
	"ipcc":       2,
	"biomedical": 3,
	"borgcron":   10000, // Used for legacy base cache
	"infrequent": 10000,
}

// Import group with unspecified rank should be ranked right before the
// infrequent group and after all other groups.
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
	metadata        *resource.Metadata
}

// NewGroup creates a BigtableGroup
func NewGroup(
	tables []*Table,
	branchTableName string,
	metadata *resource.Metadata,
) *Group {
	SortTables(tables)
	return &Group{
		tables:          tables,
		branchTableName: branchTableName,
		metadata:        metadata,
	}
}

// GetFrequentGroup creates a group that only has frequent import group table.
func GetFrequentGroup(g *Group) *Group {
	result := &Group{tables: []*Table{}}
	for _, t := range g.tables {
		if strings.HasPrefix(t.name, "frequent_") {
			result.tables = append(result.tables, t)
			break
		}
	}
	return result
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
	SortTables(g.tables)
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
		// This is to parse the table name like "frequent_2022_02_01_14_20_47"
		// and get the actual import group name.
		ni := strings.Split(tables[i].name, "_")[0]
		ri, ok := groupRank[ni]
		if !ok {
			ri = defaultRank
		}
		// ranking for j
		nj := strings.Split(tables[j].name, "_")[0]
		rj, ok := groupRank[nj]
		if !ok {
			rj = defaultRank
		}
		if ri == rj {
			return strings.TrimPrefix(tables[i].name, ni) > strings.TrimPrefix(tables[j].name, nj)
		}
		return ri < rj
	})
}
