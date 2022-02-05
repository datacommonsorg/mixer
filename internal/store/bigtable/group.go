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

// Group represents all the cloud bigtables that mixer talks to.
type Group struct {
	baseTables  []*cbt.Table
	branchTable *cbt.Table
	branchLock  sync.RWMutex
	isProto     bool
}

//  TableConfig represents the config for a list bigtables.
type TableConfig struct {
	Tables []string `json:"tables,omitempty"`
}

// NewGroup creates a BigtableGroup
func NewGroup(
	baseTables []*cbt.Table,
	branchTable *cbt.Table,
	useImportGroup bool,
) *Group {
	return &Group{
		baseTables:  baseTables,
		branchTable: branchTable,
		isProto:     useImportGroup,
	}
}

// BaseTables is the accessor for base bigtables
func (g *Group) BaseTables() []*cbt.Table {
	return g.baseTables
}

// BranchTable is the accessor for branch bigtable
func (g *Group) BranchTable() *cbt.Table {
	g.branchLock.RLock()
	defer g.branchLock.RUnlock()
	return g.branchTable
}

// UpdateBranchTable updates the branch bigtable
func (g *Group) UpdateBranchTable(branchTable *cbt.Table) {
	g.branchLock.Lock()
	defer g.branchLock.Unlock()
	g.branchTable = branchTable
}

// NewGroupWithPreferredBase creates a new group with only one base table.
// The base table is the preferred Bigtable, which is used for data that needs
// not be merged.
func NewGroupWithPreferredBase(g *Group) *Group {
	return &Group{
		baseTables:  g.BaseTables()[:1],
		branchTable: nil,
		isProto:     g.isProto,
	}
}

// NewTable creates a new cbt.Table instance.
func NewTable(ctx context.Context, projectID, instanceID, tableID string) (
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
func SortTables(tableNames []string) {
	sort.SliceStable(tableNames, func(i, j int) bool {
		// ranking for i
		// This is to parse the table name like "borgcron_frequent_2022_02_01_14_20_47"
		// and get the actual import group name.
		// TODO: Update this if table format changes.
		ni := strings.Split(tableNames[i], "_")[1]
		ri, ok := groupRank[ni]
		if !ok {
			ri = defaultRank
		}
		// ranking for j
		nj := strings.Split(tableNames[j], "_")[1]
		rj, ok := groupRank[nj]
		if !ok {
			rj = defaultRank
		}

		return ri < rj
	})
}
