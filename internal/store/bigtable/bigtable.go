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

package bigtable

import (
	"context"
	"sync"

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
) *Group {
	return &Group{
		baseTables:  baseTables,
		branchTable: branchTable,
		isProto:     len(baseTables) > 1,
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

type chanData struct {
	// Identifier of the data when reading multiple BigTable rows
	// concurrently. It could be place dcid, statvar dcid or other "key".
	token string
	// Data read from Cloud Bigtable
	data interface{}
}

// readRowFn generates a function to be used as the callback function in Bigtable Read.
// This utilizes the Golang closure so the arguments can be scoped in the
// generated function.
func readRowFn(
	errCtx context.Context,
	btTable *cbt.Table,
	rowSetPart cbt.RowSet,
	getToken func(string) (string, error),
	action func(string, []byte, bool) (interface{}, error),
	elemChan chan chanData,
	proto bool,
) func() error {
	return func() error {
		if err := btTable.ReadRows(errCtx, rowSetPart,
			func(btRow cbt.Row) bool {
				if len(btRow[BtFamily]) == 0 {
					return true
				}
				raw := btRow[BtFamily][0].Value

				if getToken == nil {
					getToken = util.KeyToDcid
				}
				token, err := getToken(btRow.Key())
				if err != nil {
					return false
				}

				jsonRaw, err := util.UnzipAndDecode(string(raw))
				if err != nil {
					return false
				}
				elem, err := action(token, jsonRaw, proto)
				if err != nil {
					return false
				}
				elemChan <- chanData{token, elem}
				return true
			}); err != nil {
			return err
		}
		return nil
	}
}

// Read reads BigTable rows from base Bigtable and branch
// Bigtable in parallel.
//
// Reading multiple rows is chunked as the size limit for RowSet is 500KB.
//
//
// Args:
// baseBt: The bigtable that holds the base cache
// branchBt: The bigtable that holds the branch cache.
// rowSet: BigTable rowSet containing the row keys.
// action: A callback function that converts the raw bytes into appropriate
//		go struct based on the cache content.
// getToken: A function to get back the indexed token (like place dcid) from
//		bigtable row key.
//
func Read(
	ctx context.Context,
	btGroup *Group,
	rowSet cbt.RowSet,
	action func(string, []byte, bool) (interface{}, error),
	getToken func(string) (string, error),
	readBranch bool,
) (
	[]map[string]interface{}, map[string]interface{}, error,
) {
	baseTables := btGroup.BaseTables()
	branchTable := btGroup.BranchTable()
	if len(baseTables) == 0 && branchTable == nil {
		return nil, nil, status.Errorf(
			codes.NotFound, "Bigtable instance is not specified")
	}

	// Function start
	var rowSetSize int
	var rowList cbt.RowList
	var rowRangeList cbt.RowRangeList
	switch v := rowSet.(type) {
	case cbt.RowList:
		rowList = rowSet.(cbt.RowList)
		rowSetSize = len(rowList)
	case cbt.RowRangeList:
		rowRangeList = rowSet.(cbt.RowRangeList)
		rowSetSize = len(rowRangeList)
	default:
		return nil, nil, status.Errorf(
			codes.Internal, "Unsupported RowSet type: %v", v)
	}
	if rowSetSize == 0 {
		return nil, nil, nil
	}

	baseChans := make(map[int]chan chanData)
	for i := 0; i < len(baseTables); i++ {
		baseChans[i] = make(chan chanData, rowSetSize)
	}
	branchChan := make(chan chanData, rowSetSize)

	errs, errCtx := errgroup.WithContext(ctx)
	for i := 0; i <= rowSetSize/BtBatchQuerySize; i++ {
		left := i * BtBatchQuerySize
		right := (i + 1) * BtBatchQuerySize
		if right > rowSetSize {
			right = rowSetSize
		}
		var rowSetPart cbt.RowSet
		if len(rowList) > 0 {
			rowSetPart = rowList[left:right]
		} else {
			rowSetPart = rowRangeList[left:right]
		}
		// Read from all the given tables.
		if len(baseTables) > 0 {
			for j := 0; j < len(baseTables); j++ {
				j := j
				if baseTables[j] != nil {
					errs.Go(readRowFn(errCtx, baseTables[j], rowSetPart, getToken, action, baseChans[j], btGroup.isProto))
				}
			}
		}
		if readBranch && branchTable != nil {
			errs.Go(readRowFn(errCtx, branchTable, rowSetPart, getToken, action, branchChan, btGroup.isProto))
		}
	}
	err := errs.Wait()
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < len(baseChans); i++ {
		close(baseChans[i])
	}
	close(branchChan)

	baseResult := []map[string]interface{}{}
	if baseTables != nil {
		for i := 0; i < len(baseTables); i++ {
			item := map[string]interface{}{}
			for elem := range baseChans[i] {
				item[elem.token] = elem.data
			}
			baseResult = append(baseResult, item)
		}
	}

	branchResult := map[string]interface{}{}
	if readBranch {
		if branchTable != nil {
			for elem := range branchChan {
				branchResult[elem.token] = elem.data
			}
		}
	}
	return baseResult, branchResult, nil
}
