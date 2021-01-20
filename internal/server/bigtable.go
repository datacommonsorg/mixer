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

package server

import (
	"context"

	"cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	baseBtIndex   = 1
	branchBtIndex = 0
)

// Generates a function to be used as the callback function in Bigtable Read.
// This utilizes the Golang closure so the arguments can be scoped in the
// generated function.
func readRowFn(
	errCtx context.Context,
	btTable *bigtable.Table,
	rowSetPart bigtable.RowSet,
	getToken func(string) (string, error),
	action func(string, []byte) (interface{}, error),
	elemChan chan chanData,
) func() error {
	return func() error {
		if err := btTable.ReadRows(errCtx, rowSetPart,
			func(btRow bigtable.Row) bool {
				if len(btRow[util.BtFamily]) == 0 {
					return true
				}
				raw := btRow[util.BtFamily][0].Value

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
				elem, err := action(token, jsonRaw)
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

// bigTableReadRowsParallel reads BigTable rows from multiple Bigtables
// in parallel.
//
// Reading multiple rows is chunked as the size limit for RowSet is 500KB.
//
// Args:
// btTables: A list of Cloud BigTable client that is ordered by the importance.
// 		For the same key, if data is present in multiple tables, the first table
//		in the list will be used.
// rowSet: BigTable rowSet containing the row keys.
// action: A callback function that converts the raw bytes into appropriate
//		go struct based on the cache content.
// getToken: A function to get back the indexed token (like place dcid) from
//		bigtable row key.
//
func bigTableReadRowsParallel(
	ctx context.Context,
	btTables []*bigtable.Table,
	rowSet bigtable.RowSet,
	action func(string, []byte) (interface{}, error),
	getToken func(string) (string, error)) (
	map[string]interface{}, error) {
	if len(btTables) == 0 {
		return nil, status.Errorf(
			codes.NotFound, "Bigtable instance is not specified")
	}

	// Function start
	var rowSetSize int
	var rowList bigtable.RowList
	var rowRangeList bigtable.RowRangeList
	switch v := rowSet.(type) {
	case bigtable.RowList:
		rowList = rowSet.(bigtable.RowList)
		rowSetSize = len(rowList)
	case bigtable.RowRangeList:
		rowRangeList = rowSet.(bigtable.RowRangeList)
		rowSetSize = len(rowRangeList)
	default:
		return nil, status.Errorf(codes.Internal, "Unsupported RowSet type: %v", v)
	}
	if rowSetSize == 0 {
		return nil, nil
	}

	elemChanMap := map[*bigtable.Table](chan chanData){}
	for _, btTable := range btTables {
		elemChanMap[btTable] = make(chan chanData, rowSetSize)
	}

	errs, errCtx := errgroup.WithContext(ctx)
	for i := 0; i <= rowSetSize/util.BtBatchQuerySize; i++ {
		left := i * util.BtBatchQuerySize
		right := (i + 1) * util.BtBatchQuerySize
		if right > rowSetSize {
			right = rowSetSize
		}
		var rowSetPart bigtable.RowSet
		if len(rowList) > 0 {
			rowSetPart = rowList[left:right]
		} else {
			rowSetPart = rowRangeList[left:right]
		}
		// Read from all the given tables.
		// The result is sent to the "elemChan" channel.
		for _, btTable := range btTables {
			errs.Go(readRowFn(errCtx, btTable, rowSetPart, getToken, action, elemChanMap[btTable]))
		}
	}

	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	for _, ch := range elemChanMap {
		close(ch)
	}

	result := map[string]interface{}{}
	for _, table := range btTables {
		ch := elemChanMap[table]
		for elem := range ch {
			// If date does not exist for this token, added. Otherwise, it is already
			// added from a prefered table.
			if _, ok := result[elem.token]; !ok {
				result[elem.token] = elem.data
			}
		}
	}
	return result, nil
}
