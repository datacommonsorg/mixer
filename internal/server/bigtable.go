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
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// bigTableReadRowsParallel reads BigTable rows from base Bigtable and branch
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
func bigTableReadRowsParallel(
	ctx context.Context,
	store *store.Store,
	rowSet bigtable.RowSet,
	action func(string, []byte) (interface{}, error),
	getToken func(string) (string, error)) (
	map[string]interface{}, map[string]interface{}, error) {
	baseBt := store.BaseBt()
	branchBt := store.BranchBt()
	if baseBt == nil && branchBt == nil {
		return nil, nil, status.Errorf(
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
		return nil, nil, status.Errorf(codes.Internal, "Unsupported RowSet type: %v", v)
	}
	if rowSetSize == 0 {
		return nil, nil, nil
	}

	baseChan := make(chan chanData, rowSetSize)
	branchChan := make(chan chanData, rowSetSize)

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
		if baseBt != nil {
			errs.Go(readRowFn(errCtx, baseBt, rowSetPart, getToken, action, baseChan))
		}
		if branchBt != nil {
			errs.Go(readRowFn(errCtx, branchBt, rowSetPart, getToken, action, branchChan))
		}
	}

	err := errs.Wait()
	if err != nil {
		return nil, nil, err
	}
	close(baseChan)
	close(branchChan)

	baseResult := map[string]interface{}{}
	branchResult := map[string]interface{}{}

	if baseBt != nil {
		for elem := range baseChan {
			baseResult[elem.token] = elem.data
		}
	}
	if branchBt != nil {
		for elem := range branchChan {
			branchResult[elem.token] = elem.data
		}
	}
	return baseResult, branchResult, nil
}
