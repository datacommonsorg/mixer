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
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/util"
	"golang.org/x/sync/errgroup"
)

// bigTableReadRowsParallel reads BigTable rows in parallel,
// considering the size limit for RowSet is 500KB.
func bigTableReadRowsParallel(
	ctx context.Context,
	btTable *bigtable.Table,
	rowSet bigtable.RowSet,
	action func(string, []byte) (interface{}, error),
	opts ...bool) (
	map[string]interface{}, error) {
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
		return nil, fmt.Errorf("unsupported RowSet type: %v", v)
	}
	if rowSetSize == 0 {
		return nil, nil
	}

	errs, errCtx := errgroup.WithContext(ctx)
	elemChan := make(chan chanData, rowSetSize)
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
		errs.Go(func() error {
			if err := btTable.ReadRows(errCtx, rowSetPart,
				func(btRow bigtable.Row) bool {
					if len(btRow[util.BtFamily]) == 0 {
						return true
					}
					raw := btRow[util.BtFamily][0].Value
					var token string
					var err error
					if len(opts) > 0 && opts[0] == true {
						token, err = util.RemoveKeyPrefix(btRow.Key())
						if err != nil {
							return false
						}
					} else {
						token, err = util.KeyToDcid(btRow.Key())
						if err != nil {
							return false
						}
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
		})
	}

	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	close(elemChan)

	result := map[string]interface{}{}
	for elem := range elemChan {
		result[elem.dcid] = elem.data
	}
	return result, nil
}
