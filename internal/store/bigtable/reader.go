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

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	action func(string, []byte) (interface{}, error),
	elemChan chan chanData,
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

// Read reads BigTable rows from multiple Bigtable in parallel.
//
// Reading multiple rows is chunked as the size limit for RowSet is 500KB.
//
// Args:
// baseBt: The bigtable that holds the base cache
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
	action func(string, []byte) (interface{}, error),
	getToken func(string) (string, error),
) ([]map[string]interface{}, error) {
	tables := btGroup.Tables()
	if len(tables) == 0 {
		return nil, status.Errorf(
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
		return nil, status.Errorf(codes.Internal, "Unsupported RowSet type: %v", v)
	}
	if rowSetSize == 0 {
		return nil, nil
	}

	chans := make(map[int]chan chanData)
	for i := 0; i < len(tables); i++ {
		chans[i] = make(chan chanData, rowSetSize)
	}
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
		if len(tables) > 0 {
			for j := 0; j < len(tables); j++ {
				j := j
				if tables[j] != nil {
					errs.Go(readRowFn(errCtx, tables[j], rowSetPart, getToken, action, chans[j]))
				}
			}
		}
	}
	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chans); i++ {
		close(chans[i])
	}

	result := []map[string]interface{}{}
	if tables != nil {
		for i := 0; i < len(tables); i++ {
			item := map[string]interface{}{}
			for elem := range chans[i] {
				item[elem.token] = elem.data
			}
			result = append(result, item)
		}
	}
	return result, nil
}
