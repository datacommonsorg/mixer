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
	action func([]byte) (interface{}, error),
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
				elem, err := action(jsonRaw)
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
// Note all Bigtable read use the same set of rowList.
func Read(
	ctx context.Context,
	btGroup *Group,
	rowSet cbt.RowList,
	action func([]byte) (interface{}, error),
	getToken func(string) (string, error),
) ([]map[string]interface{}, error) {
	rowSetMap := map[int]cbt.RowList{}
	for i := 0; i < len(btGroup.Tables()); i++ {
		rowSetMap[i] = rowSet
	}
	return ReadWithGroupRowSet(ctx, btGroup, rowSetMap, action, getToken)
}

// ReadWithGroupRowSet reads BigTable rows from multiple Bigtable in parallel.
// Reading is chunked as the size limit for RowSet is 500KB.
//
// Note the read could have different RowList for each import group Bigtable as
// needed by the pagination APIs.
func ReadWithGroupRowSet(
	ctx context.Context,
	btGroup *Group,
	rowSetMap map[int]cbt.RowList,
	action func([]byte) (interface{}, error),
	getToken func(string) (string, error),
) ([]map[string]interface{}, error) {
	tables := btGroup.Tables()
	if len(tables) == 0 {
		return nil, status.Errorf(codes.NotFound, "Bigtable instance is not specified")
	}
	if len(tables) != len(rowSetMap) {
		return nil, status.Errorf(codes.Internal, "Number of tables and rowSet don't match")
	}
	// Channels for each import group read.
	chans := make(map[int]chan chanData)
	for i := 0; i < len(tables); i++ {
		chans[i] = make(chan chanData, len(rowSetMap[i]))
	}

	errs, errCtx := errgroup.WithContext(ctx)
	// Read from each import group tables. Note each table could have different
	// rowSet in pagination APIs.
	for i := 0; i < len(tables); i++ {
		i := i
		rowSet := rowSetMap[i]
		rowSetSize := len(rowSet)
		for j := 0; j <= rowSetSize/BtBatchQuerySize; j++ {
			left := j * BtBatchQuerySize
			right := (j + 1) * BtBatchQuerySize
			if right > rowSetSize {
				right = rowSetSize
			}
			rowSetPart := rowSet[left:right]
			if tables[i] != nil {
				errs.Go(readRowFn(errCtx, tables[i], rowSetPart, getToken, action, chans[i]))
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
