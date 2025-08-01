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
	"strings"
	"time"

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/metrics"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
)

// BtRow contains the BT read key tokens and the cache data.
type BtRow struct {
	// The body parts of the BT key, which are used to identify the place, dcid
	// or other properties that related to the data.  This is to be used by the
	// caller to group the result.
	Parts []string
	// Data read from Cloud Bigtable
	Data interface{}
}

// readRowFn generates a function to be used as the callback function in Bigtable Read.
// This utilizes the Golang closure so the arguments can be scoped in the
// generated function.
func readRowFn(
	errCtx context.Context,
	btTable *cbt.Table,
	rowSetPart cbt.RowSet,
	action func([]byte) (interface{}, error),
	btRowChan chan BtRow,
	prefix string,
) func() error {
	return func() error {
		if err := btTable.ReadRows(errCtx, rowSetPart,
			func(btRow cbt.Row) bool {
				if len(btRow[BtFamily]) == 0 {
					return true
				}
				raw := btRow[BtFamily][0].Value
				jsonRaw, err := util.UnzipAndDecode(string(raw))
				if err != nil {
					return false
				}
				elem, err := action(jsonRaw)
				if err != nil {
					return false
				}
				parts := strings.Split(strings.TrimPrefix(btRow.Key(), prefix), "^")
				btRowChan <- BtRow{parts, elem}
				return true
			},
		); err != nil {
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
	prefix string,
	body [][]string,
	action func([]byte) (interface{}, error),
) ([][]BtRow, error) {
	if btGroup == nil {
		return nil, nil
	}
	accs := []*Accessor{}
	tables := btGroup.Tables(nil)
	for i := 0; i < len(tables); i++ {
		accs = append(accs, &Accessor{i, body})
	}
	return ReadWithGroupRowList(ctx, tables, prefix, accs, action)
}

// FilterRead reads BigTable rows from multiple Bigtable in parallel.
// This will filter the table based on given function.
func ReadWithFilter(
	ctx context.Context,
	btGroup *Group,
	prefix string,
	body [][]string,
	action func([]byte) (interface{}, error),
	filter func(*Table) bool,
) ([][]BtRow, error) {
	if btGroup == nil {
		return nil, nil
	}
	tables := btGroup.Tables(filter)
	accs := []*Accessor{}
	for i := 0; i < len(tables); i++ {
		accs = append(accs, &Accessor{i, body})
	}
	return ReadWithGroupRowList(ctx, tables, prefix, accs, action)
}

// ReadWithGroupRowList reads BigTable rows from multiple Bigtable in parallel.
// Reading is chunked as the size limit for RowSet is 500KB.
//
// Note the read could have different RowList for each import group Bigtable as
// needed by the pagination APIs.
func ReadWithGroupRowList(
	ctx context.Context,
	tables []*cbt.Table,
	prefix string,
	accs []*Accessor,
	unmarshalFunc func([]byte) (interface{}, error),
) ([][]BtRow, error) {
	if len(tables) == 0 {
		// Custom DC could have no bigtable but read all data from remote mixer
		return nil, nil
	}
	rowListMap := map[int]cbt.RowList{}
	for _, acc := range accs {
		rowListMap[acc.ImportGroup] = append(
			rowListMap[acc.ImportGroup],
			BuildRowList(prefix, acc.Body)...,
		)
	}
	// Channels for each import group read.
	chans := make(map[int]chan BtRow)
	for i := 0; i < len(tables); i++ {
		chans[i] = make(chan BtRow, len(rowListMap[i]))
	}

	errs, errCtx := errgroup.WithContext(ctx)
	readStartTime := time.Now()
	// Read from each import group tables. Note each table could have different
	// rowList in pagination APIs.
	for i := 0; i < len(tables); i++ {
		rowSet := rowListMap[i]
		rowSetSize := len(rowSet)
		if rowSetSize == 0 {
			continue
		}
		for j := 0; j <= rowSetSize/BtBatchQuerySize; j++ {
			left := j * BtBatchQuerySize
			right := (j + 1) * BtBatchQuerySize
			if right > rowSetSize {
				right = rowSetSize
			}
			rowSetPart := rowSet[left:right]
			if tables[i] != nil {
				errs.Go(readRowFn(errCtx, tables[i], rowSetPart, unmarshalFunc, chans[i], prefix))
			}
		}
	}
	err := errs.Wait()
	if err != nil {
		readDuration := time.Since(readStartTime)
		metrics.RecordBigtableReadDuration(ctx, readDuration, metrics.BigtableReadOutcomeError, prefix)
		return nil, err
	}
	for i := 0; i < len(chans); i++ {
		close(chans[i])
	}
	hasData := false
	result := [][]BtRow{}
	if tables != nil {
		for i := 0; i < len(tables); i++ {
			items := []BtRow{}
			for elem := range chans[i] {
				items = append(items, elem)
			}
			if len(items) > 0 {
				hasData = true
			}
			result = append(result, items)
		}
	}
	readDuration := time.Since(readStartTime)
	outcome := metrics.BigtableReadOutcomeOK
	if !hasData {
		outcome = metrics.BigtableReadOutcomeEmpty
	}

	metrics.RecordBigtableReadDuration(ctx, readDuration, outcome, prefix)
	return result, nil
}
