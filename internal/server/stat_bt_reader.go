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
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStats(
	ctx context.Context,
	btTable *bigtable.Table,
	rowList bigtable.RowList,
	keyTokens map[string]*placeStatVar) (
	map[string]map[string]*ObsTimeSeries, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsSeries, tokenFn(keyTokens),
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*ObsTimeSeries{}
	for token, data := range dataMap {
		parts := strings.Split(token, "^")
		place := parts[0]
		statVar := parts[1]
		if result[place] == nil {
			result[place] = map[string]*ObsTimeSeries{}
		}
		s, ok := data.(*ObsTimeSeries)
		if ok {
			result[place][statVar] = s
		} else {
			result[place][statVar] = nil
		}
	}
	return result, nil
}

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStatsPb(
	ctx context.Context,
	btTable *bigtable.Table,
	rowList bigtable.RowList,
	keyTokens map[string]*placeStatVar) (
	map[string]map[string]*pb.ObsTimeSeries, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsSeriesPb, tokenFn(keyTokens),
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*pb.ObsTimeSeries{}
	for token, data := range dataMap {
		parts := strings.Split(token, "^")
		place := parts[0]
		statVar := parts[1]
		if result[place] == nil {
			result[place] = map[string]*pb.ObsTimeSeries{}
		}
		s, ok := data.(*pb.ObsTimeSeries)
		if ok {
			result[place][statVar] = s
		} else {
			result[place][statVar] = nil
		}
	}
	return result, nil
}

// readStatCollection reads and process ObsCollection cache from BigTable
// in parallel.
func readStatCollection(
	ctx context.Context,
	btTable *bigtable.Table,
	rowList bigtable.RowList,
	keyTokens map[string]string) (
	map[string]*pb.ObsCollection, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsCollection,
		func(rowKey string) (string, error) {
			return keyTokens[rowKey], nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.ObsCollection{}
	for token, data := range dataMap {
		collection, ok := data.(*pb.ObsCollection)
		if ok {
			result[token] = collection
		} else {
			result[token] = nil
		}
	}
	return result, nil
}
