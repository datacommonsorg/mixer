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

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
)

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStats(
	ctx context.Context,
	store *store.Store,
	rowList cbt.RowList,
	keyTokens map[string]*util.PlaceStatVar) (
	map[string]map[string]*ObsTimeSeries, error) {

	keyToTokenFn := tokenFn(keyTokens)
	baseDataMap, branchDataMap, err := bigtable.Read(
		ctx, store.BtGroup, rowList, convertToObsSeries, tokenFn(keyTokens), true, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*ObsTimeSeries{}
	for _, psv := range keyTokens {
		if _, ok := result[psv.Place]; !ok {
			result[psv.Place] = map[string]*ObsTimeSeries{}
		}
		if _, ok := result[psv.StatVar]; !ok {
			result[psv.Place][psv.StatVar] = nil
		}
	}
	for _, rowKey := range rowList {
		token, _ := keyToTokenFn(rowKey)
		psv := keyTokens[rowKey]
		if data, ok := branchDataMap[token]; ok {
			result[psv.Place][psv.StatVar] = data.(*ObsTimeSeries)
		} else if data, ok := baseDataMap[token]; ok {
			result[psv.Place][psv.StatVar] = data.(*ObsTimeSeries)
		}
	}
	return result, nil
}

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStatsPb(
	ctx context.Context,
	store *store.Store,
	rowList cbt.RowList,
	keyTokens map[string]*util.PlaceStatVar) (
	map[string]map[string]*pb.ObsTimeSeries, error) {

	keyToTokenFn := tokenFn(keyTokens)
	baseDataMap, branchDataMap, err := bigtable.Read(
		ctx, store.BtGroup, rowList, convertToObsSeriesPb, keyToTokenFn, true, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*pb.ObsTimeSeries{}
	for _, psv := range keyTokens {
		if _, ok := result[psv.Place]; !ok {
			result[psv.Place] = map[string]*pb.ObsTimeSeries{}
		}
		if _, ok := result[psv.StatVar]; !ok {
			result[psv.Place][psv.StatVar] = nil
		}
	}

	for _, rowKey := range rowList {
		token, _ := keyToTokenFn(rowKey)
		psv := keyTokens[rowKey]
		if data, ok := branchDataMap[token]; ok && data != nil {
			result[psv.Place][psv.StatVar] = data.(*pb.ObsTimeSeries)
		} else if data, ok := baseDataMap[token]; ok && data != nil {
			result[psv.Place][psv.StatVar] = data.(*pb.ObsTimeSeries)
		}
	}
	return result, nil
}

// readStatCollection reads and process ObsCollection cache from BigTable
// in parallel.
func readStatCollection(
	ctx context.Context,
	store *store.Store,
	rowList cbt.RowList,
	keyTokens map[string]string) (
	map[string]*pb.ObsCollection, error) {

	baseDataMap, branchDataMap, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		convertToObsCollection,
		func(rowKey string) (string, error) {
			return keyTokens[rowKey], nil
		},
		true, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.ObsCollection{}
	for _, rowKey := range rowList {
		token := keyTokens[rowKey]
		if data, ok := branchDataMap[token]; ok && data != nil {
			result[token] = data.(*pb.ObsCollection)
		} else if data, ok := baseDataMap[token]; ok && data != nil {
			result[token] = data.(*pb.ObsCollection)
		} else {
			result[token] = nil
		}
	}
	return result, nil
}
