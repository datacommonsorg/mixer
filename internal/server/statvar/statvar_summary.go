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

package statvar

import (
	"context"
	"database/sql"

	"github.com/datacommonsorg/mixer/internal/merger"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// GetStatVarSummaryHelper is a wrapper to get stat var summary.
func GetStatVarSummaryHelper(
	ctx context.Context, entities []string, store *store.Store) (
	map[string]*pb.StatVarSummary, error) {
	if store.BtGroup == nil && store.SQLClient == nil {
		return nil, status.Error(codes.Internal, "No store found")
	}

	errGroup, errCtx := errgroup.WithContext(ctx)

	btChan := make(chan map[string]*pb.StatVarSummary, 1)
	sqlChan := make(chan map[string]*pb.StatVarSummary, 1)

	if store.BtGroup != nil {
		errGroup.Go(func() error {
			bt, err := btGetStatVarSummary(errCtx, entities, store.BtGroup)
			if err != nil {
				return err
			}
			btChan <- bt
			return nil
		})
	} else {
		btChan <- map[string]*pb.StatVarSummary{}
	}

	if store.SQLClient != nil {
		errGroup.Go(func() error {
			sql, err := sqlGetStatVarSummary(entities, store.SQLClient.DB)
			if err != nil {
				return err
			}
			sqlChan <- sql
			return nil
		})
	} else {
		sqlChan <- map[string]*pb.StatVarSummary{}
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(btChan)
	close(sqlChan)

	return merger.MergeStatVarSummary(<-btChan, <-sqlChan), nil

}

func sqlGetStatVarSummary(entities []string, sqlClient *sql.DB) (
	map[string]*pb.StatVarSummary, error) {
	return sqlquery.GetStatVarSummaries(sqlClient, entities)
}

func btGetStatVarSummary(
	ctx context.Context, entities []string, btGroup *bigtable.Group) (
	map[string]*pb.StatVarSummary, error) {
	btDataList, err := bigtable.Read(
		ctx,
		btGroup,
		bigtable.BtStatVarSummary,
		[][]string{entities},
		func(jsonRaw []byte) (interface{}, error) {
			var statVarSummary pb.StatVarSummary
			if err := proto.Unmarshal(jsonRaw, &statVarSummary); err != nil {
				return nil, err
			}
			return &statVarSummary, nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.StatVarSummary{}
	// Merge strategy
	// 1. "place_type_summary": For a given place type, pick the Bigtable with the
	//    most places.
	// 2. "provenance_summary": Merge provenances from all the Bigtables.
	for _, btData := range btDataList {
		for _, row := range btData {
			dcid := row.Parts[0]
			svs, ok := row.Data.(*pb.StatVarSummary)
			if !ok {
				return nil, status.Errorf(codes.Internal, "Can not read StatVarSummary")
			}
			if _, ok := result[dcid]; !ok {
				result[dcid] = svs
				continue
			}
			res := result[dcid]
			// Pick place type summary with the most places.
			for pt := range svs.PlaceTypeSummary {
				summary, ok := res.PlaceTypeSummary[pt]
				if ok && svs.PlaceTypeSummary[pt].GetPlaceCount() < summary.GetPlaceCount() {
					continue
				}
				res.PlaceTypeSummary[pt] = svs.PlaceTypeSummary[pt]
			}
			//
			for source := range svs.ProvenanceSummary {
				// Only set the the source if it has not been found in a preferred
				// import group.
				if _, ok := res.ProvenanceSummary[source]; !ok {
					res.ProvenanceSummary[source] = svs.ProvenanceSummary[source]
				}
			}
		}
	}
	return result, nil
}
