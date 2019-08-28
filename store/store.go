// Copyright 2019 Google LLC
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

// Package store is a library for querying datacommons backend storage.
package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	"golang.org/x/sync/errgroup"

	"github.com/datacommonsorg/mixer/base"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/translator"
	"github.com/datacommonsorg/mixer/util"

	"google.golang.org/api/option"
)

// Interface exposes the database access for mixer.
type Interface interface {
	Query(ctx context.Context,
		in *pb.QueryRequest, out *pb.QueryResponse) error

	GetPropertyLabels(ctx context.Context,
		in *pb.GetPropertyLabelsRequest, out *pb.GetPropertyLabelsResponse) error

	GetPropertyValues(ctx context.Context,
		in *pb.GetPropertyValuesRequest, out *pb.GetPropertyValuesResponse) error

	GetTriples(ctx context.Context,
		in *pb.GetTriplesRequest, out *pb.GetTriplesResponse) error

	GetPopObs(ctx context.Context,
		in *pb.GetPopObsRequest, out *pb.GetPopObsResponse) error

	GetPlaceObs(ctx context.Context,
		in *pb.GetPlaceObsRequest, out *pb.GetPlaceObsResponse) error

	GetPopulations(ctx context.Context,
		in *pb.GetPopulationsRequest, out *pb.GetPopulationsResponse) error

	GetObservations(ctx context.Context,
		in *pb.GetObservationsRequest, out *pb.GetObservationsResponse) error

	GetPlacesIn(ctx context.Context,
		in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error
}

type store struct {
	bqDb        string
	bqClient    *bigquery.Client
	bqMapping   []*base.Mapping
	outArcInfo  map[string]map[string][]translator.OutArcInfo
	inArcInfo   map[string][]translator.InArcInfo
	subTypeMap  map[string]string
	containedIn map[util.TypePair][]string
	btTable     *bigtable.Table
}

// NewStore returns an implementation of Interface backed by BigQuery and BigTable.
func NewStore(
	ctx context.Context,
	bqDataset, btTable, btProject, btInstance, projectID, schemaPath string,
	subTypeMap map[string]string, containedIn map[util.TypePair][]string,
	opts ...option.ClientOption) (Interface, error) {
	// BigQuery.
	bqClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(schemaPath)
	if err != nil {
		return nil, err
	}
	mappings := []*base.Mapping{}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".mcf") {
			mappingStr, err := ioutil.ReadFile(filepath.Join(schemaPath, f.Name()))
			if err != nil {
				return nil, err
			}
			mapping, err := translator.ParseMapping(string(mappingStr))
			if err != nil {
				return nil, err
			}
			mappings = append(mappings, mapping...)
		}
	}
	outArcInfo := map[string]map[string][]translator.OutArcInfo{}
	inArcInfo := map[string][]translator.InArcInfo{}

	// Bigtable.
	btClient, err := bigtable.NewClient(ctx, btProject, btInstance, opts...)
	if err != nil {
		return nil, err
	}

	return &store{bqDataset, bqClient, mappings, outArcInfo, inArcInfo,
		subTypeMap, containedIn, btClient.Open(btTable)}, nil
}

// bigTableReadRowsParallel reads BigTable rows in parallel, considering the size limit for RowSet
// is 500KB.
func bigTableReadRowsParallel(ctx context.Context, btTable *bigtable.Table,
	rowSet bigtable.RowSet, action func(row bigtable.Row) error) error {
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
		return fmt.Errorf("unsupported RowSet type: %v", v)
	}
	if rowSetSize == 0 {
		return nil
	}

	errs, errCtx := errgroup.WithContext(ctx)
	rowChan := make(chan []bigtable.Row, rowSetSize)
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
			btRows := []bigtable.Row{}
			if err := btTable.ReadRows(errCtx, rowSetPart,
				func(btRow bigtable.Row) bool {
					btRows = append(btRows, btRow)
					return true
				}); err != nil {
				return err
			}
			rowChan <- btRows
			return nil
		})
	}

	err := errs.Wait()
	if err != nil {
		return err
	}
	close(rowChan)

	for rows := range rowChan {
		for _, row := range rows {
			if err := action(row); err != nil {
				return err
			}
		}
	}

	return nil
}
