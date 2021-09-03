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

package store

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/datacommonsorg/mixer/internal/parser"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/api/iterator"
)

// MemDb holds imported data in memory.
type MemDb struct {
	statSeries map[string]map[string]pb.SeriesMap
}

// NewMemDb initialize a MemDb instance.
func NewMemDb() *MemDb {
	return &MemDb{
		statSeries: map[string]map[string]pb.SeriesMap{},
	}
}

// LoadFromGcs loads tmcf + csv files into memory database
func (memDb *MemDb) LoadFromGcs(ctx context.Context, bucket string) error {
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	// The bucket should contain one tmcf and multiple compatible csv files.
	bkt := gcsClient.Bucket(bucket)
	objectQuery := &storage.Query{Prefix: ""}
	var objects []string
	it := bkt.Objects(ctx, objectQuery)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		objects = append(objects, attrs.Name)
	}
	// Read TMCF
	var schemaMapping map[string]*parser.TableSchema
	for _, object := range objects {
		if strings.HasSuffix(object, ".tmcf") {
			obj := bkt.Object(object)
			r, err := obj.NewReader(ctx)
			if err != nil {
				return err
			}
			defer r.Close()
			buf := new(strings.Builder)
			if _, err := io.Copy(buf, r); err != nil {
				return err
			}
			schemaMapping, err = parser.ParseTmcf(buf.String())
			if err != nil {
				return err
			}
			break
		}
	}

	for _, object := range objects {
		if strings.HasSuffix(object, ".csv") {
			obj := bkt.Object(object)
			r, err := obj.NewReader(ctx)
			if err != nil {
				return err
			}
			defer r.Close()
			tableName := strings.TrimSuffix(object, ".csv")
			csvReader := csv.NewReader(r)
			for {
				row, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				addRow(schemaMapping[tableName], memDb.statSeries, row)
			}
		}
	}
	return nil
}

// addRecord add one csv row to memdb
func addRow(
	schemaMapping *parser.TableSchema,
	statSeries map[string]map[string]pb.SeriesMap,
	row []string,
) {
	// TODO: implement csv parsing and actual data loading
	// for _, cell := range row {
	// 	cell = strings.TrimSpace(cell)
	// }
	log.Println(row)
}
