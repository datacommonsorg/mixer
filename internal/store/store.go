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
	"database/sql"

	"cloud.google.com/go/bigquery"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/files"
)

// Store holds the handlers to BigQuery and Bigtable
type Store struct {
	BqClient        *bigquery.Client
	BtGroup         *bigtable.Group
	RecogPlaceStore *files.RecogPlaceStore
	SQLiteClient    *sql.DB
}

// NewStore creates a new store.
func NewStore(
	bqClient *bigquery.Client,
	sqlClient *sql.DB,
	tables []*bigtable.Table,
	branchTableName string,
	metadata *resource.Metadata,
) (*Store, error) {
	// Table validation happens when creating the store
	validTables := []*bigtable.Table{}
	for _, t := range tables {
		if t != nil {
			validTables = append(validTables, t)
		}
	}
	var btGroup *bigtable.Group
	if len(validTables) > 0 {
		btGroup = bigtable.NewGroup(validTables, branchTableName, metadata)
	}
	recogPlaceStore, err := files.LoadRecogPlaceStore()
	if err != nil {
		return nil, err
	}
	return &Store{
		BqClient:        bqClient,
		SQLiteClient:    sqlClient,
		BtGroup:         btGroup,
		RecogPlaceStore: recogPlaceStore,
	}, nil
}
