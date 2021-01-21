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
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
)

// Store holds the handlers to BigQuery and Bigtable
type Store struct {
	BqClient    *bigquery.Client
	baseTable   *bigtable.Table
	branchTable *bigtable.Table
	branchLock  sync.RWMutex
}

// BaseBt is the accessor for base bigtable
func (st *Store) BaseBt() *bigtable.Table {
	return st.baseTable
}

// BranchBt is the accessor for branch bigtable
func (st *Store) BranchBt() *bigtable.Table {
	st.branchLock.RLock()
	defer st.branchLock.RUnlock()
	return st.branchTable
}

// UpdateBranchBt updates the branch bigtable
func (st *Store) UpdateBranchBt(branchTable *bigtable.Table) {
	st.branchLock.Lock()
	defer st.branchLock.Unlock()
	st.branchTable = branchTable
}

// NewStore creates a new store.
func NewStore(
	bqClient *bigquery.Client,
	baseTable *bigtable.Table,
	branchTable *bigtable.Table) *Store {
	return &Store{
		BqClient:    bqClient,
		baseTable:   baseTable,
		branchTable: branchTable,
	}
}
