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

// Defines struct and util functions of bigtable table and groups.

package bigtable

import (
	"context"
	"log"

	pb "github.com/datacommonsorg/mixer/internal/proto"

	cbt "cloud.google.com/go/bigtable"
	"gopkg.in/yaml.v3"
)

// Table holds the bigtable name and client stub.
type Table struct {
	name     string
	isCustom bool
	table    *cbt.Table
}

// NewTable creates a new Table struct.
func NewTable(name string, table *cbt.Table, isCustom bool) *Table {
	return &Table{name: name, table: table, isCustom: isCustom}
}

// Name access the name of a table
func (t *Table) Name() string {
	return t.name
}

// IsCustom access the isCustom bit of a table
func (t *Table) IsCustom() bool {
	return t.isCustom
}

// NewBtTable creates a new cbt.Table instance.
func NewBtTable(ctx context.Context, projectID, instanceID, tableID string) (
	*cbt.Table, error) {
	btClient, err := cbt.NewClient(ctx, projectID, instanceID)
	if err != nil {
		return nil, err
	}
	return btClient.Open(tableID), nil
}

func parseTableInfo(s string) (*pb.BigtableInfo, error) {
	var bigtableInfo pb.BigtableInfo
	err := yaml.Unmarshal([]byte(s), &bigtableInfo)
	if err != nil {
		return nil, err
	}
	return &bigtableInfo, nil
}

// CreateBigtables creates a list of Bigtable from a yaml config file.
func CreateBigtables(ctx context.Context, s string, isCustom bool) ([]*Table, error) {
	bigtableInfo, err := parseTableInfo(s)
	if err != nil {
		return nil, err
	}
	var tables []*Table
	for _, name := range bigtableInfo.Tables {
		t, err := NewBtTable(
			ctx, bigtableInfo.Project, bigtableInfo.Instance, name)
		if err != nil {
			log.Fatalf("Failed to create BigTable client: %v", err)
		}
		tables = append(tables, NewTable(name, t, isCustom))
	}
	return tables, nil
}
