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

package node

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// GetPropertiesHelper is a wrapper to fetch node properties from BT store.
func GetPropertiesHelper(
	ctx context.Context,
	nodes []string,
	store *store.Store,
	direction string,
) (map[string][]string, error) {
	result := map[string][]string{}
	for _, node := range nodes {
		result[node] = []string{}
	}
	// Fetch data from Bigtable
	if store.BtGroup != nil {
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			bigtable.BtArcsPrefix,
			[][]string{nodes},
			func(jsonRaw []byte) (interface{}, error) {
				var propLabels pb.PropertyLabels
				if err := proto.Unmarshal(jsonRaw, &propLabels); err != nil {
					return nil, err
				}
				return &propLabels, nil
			},
		)
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			labels := [][]string{}
			// Merge cache value from all import groups
			for _, btData := range btDataList {
				for _, row := range btData {
					if row.Parts[0] == node {
						if direction == util.DirectionIn {
							if item := row.Data.(*pb.PropertyLabels).InLabels; item != nil {
								labels = append(labels, item)
							}
						} else {
							if item := row.Data.(*pb.PropertyLabels).OutLabels; item != nil {
								labels = append(labels, item)
							}
						}
					}
				}
			}
			result[node] = util.MergeDedupe(labels...)
		}
	}
	// Fetch data from SQLite
	if sqldb.IsConnected(&store.SQLClient) {
		rows, err := store.SQLClient.GetNodePredicates(ctx, nodes, direction)
		if err != nil {
			return nil, err
		}

		tmp := map[string][]string{}
		for _, node := range nodes {
			tmp[node] = []string{}
		}
		for _, row := range rows {
			tmp[row.Node] = append(tmp[row.Node], row.Predicate)
		}
		for _, node := range nodes {
			result[node] = util.MergeDedupe(result[node], tmp[node])
		}
	}
	return result, nil
}
