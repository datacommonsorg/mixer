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
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// GetPropertiesHelper is a wrapper to fetch entity properties from BT store.
func GetPropertiesHelper(
	ctx context.Context,
	entities []string,
	store *store.Store,
) (map[string]*pb.PropertyLabels, error) {
	rowList := bigtable.BuildPropertyLabelKey(entities)
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(jsonRaw []byte) (interface{}, error) {
			var propLabels pb.PropertyLabels
			if err := proto.Unmarshal(jsonRaw, &propLabels); err != nil {
				return nil, err
			}
			return &propLabels, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.PropertyLabels{}
	for _, entity := range entities {
		result[entity] = &pb.PropertyLabels{
			InLabels:  []string{},
			OutLabels: []string{},
		}
		inLabelList := [][]string{}
		outLabelList := [][]string{}
		// Merge cache value from base and branch caches
		for _, btData := range btDataList {
			if data, ok := btData[entity]; ok {
				if item := data.(*pb.PropertyLabels).InLabels; item != nil {
					inLabelList = append(inLabelList, item)
				}
				if item := data.(*pb.PropertyLabels).OutLabels; item != nil {
					outLabelList = append(outLabelList, item)
				}
			}
		}
		result[entity].InLabels = util.MergeDedupe(inLabelList...)
		result[entity].OutLabels = util.MergeDedupe(outLabelList...)
	}
	return result, nil
}
