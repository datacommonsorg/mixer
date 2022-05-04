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

package propertyvalues

import (
	"fmt"

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
)

func buildRowList(entities, properties []string, arcOut bool, page int32) cbt.RowList {
	prefix := bigtable.BtPagedPropValOut
	if !arcOut {
		prefix = bigtable.BtPagedPropValIn
	}
	rowList := cbt.RowList{}
	for _, entity := range entities {
		for _, property := range properties {
			rowKey := fmt.Sprintf("%s%s^%s^%d", prefix, entity, property, page)
			rowList = append(rowList, rowKey)
		}
	}
	return rowList
}

func buildSimpleRequestRowList(
	entity, property string, arcOut bool, page int32) cbt.RowList {
	return buildRowList([]string{entity}, []string{property}, arcOut, page)
}

// func buildMultiEntityRowList(
// 	entities []string, property string, arcOut bool, page int32) cbt.RowList {
// 	return buildRowList(entities, []string{property}, arcOut, page)
// }

// func buildMultiPropertyRowList(
// 	entity string, properties []string, arcOut bool, page int32) cbt.RowList {
// 	return buildRowList([]string{entity}, properties, arcOut, page)
// }
