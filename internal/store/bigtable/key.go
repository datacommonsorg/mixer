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

package bigtable

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
)

const (
	// BtPlaceStatsVarPrefix for place to statsvar list cache.
	BtPlaceStatsVarPrefix = "d/0/"
	// BtStatVarGroup is the key for statvar group cache.
	BtStatVarGroup = "d/1"
	// BtSVAndSVGExistence is the key for stat var and stat var group existence cache.
	BtSVAndSVGExistence = "d/2/"
	// BtPlacePagePrefix for place page cache.
	BtPlacePagePrefix = "d/4/"
	// BtProteinPagePrefix for protein page cache.
	BtProteinPagePrefix = "d/6/"
	// BtTriplesPrefix for internal GetTriples cache.
	BtTriplesPrefix = "d/7/"
	// BtArcsPrefix for internal arcs cache.
	BtArcsPrefix = "d/9/"
	// BtStatVarSummary for stat var summary cache.
	BtStatVarSummary = "d/a/"
	// BtPlacesInPrefix for GetPlacesIn cache.
	BtPlacesInPrefix = "d/c/"
	// BtPlacesMetadataPrefix for GetPlaceMetadata cache.
	BtPlacesMetadataPrefix = "d/d/"
	// BtChartDataPrefix for chart data.
	BtChartDataPrefix = "d/f/"
	// BtInPropValPrefix for in-arc prop value.
	BtInPropValPrefix = "d/l/"
	// BtOutPropValPrefix for out-arc prop value.
	BtOutPropValPrefix = "d/m/"
	// BtRelatedLocationsSameTypePrefix for related places with same type.
	BtRelatedLocationsSameTypePrefix = "d/o/"
	// BtRelatedLocationsSameTypeAndAncestorPrefix for related places with same type and ancestor.
	BtRelatedLocationsSameTypeAndAncestorPrefix = "d/q/"
	// BtRelatedLocationsSameTypePCPrefix for related places with same type, per capita.
	BtRelatedLocationsSameTypePCPrefix = "d/o0/"
	// BtRelatedLocationsSameTypeAndAncestorPCPrefix for related places with same type and ancestor,
	// per capita.
	BtRelatedLocationsSameTypeAndAncestorPCPrefix = "d/q0/"

	// BtFamily is the key for the row.
	BtFamily = "csv"
	// BtCacheLimit is the cache limit. The limit is per predicate and neighbor type.
	BtCacheLimit = 500
	// BtBatchQuerySize is the size of BigTable batch query.
	BtBatchQuerySize = 1000
)

var propValkeyPrefix = map[bool]string{
	true:  BtOutPropValPrefix,
	false: BtInPropValPrefix,
}

// BuildTriplesKey builds bigtable key for triples cache
func BuildTriplesKey(dcids []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s", BtTriplesPrefix, dcid))
	}
	return rowList
}

// BuildStatsKey builds bigtable key for stat cache
func BuildStatsKey(
	places []string, statVars []string) (
	bigtable.RowList, map[string]*util.PlaceStatVar) {
	rowList := bigtable.RowList{}
	keyToToken := map[string]*util.PlaceStatVar{}
	for _, svID := range statVars {
		for _, place := range places {
			rowKey := fmt.Sprintf("%s%s^%s", BtChartDataPrefix, place, svID)
			rowList = append(rowList, rowKey)
			keyToToken[rowKey] = &util.PlaceStatVar{Place: place, StatVar: svID}
		}
	}
	return rowList, keyToToken
}

// BuildStatSetWithinPlaceKey builds bigtable key for stat within-place cache
func BuildStatSetWithinPlaceKey(parentPlace, childType, date string, statVars []string) (
	bigtable.RowList, map[string]string) {

	rowList := bigtable.RowList{}
	keyToToken := map[string]string{}
	for _, sv := range statVars {
		rowKey := strings.Join([]string{
			BtChartDataPrefix + parentPlace,
			childType,
			sv,
			date,
		}, "^")
		rowList = append(rowList, rowKey)
		keyToToken[rowKey] = sv
	}
	return rowList, keyToToken
}

// BuildStatExistenceKey builds bigtable key for stat existence cache
func BuildStatExistenceKey(
	places []string, statVars []string) (
	bigtable.RowList, map[string]*util.PlaceStatVar) {
	rowList := bigtable.RowList{}
	keyToToken := map[string]*util.PlaceStatVar{}
	for _, sv := range statVars {
		for _, place := range places {
			rowKey := fmt.Sprintf("%s%s^%s", BtSVAndSVGExistence, place, sv)
			rowList = append(rowList, rowKey)
			keyToToken[rowKey] = &util.PlaceStatVar{Place: place, StatVar: sv}
		}
	}
	return rowList, keyToToken
}

// BuildPropertyValuesKey builds bigtable key for property value cache
func BuildPropertyValuesKey(
	dcids []string, prop string, arcOut bool) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowKey := fmt.Sprintf("%s%s^%s", propValkeyPrefix[arcOut], dcid, prop)
		rowList = append(rowList, rowKey)
	}
	return rowList
}

// BuildPropertyLabelKey builds bigtable key for property label cache
func BuildPropertyLabelKey(dcids []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s", BtArcsPrefix, dcid))
	}
	return rowList
}

// BuildPlaceInKey builds bigtable key for place in cache
func BuildPlaceInKey(dcids []string, placeType string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(
			rowList, fmt.Sprintf("%s%s^%s", BtPlacesInPrefix, dcid, placeType))
	}
	return rowList
}

// BuildPlaceStatsVarKey builds bigtable key for place stat vars cache
func BuildPlaceStatsVarKey(dcids []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(
			rowList, fmt.Sprintf("%s%s", BtPlaceStatsVarPrefix, dcid))
	}
	return rowList
}

// BuildPlaceMetaDataKey builds Bigtable key for place metadata cache
func BuildPlaceMetaDataKey(places []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, place := range places {
		rowList = append(rowList, fmt.Sprintf("%s%s", BtPlacesMetadataPrefix, place))
	}
	return rowList
}

// BuildStatVarSummaryKey builds bigtable key for place stat var summary cache
func BuildStatVarSummaryKey(statVars []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, sv := range statVars {
		rowList = append(rowList, fmt.Sprintf("%s%s", BtStatVarSummary, sv))
	}
	return rowList
}
