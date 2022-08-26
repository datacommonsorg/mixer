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
	"cloud.google.com/go/bigtable"
)

const (
	// BtPlaceStatsVarPrefix for place to statsvar list cache.
	BtPlaceStatsVarPrefix = "d/0/"
	// BtStatVarGroup is the key for statvar group cache.
	BtStatVarGroup = "d/1"
	// BtSVAndSVGExistence is the key for stat var and stat var group existence cache.
	BtSVAndSVGExistence = "d/2/"
	// BtObsTimeSeries is the key for obs time series cache.
	BtObsTimeSeries = "d/3/"
	// BtPlacePagePrefix for place page cache.
	BtPlacePagePrefix = "d/4/"
	// BtBioPagePrefix for biology page cache.
	BtBioPagePrefix = "d/6/"
	// BtPropType is the prefix for getting the types for given entity and property
	BtPropType = "d/7/"
	// BtObsCollectionDateFrequency for obs collection cache that contains the frequency of each
	// date across places.
	BtObsCollectionDateFrequency = "d/8/"
	// BtArcsPrefix for internal arcs cache.
	BtArcsPrefix = "d/9/"
	// BtStatVarSummary for stat var summary cache.
	BtStatVarSummary = "d/a/"
	// BtPlacesInPrefix for GetPlacesIn cache.
	BtPlacesInPrefix = "d/c/"
	// BtPlacesMetadataPrefix for GetPlaceMetadata cache.
	BtPlacesMetadataPrefix = "d/d/"
	// BtObsCollection for obs collection cache.
	BtObsCollection = "d/e/"
  // BtPlacePageCategoricalPrefix for place page Categorical cache.
	BtPlacePageCategoricalPrefix = "d/f/"
	// BtPagedPropTypeValIn for in-arc paged entities by type.
	// Key: <dcid^predicate^type^page>
	BtPagedPropTypeValIn = "d/l/"
	// BtPagedPropTypeValOut for out-arc paged entities.
	// Key: <dcid^predicate^type^page>
	BtPagedPropTypeValOut = "d/m/"
	// BtRelatedLocationsSameTypePrefix for related places with same type.
	BtRelatedLocationsSameTypePrefix = "d/o/"
	// BtRelatedLocationsSameTypeAndAncestorPrefix for related places with same type and ancestor.
	BtRelatedLocationsSameTypeAndAncestorPrefix = "d/q/"
	// BtRelatedLocationsSameTypePCPrefix for related places with same type, per capita.
	BtRelatedLocationsSameTypePCPrefix = "d/o0/"
	// BtRelatedLocationsSameTypeAndAncestorPCPrefix for related places with same type and ancestor,
	// per capita.
	BtRelatedLocationsSameTypeAndAncestorPCPrefix = "d/q0/"

	// BtReconIDMapPrefix for ID mapping for ID-based recon. The key excludes DCID.
	BtReconIDMapPrefix = "d/5/"
	// BtCoordinateReconPrefix for coordinate recon.
	BtCoordinateReconPrefix = "d/b/"

	// BtFamily is the key for the row.
	BtFamily = "csv"
	// BtCacheLimit is the cache limit. The limit is per predicate and neighbor type.
	BtCacheLimit = 500
	// BtBatchQuerySize is the size of BigTable batch query.
	BtBatchQuerySize = 1000
)

// Accessor represents data used to access bigtable row.
type Accessor struct {
	// Import group table index.
	ImportGroup int
	// A list of body components, each component contains all the key element
	// for that part.
	// One key is constructed by concatenating one element from each component.
	Body [][]string
}

// BuildRowList builds row list from BT prefix and token components.
func BuildRowList(prefix string, body [][]string) bigtable.RowList {
	rowList := bigtable.RowList{prefix}
	for idx, component := range body {
		c := ""
		if idx > 0 {
			c = "^"
		}
		tmp := rowList
		rowList = bigtable.RowList{}
		for _, element := range component {
			for _, key := range tmp {
				rowList = append(rowList, key+c+element)
			}
		}
	}
	return rowList
}
