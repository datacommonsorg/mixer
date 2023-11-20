// Copyright 2023 Google LLC
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

package cache

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/sqldb/query"
	"github.com/datacommonsorg/mixer/internal/store"
)

const (
	blockListSvgJsonPath = "/datacommons/svg/blocklist_svg.json"
)

type SearchOptions struct {
	UseSearch           bool
	BuildSvgSearchIndex bool
}

// NewCache initializes the cache for stat var hierarchy.
func NewCache(
	ctx context.Context,
	store *store.Store,
	searchOptions SearchOptions,
) (*resource.Cache, error) {
	var blocklistSvg []string
	// Read blocklisted svg from file.
	file, err := os.ReadFile(blockListSvgJsonPath)
	if err != nil {
		log.Printf("Could not read blocklist svg file. Use empty blocklist svg list.")
		blocklistSvg = []string{}
	} else {
		if err := json.Unmarshal(file, &blocklistSvg); err != nil {
			log.Printf("Could not unmarshal blocklist svg file. Use empty blocklist svg list.")
			blocklistSvg = []string{}
		}
	}
	rawSvg, err := statvar.GetRawSvg(ctx, store)
	if err != nil {
		return nil, err
	}
	parentSvgMap := statvar.BuildParentSvgMap(rawSvg)
	result := &resource.Cache{
		RawSvg:       rawSvg,
		ParentSvg:    parentSvgMap,
		BlockListSvg: map[string]struct{}{},
	}
	for _, svg := range blocklistSvg {
		statvar.RemoveSvg(rawSvg, parentSvgMap, svg)
		result.BlockListSvg[svg] = struct{}{}
	}
	if searchOptions.UseSearch {
		if searchOptions.BuildSvgSearchIndex {
			result.SvgSearchIndex = statvar.BuildStatVarSearchIndex(rawSvg, parentSvgMap, blocklistSvg)
		}
	}
	if store.SQLClient != nil {
		customProv, err := query.GetProvenances(store.SQLClient)
		if err != nil {
			return nil, err
		}
		result.CustomProvenances = customProv
	}
	return result, nil
}
