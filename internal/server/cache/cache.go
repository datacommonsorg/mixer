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
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"sync"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/statvar/fetcher"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	"github.com/datacommonsorg/mixer/internal/sqldb/query"
	"github.com/datacommonsorg/mixer/internal/store"
)

const (
	blockListSvgJsonPath = "/datacommons/svg/blocklist_svg.json"
)

// Options for using the Cache object
type Options struct {
	FetchSVG   bool
	SearchSVG  bool
	CustomProv bool
}

// Cache holds cached data for the mixer server.
type Cache struct {
	// ParentSvg is a map of sv/svg id to a list of its parent svgs sorted alphabetically.
	parentSvg map[string][]string
	// SvgInfo is a map of svg id to its information.
	rawSvg map[string]*pb.StatVarGroupNode
	// A list of blocked top level svg.
	blockListSvg map[string]struct{}
	// SVG search index
	svgSearchIndex *resource.SearchIndex
	// Custom provenance from SQL storage
	customProvenances map[string]*pb.Facet
	// Lock for updating cache
	mu sync.RWMutex
}

func (cache *Cache) ParentSvg() map[string][]string {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.parentSvg
}

func (cache *Cache) RawSvg() map[string]*pb.StatVarGroupNode {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.rawSvg
}

func (cache *Cache) BlockListSvg() map[string]struct{} {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.blockListSvg
}

func (cache *Cache) SvgSearchIndex() *resource.SearchIndex {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.svgSearchIndex
}

func (cache *Cache) CustomProvenances() map[string]*pb.Facet {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.customProvenances
}

func (cache *Cache) UpdateSVGCache(ctx context.Context, store *store.Store) error {
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
	rawSvg, err := fetcher.FetchAllSVG(ctx, store)
	if err != nil {
		return err
	}
	parentSvgMap := hierarchy.BuildParentSvgMap(rawSvg)
	// Lock and update the cache.
	cache.mu.Lock()
	cache.rawSvg = rawSvg
	cache.parentSvg = parentSvgMap
	cache.blockListSvg = map[string]struct{}{}
	for _, svg := range blocklistSvg {
		hierarchy.RemoveSvg(rawSvg, parentSvgMap, svg)
		cache.blockListSvg[svg] = struct{}{}
	}
	cache.mu.Unlock()
	return nil
}

func (cache *Cache) UpdateStatVarSearchIndex() {
	cache.mu.Lock()
	cache.svgSearchIndex = hierarchy.BuildStatVarSearchIndex(cache.rawSvg, cache.parentSvg, cache.blockListSvg)
	cache.mu.Unlock()
}

func (cache *Cache) UpdateCustomCache(sqlClient *sql.DB) error {
	customProv, err := query.GetProvenances(sqlClient)
	if err != nil {
		return err
	}
	cache.mu.Lock()
	cache.customProvenances = customProv
	cache.mu.Unlock()
	return nil
}

// NewCache initializes the cache for stat var hierarchy.
func NewCache(
	ctx context.Context,
	store *store.Store,
	options Options,
) (*Cache, error) {
	result := &Cache{}
	if options.FetchSVG {
		if err := result.UpdateSVGCache(ctx, store); err != nil {
			return nil, err
		}
	}
	if options.SearchSVG {
		result.UpdateStatVarSearchIndex()
	}
	if options.CustomProv {
		if err := result.UpdateCustomCache(store.SQLClient); err != nil {
			return nil, err
		}
	}
	return result, nil
}
