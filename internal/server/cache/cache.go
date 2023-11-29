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
	blocklistSvgJsonPath = "/datacommons/svg/blocklist_svg.json"
)

// Options for using the Cache object
type CacheOptions struct {
	FetchSVG   bool
	SearchSVG  bool
	CustomProv bool
}

// Cache holds cached data for the mixer server.
type Cache struct {
	// parentSvgs is a map of sv/svg id to a list of its parent svgs sorted alphabetically.
	parentSvgs map[string][]string
	// rawSvgs is a map of svg id to its information.
	rawSvgs map[string]*pb.StatVarGroupNode
	// A list of blocked top level svg.
	blocklistSvgs map[string]struct{}
	// SVG search index
	svgSearchIndex *resource.SearchIndex
	// Custom provenance from SQL storage
	customProvenances map[string]*pb.Facet
	// Lock for updating cache
	mu sync.RWMutex
}

func (c *Cache) ParentSvgs() map[string][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.parentSvgs
}

func (c *Cache) RawSvgs() map[string]*pb.StatVarGroupNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rawSvgs
}

func (c *Cache) BlocklistSvgs() map[string]struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blocklistSvgs
}

func (c *Cache) SvgSearchIndex() *resource.SearchIndex {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.svgSearchIndex
}

func (c *Cache) CustomProvenances() map[string]*pb.Facet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.customProvenances
}

func (c *Cache) UpdateSVGCache(ctx context.Context, store *store.Store) error {
	var blocklistSvg []string
	// Read blocklisted svg from file.
	file, err := os.ReadFile(blocklistSvgJsonPath)
	if err != nil {
		log.Printf("Could not read blocklist svg file. Use empty blocklist svg list.")
		blocklistSvg = []string{}
	} else {
		if err := json.Unmarshal(file, &blocklistSvg); err != nil {
			log.Printf("Could not unmarshal blocklist svg file. Use empty blocklist svg list.")
			blocklistSvg = []string{}
		}
	}
	rawSvgs, err := fetcher.FetchAllSVG(ctx, store)
	if err != nil {
		return err
	}
	parentSvgs := hierarchy.BuildParentSvgMap(rawSvgs)
	// Lock and update the c.
	c.mu.Lock()
	c.rawSvgs = rawSvgs
	c.parentSvgs = parentSvgs
	c.blocklistSvgs = map[string]struct{}{}
	for _, svg := range blocklistSvg {
		hierarchy.RemoveSvg(rawSvgs, parentSvgs, svg)
		c.blocklistSvgs[svg] = struct{}{}
	}
	c.mu.Unlock()
	return nil
}

func (c *Cache) UpdateStatVarSearchIndex() {
	c.mu.Lock()
	c.svgSearchIndex = hierarchy.BuildStatVarSearchIndex(c.rawSvgs, c.parentSvgs, c.blocklistSvgs)
	c.mu.Unlock()
}

func (c *Cache) UpdateCustomCache(sqlClient *sql.DB) error {
	customProv, err := query.GetProvenances(sqlClient)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.customProvenances = customProv
	c.mu.Unlock()
	return nil
}

func (c *Cache) Update(ctx context.Context, store *store.Store) error {
	if err := c.UpdateSVGCache(ctx, store); err != nil {
		return err
	}
	c.UpdateStatVarSearchIndex()
	if store.SQLClient != nil {
		if err := c.UpdateCustomCache(store.SQLClient); err != nil {
			return err
		}
	}
	return nil
}

// NewCache initializes the cache for stat var hierarchy.
func NewCache(
	ctx context.Context,
	store *store.Store,
	options CacheOptions,
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
