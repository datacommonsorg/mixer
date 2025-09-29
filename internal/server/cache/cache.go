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

	"github.com/datacommonsorg/mixer/internal/metrics"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/statvar/fetcher"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
)

const (
	blocklistSvgJsonPath = "/datacommons/svg/blocklist_svg.json"
)

// Options for using the Cache object
type CacheOptions struct {
	FetchSVG       bool
	SearchSVG      bool
	CacheSQL       bool
	CacheSVFormula bool
}

// Cache holds cached data for the mixer server.
// TODO Instrument this class
type Cache struct {
	// parentSvgs is a map of sv/svg id to a list of its parent svgs sorted alphabetically.
	parentSvgs map[string][]string
	// rawSvgs is a map of svg id to its information.
	rawSvgs map[string]*pb.StatVarGroupNode
	// A list of blocked top level svg.
	blocklistSvgs map[string]struct{}
	// SVG search index
	svgSearchIndex *resource.SearchIndex
	// Provenance from SQL storage
	sqlProvenances map[string]*pb.Facet
	// SQL database entity, variable existence pairs
	sqlExistenceMap map[util.EntityVariable]struct{}
	// Map of SV dcid to list of inputPropertyExpressions for StatisticalCalculations.
	svFormulas map[string][]string
	// CacheOption for this Cache object
	options CacheOptions
}

func (c *Cache) ParentSvgs(ctx context.Context) map[string][]string {
	metrics.RecordCachedataRead(ctx, "parent_svgs")
	return c.parentSvgs
}

func (c *Cache) RawSvgs(ctx context.Context) map[string]*pb.StatVarGroupNode {
	metrics.RecordCachedataRead(ctx, "raw_svgs")
	return c.rawSvgs
}

func (c *Cache) BlocklistSvgs(ctx context.Context) map[string]struct{} {
	metrics.RecordCachedataRead(ctx, "blocklist_svgs")
	return c.blocklistSvgs
}

func (c *Cache) SvgSearchIndex(ctx context.Context) *resource.SearchIndex {
	metrics.RecordCachedataRead(ctx, "svg_search_index")
	return c.svgSearchIndex
}

func (c *Cache) SQLProvenances(ctx context.Context) map[string]*pb.Facet {
	metrics.RecordCachedataRead(ctx, "sql_provenances")
	return c.sqlProvenances
}

func (c *Cache) SQLExistenceMap(ctx context.Context) map[util.EntityVariable]struct{} {
	metrics.RecordCachedataRead(ctx, "sql_existence_map")
	return c.sqlExistenceMap
}

func (c *Cache) SVFormula(ctx context.Context) map[string][]string {
	metrics.RecordCachedataRead(ctx, "sv_formulas")
	return c.svFormulas
}

func (c *Cache) Options() *CacheOptions {
	return &c.options
}

// NewCache initializes the cache for stat var hierarchy.
func NewCache(
	ctx context.Context,
	store *store.Store,
	options CacheOptions,
	metadata *resource.Metadata,
) (*Cache, error) {
	c := &Cache{options: options}
	if options.FetchSVG {
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
			return nil, err
		}
		parentSvgs := hierarchy.BuildParentSvgMap(rawSvgs)
		c.rawSvgs = rawSvgs
		c.parentSvgs = parentSvgs
		c.blocklistSvgs = map[string]struct{}{}
		for _, svg := range blocklistSvg {
			hierarchy.RemoveSvg(rawSvgs, parentSvgs, svg)
			c.blocklistSvgs[svg] = struct{}{}
		}
	}

	if options.SearchSVG {
		c.svgSearchIndex = hierarchy.BuildStatVarSearchIndex(c.rawSvgs, c.parentSvgs, c.blocklistSvgs)
	}

	if options.CacheSQL {
		sqlProv, err := sqlquery.GetProvenances(ctx, &store.SQLClient)
		if err != nil {
			return nil, err
		}
		c.sqlProvenances = sqlProv
		sqlExistenceMap, err := sqlquery.EntityVariableExistence(ctx, &store.SQLClient)
		if err != nil {
			return nil, err
		}
		c.sqlExistenceMap = sqlExistenceMap
	}

	if options.CacheSVFormula {
		svFormulas, err := fetcher.FetchFormulas(ctx, store, metadata)
		if err != nil {
			return nil, err
		}
		c.svFormulas = svFormulas
	}
	return c, nil
}

// NewDataSourceCache initializes the in-memory mixer cache from DataSources.
func NewDataSourceCache(
	ctx context.Context,
	ds *datasources.DataSources,
	options CacheOptions,
) (*Cache, error) {
	c := &Cache{options: options}
	if options.CacheSVFormula {
		svFormulas, err := fetcher.FetchSVFormulas(ctx, ds)
		if err != nil {
			return nil, err
		}
		c.svFormulas = svFormulas
	}

	// TODO: Add support for other options if needed.

	return c, nil
}
