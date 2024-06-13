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

package count

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/statvar/formula"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

func countInternal(
	ctx context.Context,
	st *store.Store,
	cachedata *cache.Cache,
	svOrSvgs []string,
	entities []string,
) (map[string]map[string]int32, error) {
	// Initialize result
	result := map[string]map[string]int32{}
	for _, svOrSvg := range svOrSvgs {
		result[svOrSvg] = map[string]int32{}
	}
	if st.BtGroup != nil {
		btDataList, err := bigtable.Read(
			ctx,
			st.BtGroup,
			bigtable.BtSVAndSVGExistence,
			[][]string{entities, svOrSvgs},
			func(jsonRaw []byte) (interface{}, error) {
				var statVarExistence pb.EntityStatVarExistence
				if err := proto.Unmarshal(jsonRaw, &statVarExistence); err != nil {
					return nil, err
				}
				return &statVarExistence, nil
			},
		)
		if err != nil {
			return nil, err
		}

		// Populate the count
		for _, btData := range btDataList {
			for _, row := range btData {
				e := row.Parts[0]
				svOrSvg := row.Parts[1]
				c := row.Data.(*pb.EntityStatVarExistence)
				descSVCount := c.GetDescendentStatVarCount()
				if _, ok := result[svOrSvg][e]; !ok {
					// When c.GetDescendentStatVarCount() is 0, v represents an stat var
					// (not a stat var group). In this case the check here is necessary,
					// otherwise the proto default 0 is compared, and this map field will
					// not be populated.
					result[svOrSvg][e] = descSVCount
				} else if descSVCount > result[svOrSvg][e] {
					result[svOrSvg][e] = descSVCount
				}
			}
		}
	}
	if st.SQLClient != nil {
		// all SV contains the SV in the request and child SV in the request SVG.
		allSV := []string{}
		for _, svOrSvg := range svOrSvgs {
			if _, ok := cachedata.SQLExistenceMap()[util.EntityVariable{V: svOrSvg}]; ok {
				allSV = append(allSV, svOrSvg)
			}
		}
		requestSV := map[string]struct{}{}
		for _, sv := range allSV {
			requestSV[sv] = struct{}{}
		}
		requestSVG, err := sqlquery.CheckVariableGroups(st.SQLClient, svOrSvgs)
		if err != nil {
			return nil, err
		}
		ancestorSVG := map[string][]string{}
		for _, svg := range requestSVG {
			descendantSVs := util.GetAllDescendentSV(cachedata.RawSvgs(), svg)
			for _, sv := range descendantSVs {
				allSV = append(allSV, sv)
				if _, ok := ancestorSVG[sv]; !ok {
					ancestorSVG[sv] = []string{}
				}
				ancestorSVG[sv] = append(ancestorSVG[sv], svg)
			}
		}
		if len(allSV) == 0 {
			return result, nil
		}
		// Remove duplicate from directly queried SV and SV under queried SVG
		allSV = util.MergeDedupe(allSV, []string{})
		for _, e := range entities {
			for _, v := range allSV {
				if _, ok := cachedata.SQLExistenceMap()[util.EntityVariable{E: e, V: v}]; ok {
					// This is an sv in the original query variable list.
					if _, ok := requestSV[v]; ok {
						result[v][e] = 0
					}
					// Add count for each SVG with descendants.
					for _, ancestor := range ancestorSVG[v] {
						if _, ok := result[ancestor]; !ok {
							result[ancestor] = map[string]int32{}
						}
						result[ancestor][e] += 1
					}
				}
			}
		}
	}
	return result, nil
}

// Count checks if entities have data for stat vars and stat var groups.
//
// Returns a two level map from sv/svg dcid to entity dcid to the number of
// sv with data. For a given sv/svg, if an entity has no data, it will
// not show up in the second level map.
func Count(
	ctx context.Context,
	st *store.Store,
	cachedata *cache.Cache,
	svOrSvgs []string,
	entities []string,
) (map[string]map[string]int32, error) {
	result, err := countInternal(ctx, st, cachedata, svOrSvgs, entities)
	if err != nil {
		return nil, err
	}
	// Check for count for computed observations.
	// Use counts of formula variables as a heuristic.
	for _, dcid := range svOrSvgs {
		formulas, ok := cachedata.SVFormula()[dcid]
		if !ok {
			continue
		}
		for _, entity := range entities {
			if _, ok := result[dcid][entity]; ok {
				continue
			}
			for _, f := range formulas {
				variableFormula, err := formula.NewVariableFormula(f)
				if err != nil {
					return nil, err
				}
				calculatedCount, err := countInternal(
					ctx,
					st,
					cachedata,
					variableFormula.StatVars,
					[]string{entity},
				)
				if err != nil {
					return nil, err
				}
				allExist := true
				for _, entityCount := range calculatedCount {
					if _, ok := entityCount[entity]; !ok {
						allExist = false
						break
					}
				}
				if allExist {
					result[dcid][entity] = 0
					break
				}
			}
		}
	}
	return result, nil
}
