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
	"github.com/datacommonsorg/mixer/internal/sqldb/query"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

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
		var allSV []string
		requestSV := map[string]struct{}{}
		allSV, err := query.CheckVariables(st.SQLClient, svOrSvgs)
		for _, sv := range allSV {
			requestSV[sv] = struct{}{}
		}
		if err != nil {
			return nil, err
		}
		requestSVG, err := query.CheckVariableGroups(st.SQLClient, svOrSvgs)
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

		observationCount, err := query.CountObservation(st.SQLClient, entities, allSV)
		if err != nil {
			return nil, err
		}
		for v, eCount := range observationCount {
			for e, c := range eCount {
				if c == 0 {
					continue
				}
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
	return result, nil
}
