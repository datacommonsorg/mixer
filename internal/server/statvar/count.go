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

package statvar

import (
	"context"
	"fmt"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
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
	cache *resource.Cache,
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
	if st.SQLiteClient != nil {
		allSV := []string{}
		querySV := map[string]struct{}{}
		// Find all the sv that are in the sqlite database
		query := fmt.Sprintf(
			`
				SELECT DISTINCT(variable) FROM observations o
				WHERE o.variable IN (%s)
			`,
			util.SQLInParam(len(svOrSvgs)),
		)
		// Execute query
		rows, err := st.SQLiteClient.Query(
			query,
			util.ConvertArgs(svOrSvgs)...,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		// Process the query result
		for rows.Next() {
			var sv string
			err = rows.Scan(&sv)
			if err != nil {
				return nil, err
			}
			allSV = append(allSV, sv)
			querySV[sv] = struct{}{}
		}

		// Find all the svg that are in the sqlite database
		ancestorSVG := map[string][]string{}
		// Execute query
		query = fmt.Sprintf(
			`
				SELECT DISTINCT(subject_id) FROM triples
				WHERE predicate = "typeOf"
				AND subject_id IN (%s)
				AND object_id = 'StatVarGroup';
			`,
			util.SQLInParam(len(svOrSvgs)),
		)
		rows, err = st.SQLiteClient.Query(
			query,
			util.ConvertArgs(svOrSvgs)...,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		// Process the query result
		for rows.Next() {
			var svg string
			err = rows.Scan(&svg)
			if err != nil {
				return nil, err
			}
			descendantSVs := getAllDescendentSV(cache.RawSvg, svg)
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
		allSV = util.MergeDedupe(allSV, []string{})

		// Query the count for entity, variable pairs
		query = fmt.Sprintf(
			`
				WITH entity_list(entity) AS (
						VALUES %s
				),
				variable_list(variable) AS (
						VALUES %s
				),
				all_pairs AS (
						SELECT e.entity, v.variable
						FROM entity_list e
						CROSS JOIN variable_list v
				)
				SELECT a.entity, a.variable, COUNT(o.entity)
				FROM all_pairs a
				LEFT JOIN observations o ON a.entity = o.entity AND a.variable = o.variable
				GROUP BY a.entity, a.variable;
			`,
			util.SQLValuesParam(len(entities)),
			util.SQLValuesParam(len(allSV)),
		)
		args := entities
		args = append(args, allSV...)

		// Execute query
		rows, err = st.SQLiteClient.Query(query, util.ConvertArgs(args)...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var e, v string
			var count int
			err = rows.Scan(&e, &v, &count)
			if err != nil {
				return nil, err
			}
			if count == 0 {
				continue
			}
			// This is an sv in the original query variable list.
			if _, ok := querySV[v]; ok {
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

	return result, nil
}
