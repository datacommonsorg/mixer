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

package fetcher

import (
	"context"
	"database/sql"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	"github.com/datacommonsorg/mixer/internal/sqldb/sqlquery"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/proto"
)

// FetchAllSVG fetches entire SVG from storage
func FetchAllSVG(
	ctx context.Context,
	store *store.Store,
) (map[string]*pb.StatVarGroupNode, error) {
	result := map[string]*pb.StatVarGroupNode{}
	if store.BtGroup != nil {
		// Read stat var group cache from the allowed import group table.
		btDataList, err := bigtable.ReadWithFilter(
			ctx,
			store.BtGroup,
			bigtable.BtStatVarGroup,
			[][]string{{""}},
			func(jsonRaw []byte) (interface{}, error) {
				var svgResp pb.StatVarGroups
				if err := proto.Unmarshal(jsonRaw, &svgResp); err != nil {
					return nil, err
				}
				return &svgResp, nil
			},
			// Only use svg from "schema", "experimental" and custom import groups.
			// These two import groups have the latest and wanted sv/svgs. We don't
			// want to include those in "infrequent" etc that may have stale sv/svg.
			func(t *bigtable.Table) bool {
				return (strings.HasPrefix(t.Name(), "schema") ||
					strings.HasPrefix(t.Name(), "experimental") ||
					t.IsCustom())
			},
		)
		if err != nil {
			return nil, err
		}
		// Merge all SVGs regardless of the import group rank as one SVG
		// can exist in multiple import group.
		var customRootNode *pb.StatVarGroupNode
		for _, btData := range btDataList {
			for _, row := range btData {
				svgData, ok := row.Data.(*pb.StatVarGroups)
				if ok && len(svgData.StatVarGroups) > 0 {
					for k, v := range svgData.StatVarGroups {
						if k == hierarchy.CustomSvgRoot && customRootNode == nil {
							customRootNode = v
						}
						if _, ok := result[k]; !ok {
							result[k] = v
						} else {
							hierarchy.MergeSVGNodes(result[k], v)
						}
					}
				}
			}
		}
		if customRootNode != nil {
			customRootExist := false
			// If custom schema is built together with base schema, then it is
			// already in the child stat var group of "dc/g/Root".
			for _, x := range result[hierarchy.SvgRoot].ChildStatVarGroups {
				if x.Id == hierarchy.CustomSvgRoot {
					customRootExist = true
					break
				}
			}
			// Populate dc/g/Custom_Root as children of dc/g/Root
			if !customRootExist {
				result[hierarchy.SvgRoot].ChildStatVarGroups = append(
					result[hierarchy.SvgRoot].ChildStatVarGroups,
					&pb.StatVarGroupNode_ChildSVG{
						Id:                hierarchy.CustomSvgRoot,
						SpecializedEntity: customRootNode.AbsoluteName,
					},
				)
			}
		}
	}
	if store.SQLClient != nil {
		sqlResult, err := fetchSQLSVGs(store.SQLClient.DB)
		if err != nil {
			return nil, err
		}
		for svgId, svgNode := range sqlResult {
			if _, ok := result[svgId]; !ok {
				result[svgId] = svgNode
			} else {
				hierarchy.MergeSVGNodes(result[svgId], svgNode)
			}
		}
	}
	// Recount all descendent stat vars after merging
	hierarchy.AdjustDescendentSVCount(result, hierarchy.SvgRoot)
	return result, nil
}

// Fetches SVGs from SQL.
// First attempts to get it from key value store and falls back to querying sql table.
func fetchSQLSVGs(sqlClient *sql.DB) (map[string]*pb.StatVarGroupNode, error) {
	// Try key value first.
	keyValueSVGs, err := fetchSQLKeyValueSVGs(sqlClient)
	if err != nil {
		return map[string]*pb.StatVarGroupNode{}, err
	}
	if keyValueSVGs != nil {
		// Key value data found => return it.
		return keyValueSVGs.StatVarGroups, nil
	}

	// Query sql table.
	return fetchSQLTableSVGs(sqlClient)
}

func fetchSQLKeyValueSVGs(sqlClient *sql.DB) (*pb.StatVarGroups, error) {
	var svgs pb.StatVarGroups

	found, err := sqlquery.GetKeyValue(sqlClient, sqlquery.StatVarGroupsKey, &svgs)
	if !found || err != nil {
		return nil, err
	}

	return &svgs, nil
}

// TODO: Deprecate this approach in the future
// once the KV approach is universally available.
func fetchSQLTableSVGs(sqlClient *sql.DB) (map[string]*pb.StatVarGroupNode, error) {
	result := map[string]*pb.StatVarGroupNode{}
	// Query for all the stat var group node
	query := `
SELECT t1.subject_id, t2.object_value, t3.object_id
FROM triples t1 JOIN triples t2 ON t1.subject_id = t2.subject_id
JOIN triples t3 ON t1.subject_id = t3.subject_id
WHERE t1.predicate="typeOf"
AND t1.object_id="StatVarGroup"
AND t2.predicate="name"
AND t3.predicate="specializationOf";
`
	svgRows, err := sqlClient.Query(query)
	if err != nil {
		return nil, err
	}
	defer svgRows.Close()
	for svgRows.Next() {
		var self, name, parent string
		err = svgRows.Scan(&self, &name, &parent)
		if err != nil {
			return nil, err
		}
		result[self] = &pb.StatVarGroupNode{
			AbsoluteName: name,
		}
		if _, ok := result[parent]; !ok {
			result[parent] = &pb.StatVarGroupNode{}
		}
		result[parent].ChildStatVarGroups = append(
			result[parent].ChildStatVarGroups,
			&pb.StatVarGroupNode_ChildSVG{
				Id:                self,
				SpecializedEntity: name,
			},
		)
	}
	// Query for all the stat var nodes
	query = `
SELECT t1.subject_id, t2.object_value, t3.object_id, COALESCE(t4.object_value, '')
FROM triples t1
JOIN triples t2 ON t1.subject_id = t2.subject_id
JOIN triples t3 ON t1.subject_id = t3.subject_id
LEFT JOIN triples t4 ON t1.subject_id = t4.subject_id AND t4.predicate = "description"
WHERE t1.predicate="typeOf"
AND t1.object_id="StatisticalVariable"
AND t2.predicate="name"
AND t3.predicate="memberOf";
`
	svRows, err := sqlClient.Query(query)
	if err != nil {
		return nil, err
	}
	defer svRows.Close()
	for svRows.Next() {
		var sv, name, svg, description string
		err = svRows.Scan(&sv, &name, &svg, &description)
		if err != nil {
			return nil, err
		}
		if _, ok := result[svg]; !ok {
			result[svg] = &pb.StatVarGroupNode{}
		}
		searchNames := []string{}
		if len(name) > 0 {
			searchNames = append(searchNames, name)
		}
		if len(description) > 0 {
			searchNames = append(searchNames, description)
		}
		result[svg].ChildStatVars = append(
			result[svg].ChildStatVars,
			&pb.StatVarGroupNode_ChildSV{
				Id:          sv,
				DisplayName: name,
				SearchNames: searchNames,
			},
		)
		result[svg].DescendentStatVarCount += 1
	}
	return result, nil
}
