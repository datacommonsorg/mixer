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
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/statvar/hierarchy"
	"github.com/datacommonsorg/mixer/internal/sqldb"
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
	if sqldb.IsConnected(&store.SQLClient) {
		sqlResult, err := fetchSQLSVGs(ctx, &store.SQLClient)
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
func fetchSQLSVGs(ctx context.Context, sqlClient *sqldb.SQLClient) (map[string]*pb.StatVarGroupNode, error) {
	// Try key value first.
	keyValueSVGs, err := fetchSQLKeyValueSVGs(ctx, sqlClient)
	if err != nil {
		return map[string]*pb.StatVarGroupNode{}, err
	}
	if keyValueSVGs != nil {
		// Key value data found => return it.
		return keyValueSVGs.StatVarGroups, nil
	}

	// Query sql table.
	return fetchSQLTableSVGs(ctx, sqlClient)
}

func fetchSQLKeyValueSVGs(ctx context.Context, sqlClient *sqldb.SQLClient) (*pb.StatVarGroups, error) {
	var svgs pb.StatVarGroups

	found, err := sqlClient.GetKeyValue(ctx, sqldb.StatVarGroupsKey, &svgs)
	if !found || err != nil {
		return nil, err
	}

	return &svgs, nil
}

// TODO: Deprecate this approach in the future
// once the KV approach is universally available.
func fetchSQLTableSVGs(ctx context.Context, sqlClient *sqldb.SQLClient) (map[string]*pb.StatVarGroupNode, error) {
	result := map[string]*pb.StatVarGroupNode{}

	svgRows, err := sqlClient.GetAllStatVarGroups(ctx)
	if err != nil {
		return nil, err
	}
	for _, svgRow := range svgRows {
		result[svgRow.ID] = &pb.StatVarGroupNode{
			AbsoluteName: svgRow.Name,
		}
		if _, ok := result[svgRow.ParentID]; !ok {
			result[svgRow.ParentID] = &pb.StatVarGroupNode{}
		}
		result[svgRow.ParentID].ChildStatVarGroups = append(
			result[svgRow.ParentID].ChildStatVarGroups,
			&pb.StatVarGroupNode_ChildSVG{
				Id:                svgRow.ID,
				SpecializedEntity: svgRow.Name,
			},
		)
	}

	svRows, err := sqlClient.GetAllStatisticalVariables(ctx)
	if err != nil {
		return nil, err
	}
	for _, svRow := range svRows {
		if _, ok := result[svRow.SVGID]; !ok {
			result[svRow.SVGID] = &pb.StatVarGroupNode{}
		}
		searchNames := []string{}
		if len(svRow.Name) > 0 {
			searchNames = append(searchNames, svRow.Name)
		}
		if len(svRow.Description) > 0 {
			searchNames = append(searchNames, svRow.Description)
		}
		result[svRow.SVGID].ChildStatVars = append(
			result[svRow.SVGID].ChildStatVars,
			&pb.StatVarGroupNode_ChildSV{
				Id:          svRow.ID,
				DisplayName: svRow.Name,
				SearchNames: searchNames,
			},
		)
		result[svRow.SVGID].DescendentStatVarCount += 1
	}
	return result, nil
}
