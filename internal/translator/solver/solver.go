// Copyright 2019 Google LLC
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

package solver

import (
	"encoding/json"
	"os"

	"github.com/datacommonsorg/mixer/internal/parser/tmcf"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var skippedPred = map[string]struct{}{
	"typeOf":     {},
	"subType":    {},
	"dcid":       {},
	"isPublic":   {},
	"provenance": {},
}

type tableTypes struct {
	TableTypes []*data `json:"table_types"`
}
type data struct {
	Parent   string   `json:"parent"`
	Table    string   `json:"table"`
	Children []string `json:"children"`
}

// GetSubTypeMap gets subtype map.
func GetSubTypeMap(tableTypesJSONFilePath string) (map[string]string, error) {
	tableTypesJSON, err := os.ReadFile(tableTypesJSONFilePath)
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	tableTypes := tableTypes{}
	err = json.Unmarshal(tableTypesJSON, &tableTypes)
	if err != nil {
		return nil, err
	}
	for _, d := range tableTypes.TableTypes {
		for _, c := range d.Children {
			result[c] = d.Parent
		}
	}
	return result, nil
}

// GetNodeType obtains a map from node alias to the types.
func GetNodeType(queries []*types.Query) (map[string]string, error) {
	result := make(map[string]string)
	for _, q := range queries {
		if q.Pred == tmcf.TypeOf {
			if _, ok := result[q.Sub.Alias]; ok {
				return nil, status.Error(codes.InvalidArgument, "Duplicate select node type")
			}
			if _, ok := q.Obj.(string); !ok {
				return nil, status.Errorf(
					codes.InvalidArgument, "Node should be string, got %s of type %T", q.Obj, q.Obj)
			}
			result[q.Sub.Alias] = q.Obj.(string)
		}
	}
	return result, nil
}

// GetEntityType obtains a map from entity key to the types.
func GetEntityType(mappings []*types.Mapping) map[string][]string {
	result := make(map[string][]string)
	for _, m := range mappings {
		if m.Pred == tmcf.TypeOf {
			result[m.Sub.Key()] = append(result[m.Sub.Key()], m.Obj.(string))
		}
	}
	return result
}

// GetExplicitTypeProp obtains a map from type to list of predicate
func GetExplicitTypeProp(mappings []*types.Mapping) map[string][]string {
	entityType := GetEntityType(mappings)
	result := make(map[string][]string)
	for _, m := range mappings {
		if pred, ok := m.Pred.(string); ok {
			for _, t := range entityType[m.Sub.Key()] {
				result[t] = append(result[t], pred)
			}
		}
	}
	return result
}

// GetQueryID obtains the id for query statement.
// If two query statements match to Triples table, they would have same query id if they
// have the same predciate and subject.
// If two query statements match to non-Triples table, they would have the same query id
// if they have the same subject.
// The same query id means they point to the same spanner table instance in SQL query.
func GetQueryID(
	queries []*types.Query,
	matchTriple map[*types.Query]bool,
) map[*types.Query]int {
	result := map[*types.Query]int{}
	triplepredSub := map[[2]string]int{}
	countTriple := 0
	nonTripleSub := map[string]int{}
	countNonTriple := 0

	for _, q := range queries {
		match, ok := matchTriple[q]
		if !ok {
			match = true
		}
		if match {
			if _, ok := triplepredSub[[2]string{q.Pred, q.Sub.Alias}]; !ok {
				triplepredSub[[2]string{q.Pred, q.Sub.Alias}] = countTriple
				countTriple++
			}
		} else {
			if _, ok := nonTripleSub[q.Sub.Alias]; !ok {
				nonTripleSub[q.Sub.Alias] = countNonTriple
				countNonTriple++
			}
		}
	}
	for _, q := range queries {
		match, ok := matchTriple[q]
		if !ok {
			match = true
		}
		if match {
			result[q] = triplepredSub[[2]string{q.Pred, q.Sub.Alias}]
		} else {
			result[q] = nonTripleSub[q.Sub.Alias]
		}
	}
	return result
}

// MatchTriple takes list of queries and mappings and determines
// whether a query matches Triples table.
func MatchTriple(mappings []*types.Mapping, queries []*types.Query) (map[*types.Query]bool, error) {
	result := map[*types.Query]bool{}
	nodeType, err := GetNodeType(queries)
	if err != nil {
		return nil, err
	}
	explicitTypeProp := GetExplicitTypeProp(mappings)

	for _, q := range queries {
		// Determine if a query should match to Triples schema mapping
		// A query will NOT match to Triples table, only when all the following
		// conditions meet:
		// 	- the subject type is in speical table
		// 	- the predicate exist in the special table's mappings
		matchTriple := true
		if qSubType, ok := nodeType[q.Sub.Alias]; ok {
			if props, ok := explicitTypeProp[qSubType]; ok {
				for _, prop := range props {
					if prop == q.Pred {
						matchTriple = false
						break
					}
				}
			}
		}
		result[q] = matchTriple
	}
	return result, nil
}

// GetFuncDeps obtains the functional deps from schema mapping.
func GetFuncDeps(mappings []*types.Mapping) (map[types.Entity]map[string]interface{}, error) {
	result := map[types.Entity]map[string]interface{}{}
	for _, m := range mappings {
		if _, ok := m.Pred.(types.FuncDeps); ok {
			result[m.Sub] = map[string]interface{}{}
			if obj, ok := m.Obj.([]string); ok {
				for _, o := range obj {
					result[m.Sub][o] = nil
				}
			} else {
				return nil, status.Errorf(codes.InvalidArgument, "Invalid schema mapping: %+v", m)
			}
		}
	}
	for _, m := range mappings {
		if p2c, ok := result[m.Sub]; ok {
			if p, ok := m.Pred.(string); ok {
				if _, ok := p2c[p]; ok {
					p2c[p] = m.Obj
				}
			}
		}
	}
	for e, p2c := range result {
		for p, c := range p2c {
			if c == nil {
				return nil, status.Errorf(codes.InvalidArgument, "No functional deps for %v: %v", e, p)
			}
		}
	}
	return result, nil
}

// GetProvColumn obtains the provenance column for each table.
func GetProvColumn(mappings []*types.Mapping) (map[string]types.Column, error) {
	funcDeps, err := GetFuncDeps(mappings)
	if err != nil {
		return nil, err
	}
	result := map[string]types.Column{}
	for _, m := range mappings {
		if m.Pred == "provenance" {
			fd, ok := funcDeps[m.Obj.(types.Entity)]["dcid"]
			if !ok {
				return nil, err
			}
			col, ok := fd.(types.Column)
			if !ok {
				return nil, err
			}
			result[col.Table.Name] = col
		}
	}
	return result, nil
}

// GetNodeRef obtains a list of node reference from query statements.
func GetNodeRef(queries []*types.Query) map[types.Node]struct{} {
	res := map[types.Node]struct{}{}
	for _, q := range queries {
		res[q.Sub] = struct{}{}
	}
	return res
}

// RewriteQuery rewrites typeOf query for entity that is a subType.
func RewriteQuery(queries []*types.Query, subTypeMap map[string]string) []*types.Query {
	type info struct {
		pos int
		t   string
	}

	// Do not modify the input "queries"
	res := []*types.Query{}
	for _, q := range queries {
		tmp := *q
		res = append(res, &tmp)
	}

	typeOfNodeInfo := map[types.Node]info{}
	subTypeNodes := map[types.Node]struct{}{}
	for i, q := range res {
		if q.Pred == tmcf.TypeOf {
			if v, ok := q.Obj.(string); ok {
				if _, ok := subTypeMap[v]; ok {
					typeOfNodeInfo[q.Sub] = info{i, v}
				}
			}
		} else if q.Pred == "subType" {
			subTypeNodes[q.Sub] = struct{}{}
		}
	}

	for n := range typeOfNodeInfo {
		if _, ok := subTypeNodes[n]; ok {
			continue
		}
		in := typeOfNodeInfo[n]
		res[in.pos] = types.NewQuery(tmcf.TypeOf, n.Alias, subTypeMap[in.t])
		res = append(res, types.NewQuery("subType", n.Alias, in.t))
	}
	return res
}

// PruneMapping prunes foreign key entity mappings.
func PruneMapping(mappings []*types.Mapping) []*types.Mapping {
	tableInfo := map[types.Entity][]string{}
	redundant := map[types.Entity]struct{}{}
	result := []*types.Mapping{}
	for _, m := range mappings {
		if pred, ok := m.Pred.(string); ok {
			tableInfo[m.Sub] = append(tableInfo[m.Sub], pred)
		}
	}
	for sub, predList := range tableInfo {
		remove := true
		for _, pred := range predList {
			if pred != tmcf.TypeOf && pred != "dcid" {
				remove = false
				break
			}
		}
		if remove {
			redundant[sub] = struct{}{}
		}
	}
	for _, m := range mappings {
		if _, ok := redundant[m.Sub]; !ok {
			result = append(result, m)
		}
	}
	return result
}

// GetOutArcInfo gets the table and columns corresponding to the node properties.
func GetOutArcInfo(
	mappings []*types.Mapping,
	nodeType string,
) (map[string][]*types.OutArcInfo, error) {
	entities := map[types.Entity]struct{}{}
	for _, m := range mappings {
		if m.Pred == tmcf.TypeOf {
			if mObj, ok := m.Obj.(string); ok {
				if nodeType == mObj {
					entities[m.Sub] = struct{}{}
				}
			}
		}
	}
	funcDeps, err := GetFuncDeps(mappings)
	if err != nil {
		return nil, err
	}

	result := map[string][]*types.OutArcInfo{}
	for _, m := range mappings {
		if _, ok := entities[m.Sub]; !ok {
			continue
		}
		mPred, ok := m.Pred.(string)
		if !ok {
			continue
		}
		if _, ok := skippedPred[mPred]; ok {
			continue
		}
		if mObj, ok := m.Obj.(types.Column); ok {
			result[m.Sub.Table.Name] = append(
				result[m.Sub.Table.Name],
				&types.OutArcInfo{
					Pred:   mPred,
					Column: mObj.Name,
					IsNode: false,
				})
		} else if mObj, ok := m.Obj.(types.Entity); ok {
			if deps, ok := funcDeps[mObj]; ok {
				for p, col := range deps {
					if p == "dcid" {
						if c, _ := col.(types.Column); ok {
							result[m.Sub.Table.Name] = append(
								result[m.Sub.Table.Name],
								&types.OutArcInfo{
									Pred:   mPred,
									Column: c.Name,
									IsNode: true,
								})
						}
					}
				}
			}
		}
	}
	return result, nil
}

// GetInArcInfo gets the table and columns corresponding to the node properties.
func GetInArcInfo(
	mappings []*types.Mapping, nodeType string) ([]*types.InArcInfo, error) {

	// type InArcInfo struct {
	// 	Table string
	// 	Pred string
	// 	SubCol string
	// 	ObjCol string
	// }
	funcDeps, err := GetFuncDeps(mappings)
	if err != nil {
		return nil, err
	}

	entities := make(map[types.Entity]struct{})
	for _, m := range mappings {
		if m.Pred == tmcf.TypeOf {
			if m.Obj.(string) == nodeType {
				entities[m.Sub] = struct{}{}
			}
		}
	}

	result := []*types.InArcInfo{}
	for _, m := range mappings {
		// Obj is entity.
		mObj, ok := m.Obj.(types.Entity)
		if !ok {
			continue
		}
		// Obj is of the node type.
		if _, ok := entities[mObj]; !ok {
			continue
		}
		// Pred is string (this is to rule out Triples table).
		mPred, ok := m.Pred.(string)
		if !ok {
			continue
		}

		// Right now, only handles in node with dcid.
		// TODO(boxu): deal with the case like Weather.
		var objCol string
		if deps, ok := funcDeps[mObj]; ok {
			if len(deps) > 1 {
				continue
			}
			for _, col := range deps {
				objCol = col.(types.Column).Name
			}
		}

		var subCol string
		if deps, ok := funcDeps[m.Sub]; ok {
			if len(deps) > 1 {
				continue
			}
			for _, col := range deps {
				subCol = col.(types.Column).Name
			}
		}
		inArcInfo := &types.InArcInfo{
			Table:  m.Sub.Table.Name,
			Pred:   mPred,
			SubCol: subCol,
			ObjCol: objCol,
		}
		result = append(result, inArcInfo)
		delete(entities, mObj)
	}
	return result, nil
}
