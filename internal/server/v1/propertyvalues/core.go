// Copyright 2022 Google LLC
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

package propertyvalues

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fetch is the generic handler to fetch property values for multiple
// properties and nodes.
//
// Returned map is keyed by: node dcid, property, and target node type.
func Fetch(
	ctx context.Context,
	store *store.Store,
	nodes []string,
	properties []string,
	limit int,
	token string,
	direction string,
) (
	map[string]map[string]map[string][]*pb.EntityInfo,
	*pbv1.PaginationInfo,
	error,
) {
	var resp map[string]map[string]map[string][]*pb.EntityInfo
	var pg *pbv1.PaginationInfo
	var err error
	if store.BtGroup != nil {
		resp, pg, err = fetchBT(ctx, store.BtGroup, nodes, properties, limit, token, direction)
		if err != nil {
			return nil, nil, err
		}
	}
	// No pagination for sqlite query, so if there is a pagination token, meaning
	// the data has already been queried and returned in previous query.
	if store.SQLiteClient != nil && token == "" {
		sqlResp, err := fetchSQLite(ctx, store.SQLiteClient, nodes, properties, direction)
		if err != nil {
			return nil, nil, err
		}
		for node := range sqlResp {
			if _, ok := resp[node]; !ok {
				resp[node] = sqlResp[node]
				continue
			}
			for prop := range sqlResp[node] {
				if _, ok := resp[node][prop]; !ok {
					resp[node][prop] = sqlResp[node][prop]
					continue
				}
				for typ := range sqlResp[node][prop] {
					if _, ok := resp[node][prop][typ]; !ok {
						resp[node][prop][typ] = sqlResp[node][prop][typ]
						continue
					}
					resp[node][prop][typ] = append(resp[node][prop][typ], sqlResp[node][prop][typ]...)
				}
			}
		}
	}
	return resp, pg, err
}

func fetchSQLite(
	ctx context.Context,
	sqliteClient *sql.DB,
	nodes []string,
	properties []string,
	direction string,
) (
	map[string]map[string]map[string][]*pb.EntityInfo,
	error,
) {
	if sqliteClient == nil {
		return nil, nil
	}
	var matchColumn string
	if direction == util.DirectionOut {
		matchColumn = "subject_id"
	} else {
		matchColumn = "object_id"
	}
	query := fmt.Sprintf(
		`
			WITH node_list(node) AS (
					VALUES %s
			),
			prop_list(prop) AS (
					VALUES %s
			),
			all_pairs AS (
					SELECT n.node, p.prop
					FROM node_list n
					CROSS JOIN prop_list p
			)
			SELECT subject_id, predicate, object_id, object_value
			FROM all_pairs a
			INNER JOIN triples t ON a.node = t.%s AND a.prop = t.predicate
			GROUP BY a.node, a.prop;
		`,
		util.SQLValuesParam(len(nodes)),
		util.SQLValuesParam(len(properties)),
		matchColumn,
	)
	args := []string{}
	args = append(args, nodes...)
	args = append(args, properties...)
	args = append(args, matchColumn)
	// Execute query
	rows, err := sqliteClient.Query(query, util.ConvertArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resp := map[string]map[string]map[string][]*pb.EntityInfo{}
	for _, node := range nodes {
		resp[node] = map[string]map[string][]*pb.EntityInfo{}
	}
	for rows.Next() {
		var subject_id, predicate, object_id, object_value string
		err = rows.Scan(&subject_id, &predicate, &object_id, &object_value)
		if err != nil {
			return nil, err
		}
		var n string
		if matchColumn == "subject_id" {
			n = subject_id
		} else {
			n = object_id
		}
		if _, ok := resp[n][predicate]; !ok {
			resp[n][predicate] = map[string][]*pb.EntityInfo{}
		}
		if matchColumn == "subject_id" {
			// Always use "Thing" as type for SQLite node. Will need to improve this
			// if necessary.
			if _, ok := resp[n][predicate]["Thing"]; !ok {
				resp[n][predicate]["Thing"] = []*pb.EntityInfo{}
			}
			resp[n][predicate]["Thing"] = append(
				resp[n][predicate]["Thing"],
				&pb.EntityInfo{
					Dcid:  object_id,
					Value: object_value,
					Types: []string{"Thing"},
				},
			)
		} else {
			// object value uses "" as type
			if _, ok := resp[n][predicate][""]; !ok {
				resp[n][predicate][""] = []*pb.EntityInfo{}
			}
			resp[n][predicate][""] = append(
				resp[n][predicate][""],
				&pb.EntityInfo{
					Dcid: subject_id,
				},
			)
		}
	}
	return resp, nil
}

// fetchBT fetch property values from Bigtable Cache
//
// Returned map is keyed (in order) by: node dcid, property, target node type.
func fetchBT(
	ctx context.Context,
	btGroup *bigtable.Group,
	nodes []string,
	properties []string,
	limit int,
	token string,
	direction string,
) (
	map[string]map[string]map[string][]*pb.EntityInfo,
	*pbv1.PaginationInfo,
	error,
) {
	if btGroup == nil {
		return nil, nil, nil
	}
	var err error
	propType, err := getNodePropType(ctx, btGroup, nodes, properties, direction)
	if err != nil {
		return nil, nil, err
	}
	// Empty cursor groups when no token is given.
	var cursorGroups []*pbv1.CursorGroup
	if token == "" {
		if btGroup != nil {
			cursorGroups = buildDefaultCursorGroups(
				nodes, properties, propType, len(btGroup.Tables(nil)),
			)
		}
	} else {
		pi, err := pagination.Decode(token)
		if err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %s", token)
		}
		cursorGroups = pi.CursorGroups
	}
	if limit <= 0 || limit > defaultLimit {
		limit = defaultLimit
	}
	cursorGroup := map[string]map[string]map[string][]*pbv1.Cursor{}
	for _, g := range cursorGroups {
		keys := g.GetKeys()
		// Key is  [node, property, type]
		if len(keys) != 3 {
			return nil, nil, status.Errorf(
				codes.Internal, "cursor should have three keys, cursor: %s", g)
		}
		n, p, t := keys[0], keys[1], keys[2]
		if _, ok := cursorGroup[n]; !ok {
			cursorGroup[n] = map[string]map[string][]*pbv1.Cursor{}
		}
		if _, ok := cursorGroup[n][p]; !ok {
			cursorGroup[n][p] = map[string][]*pbv1.Cursor{}
		}
		cursorGroup[n][p][t] = g.GetCursors()
	}
	if direction == util.DirectionOut {
		s := &outState{}
		if err = s.init(ctx, btGroup, nodes, properties, limit, cursorGroup); err != nil {
			return nil, nil, err
		}
		for {
			hasNext, err := nextOut(ctx, s, btGroup)
			if err != nil {
				return nil, nil, err
			}
			if !hasNext {
				break
			}
		}
		// Out property values only use one (the preferred) import group. So here
		// should only check if that import group has more data to compute the token.
		for n := range s.rawNodes {
			for p := range s.rawNodes[n] {
				for t := range s.rawNodes[n][p] {
					if s.rawNodes[n][p][t][s.usedImportGroup[n][p][t]] != nil {
						return s.mergedNodes, s.getPagination(), nil
					}
				}
			}
		}
		return s.mergedNodes, nil, nil
	} else {
		s := &inState{}
		if err = s.init(ctx, btGroup, nodes, properties, limit, cursorGroup); err != nil {
			return nil, nil, err
		}
		for {
			hasNext, err := nextIn(ctx, s, btGroup)
			if err != nil {
				return nil, nil, err
			}
			if !hasNext {
				break
			}
		}
		// If rawNodes is not empty, there is leftover data to be fetched via
		// pagination. Need to return the pagination info. Otherwise, this reached
		// the end of all data, no need to return pagination info.
		for n := range s.rawNodes {
			for p := range s.rawNodes[n] {
				for t := range s.rawNodes[n][p] {
					for _, d := range s.rawNodes[n][p][t] {
						if d != nil {
							return s.mergedNodes, s.getPagination(), nil
						}
					}
				}
			}
		}
		return s.mergedNodes, nil, nil
	}
}

// Process the next node for out property value
//
// Out property values are not merged, only the preferred import group result
// is used.
func nextOut(ctx context.Context, s *outState, btGroup *bigtable.Group) (bool, error) {
	accs := []*bigtable.Accessor{}
	for _, n := range s.nodes {
		for _, p := range s.properties {
			types := []string{}
			for t := range s.cursorGroup[n][p] {
				types = append(types, t)
			}
			sort.Strings(types)
			for _, t := range types {
				// No raw data for this "property", "node", "type"
				if _, ok := s.next[n][p][t]; !ok {
					continue
				}
				if len(s.mergedNodes[n][p][t]) == s.limit {
					delete(s.next[n][p], t)
					continue
				}
				ig := s.usedImportGroup[n][p][t]
				// Update the cursor.
				cursor := s.cursorGroup[n][p][t][ig]
				node := s.rawNodes[n][p][t][ig][cursor.Item]
				// If this node has multiple types, check if it has been processed
				// for the other types already.
				processed := false
				if len(node.Types) > 0 {
					for _, t := range node.Types {
						if s.cursorGroup[n][p][t] == nil {
							// The cursor group for <e,p,t> is nil, which has been fully
							// processed in previous page.
							processed = true
							break
						}
						l := s.mergedNodes[n][p][t]
						if len(l) > 0 && node.Dcid <= l[len(l)-1].Dcid {
							// This node has been processed for type "t".
							processed = true
							break
						}
					}
				}
				if !processed {
					if _, ok := s.mergedNodes[n][p][t]; !ok {
						s.mergedNodes[n][p][t] = []*pb.EntityInfo{}
					}
					s.mergedNodes[n][p][t] = append(
						s.mergedNodes[n][p][t],
						node,
					)
				}
				// Proceed cursor
				cursor.Item++
				// Still need more data, mark in s.hasNext
				s.next[n][p][t] = cursor
				// Reach the end of the current page, should advance to next page.
				if int(cursor.Item) == len(s.rawNodes[n][p][t][ig]) {
					cursor.Page++
					cursor.Item = 0
					// No more pages for the import group. As out prop values only use
					// a single import group, so no more data for this <e,p,t> combination.
					if cursor.Page == int32(s.totalPage[n][p][t][ig]) {
						s.rawNodes[n][p][t][ig] = nil
						s.cursorGroup[n][p][t] = nil
						delete(s.next[n][p], t)
					} else {
						s.next[n][p][t] = cursor
						accs = append(accs, &bigtable.Accessor{
							ImportGroup: ig,
							Body: [][]string{
								{n},
								{p},
								{t},
								{strconv.Itoa(int(cursor.Page))},
							},
						})
					}
				}
			}
		}
	}
	if len(accs) > 0 {
		err := s.readBt(ctx, btGroup, true, accs)
		if err != nil {
			return false, err
		}
	}
	hasNext := false
	for n := range s.next {
		for p := range s.next[n] {
			if len(s.next[n][p]) > 0 {
				hasNext = true
				break
			}
		}
		if hasNext {
			break
		}
	}
	return hasNext, nil
}

// Process the next node for in property value
//
// As the data in each import group is sorted already. The duplication is
// handled by advancing the pointer in each import group simultaneously like
// in a merge sort.
func nextIn(ctx context.Context, s *inState, btGroup *bigtable.Group) (bool, error) {
	accs := []*bigtable.Accessor{}
	for _, n := range s.nodes {
		for _, p := range s.properties {
			types := []string{}
			for t := range s.next[n][p] {
				types = append(types, t)
			}
			sort.Strings(types)
			for _, t := range types {
				if s.heap[n][p][t].Len() == 0 {
					// All nodes in all import groups for [n, p, t] have been exhausted.
					// Delete this entry in "s.next" so the outer for loop can skip it.
					delete(s.next[n][p], t)
					s.cursorGroup[n][p][t] = nil
					continue
				}
				elem := heap.Pop(s.heap[n][p][t]).(*heapElem)
				node, ig := elem.data, elem.ig
				// If this node has multiple types, check if it has been processed
				// for the other types already.
				processed := false
				if len(node.Types) > 0 {
					for _, t := range node.Types {
						if s.cursorGroup[n][p][t] == nil {
							// The cursor group for <e,p,t> is nil, which has been fully
							// processed in previous page.
							processed = true
							break
						}
						l := s.mergedNodes[n][p][t]
						if len(l) > 0 && node.Dcid <= l[len(l)-1].Dcid {
							// This node has been processed for type "t".
							processed = true
							break
						}
					}
				}
				if !processed {
					// Add the node to "mergedNodes".
					if len(s.mergedNodes[n][p][t]) == 0 {
						s.mergedNodes[n][p][t] = []*pb.EntityInfo{node}
					} else {
						prev := s.mergedNodes[n][p][t][len(s.mergedNodes[n][p][t])-1]
						if node.Dcid != prev.Dcid || node.Value != prev.Value {
							// Find a new node, add to the result.
							s.mergedNodes[n][p][t] = append(s.mergedNodes[n][p][t], node)
						}
					}
				}
				// Got enough nodes, should stop.
				// Cursor is now at the next read item, ready to be returned.
				//
				// Here go past one node over limit to ensure all duplicated entries have
				// been processed. Otherwise, next API request could get duplicate entries.

				// For example, given the two import groups data below and limit of 1, this
				// ensures the duplicated entry "a" are all processed in this request, and
				// next request would start processing from "b".

				// import group 1: ["a", "a", "b"]
				// import group 2: ["a", "c"]
				if len(s.mergedNodes[n][p][t]) == s.limit+1 {
					s.mergedNodes[n][p][t] = s.mergedNodes[n][p][t][:s.limit]
					delete(s.next[n][p], t)
				}
				if _, ok := s.next[n][p][t]; ok {
					// Update the cursor.
					cursor := s.cursorGroup[n][p][t][ig]
					cursor.Item++
					// Reach the end of the current page, should advance to next page.
					if int(cursor.Item) == len(s.rawNodes[n][p][t][ig]) {
						cursor.Page++
						cursor.Item = 0
						// No more pages
						if cursor.Page == int32(s.totalPage[n][p][t][ig]) {
							s.rawNodes[n][p][t][ig] = nil
						} else {
							accs = append(accs, &bigtable.Accessor{
								ImportGroup: ig,
								Body: [][]string{
									{n},
									{p},
									{t},
									{strconv.Itoa(int(cursor.Page))},
								},
							})
						}
					}
					s.next[n][p][t] = cursor
				}
			}
		}
	}
	if len(accs) > 0 {
		err := s.readBt(ctx, btGroup, false, accs)
		if err != nil {
			return false, err
		}
	}
	for _, n := range s.nodes {
		for _, p := range s.properties {
			types := []string{}
			for t := range s.cursorGroup[n][p] {
				types = append(types, t)
			}
			sort.Strings(types)
			for _, t := range types {
				if cursor, ok := s.next[n][p][t]; ok {
					// If there is data available in the current import group, push to the heap.
					if s.rawNodes[n][p][t][cursor.GetImportGroup()] != nil {
						elem := &heapElem{
							ig:   int(cursor.GetImportGroup()),
							pos:  cursor.GetItem(),
							data: s.rawNodes[n][p][t][cursor.GetImportGroup()][cursor.GetItem()],
						}
						heap.Push(s.heap[n][p][t], elem)
					}
				}
			}
		}
	}

	hasNext := false
	for n := range s.next {
		for p := range s.next[n] {
			if len(s.next[n][p]) > 0 {
				hasNext = true
				break
			}
		}
		if hasNext {
			break
		}
	}
	return hasNext, nil
}
