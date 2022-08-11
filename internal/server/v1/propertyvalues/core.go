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
	"sort"
	"strconv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fetch is the generic handler to fetch property values for multiple
// properties and entities.
func Fetch(
	ctx context.Context,
	store *store.Store,
	entities []string,
	properties []string,
	limit int,
	token string,
	direction string,
) (
	map[string]map[string]map[string][]*pb.EntityInfo,
	*pb.PaginationInfo,
	error,
) {
	var err error
	propType, err := getEntityPropType(ctx, store.BtGroup, entities, properties, direction)
	if err != nil {
		return nil, nil, err
	}
	// Empty cursor groups when no token is given.
	var cursorGroups []*pb.CursorGroup
	if token == "" {
		cursorGroups = buildDefaultCursorGroups(entities, properties, propType, len(store.BtGroup.Tables()))
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
	cursorGroup := map[string]map[string]map[string][]*pb.Cursor{}
	for _, g := range cursorGroups {
		keys := g.GetKeys()
		// Key is  [entity, property, type]
		if len(keys) != 3 {
			return nil, nil, status.Errorf(
				codes.Internal, "cursor should have three keys, cursor: %s", g)
		}
		e, p, t := keys[0], keys[1], keys[2]
		if _, ok := cursorGroup[e]; !ok {
			cursorGroup[e] = map[string]map[string][]*pb.Cursor{}
		}
		if _, ok := cursorGroup[e][p]; !ok {
			cursorGroup[e][p] = map[string][]*pb.Cursor{}
		}
		cursorGroup[e][p][t] = g.GetCursors()
	}
	if direction == util.DirectionOut {
		s := &outState{}
		if err = s.init(ctx, store.BtGroup, entities, properties, limit, cursorGroup); err != nil {
			return nil, nil, err
		}
		for {
			hasNext, err := nextOut(ctx, s, store.BtGroup)
			if err != nil {
				return nil, nil, err
			}
			if !hasNext {
				break
			}
		}
		// Out property values only use one (the preferred) import group. So here
		// should only check if that import group has more data to compute the token.
		for e := range s.rawEntities {
			for p := range s.rawEntities[e] {
				for t := range s.rawEntities[e][p] {
					if s.rawEntities[e][p][t][s.usedImportGroup[e][p][t]] != nil {
						return s.mergedEntities, s.getPagination(), nil
					}
				}
			}
		}
		return s.mergedEntities, nil, nil
	} else {
		s := &inState{}
		if err = s.init(ctx, store.BtGroup, entities, properties, limit, cursorGroup); err != nil {
			return nil, nil, err
		}
		for {
			hasNext, err := nextIn(ctx, s, store.BtGroup)
			if err != nil {
				return nil, nil, err
			}
			if !hasNext {
				break
			}
		}
		// If rawEntities is not empty, there is leftover data to be fetched via
		// pagination. Need to return the pagination info. Otherwise, this reached
		// the end of all data, no need to return pagination info.
		for e := range s.rawEntities {
			for p := range s.rawEntities[e] {
				for t := range s.rawEntities[e][p] {
					for _, d := range s.rawEntities[e][p][t] {
						if d != nil {
							return s.mergedEntities, s.getPagination(), nil
						}
					}
				}
			}
		}
		return s.mergedEntities, nil, nil
	}
}

// Process the next entity for out property value
//
// Out property values are not merged, only the preferred import group result
// is used.
func nextOut(ctx context.Context, s *outState, btGroup *bigtable.Group) (bool, error) {
	accs := []*bigtable.Accessor{}
	for _, e := range s.entities {
		for _, p := range s.properties {
			types := []string{}
			for t := range s.cursorGroup[e][p] {
				types = append(types, t)
			}
			sort.Strings(types)
			for _, t := range types {
				// No raw data for this "property", "entity", "type"
				if _, ok := s.next[e][p][t]; !ok {
					continue
				}
				if len(s.mergedEntities[e][p][t]) == s.limit {
					delete(s.next[e][p], t)
					continue
				}
				ig := s.usedImportGroup[e][p][t]
				// Update the cursor.
				cursor := s.cursorGroup[e][p][t][ig]
				entity := s.rawEntities[e][p][t][ig][cursor.Item]
				// If this entity has multiple types, check if it has been processed
				// for the other types already.
				processed := false
				if len(entity.Types) > 0 {
					for _, t := range entity.Types {
						if s.cursorGroup[e][p][t] == nil {
							// The cursor group for <e,p,t> is nil, which has been fully
							// processed in previous page.
							processed = true
							break
						}
						l := s.mergedEntities[e][p][t]
						if len(l) > 0 && entity.Dcid <= l[len(l)-1].Dcid {
							// This entity has been processed for type "t".
							processed = true
							break
						}
					}
				}
				if !processed {
					if _, ok := s.mergedEntities[e][p][t]; !ok {
						s.mergedEntities[e][p][t] = []*pb.EntityInfo{}
					}
					s.mergedEntities[e][p][t] = append(
						s.mergedEntities[e][p][t],
						entity,
					)
				}
				// Proceed cursor
				cursor.Item++
				// Still need more data, mark in s.hasNext
				s.next[e][p][t] = cursor
				// Reach the end of the current page, should advance to next page.
				if int(cursor.Item) == len(s.rawEntities[e][p][t][ig]) {
					cursor.Page++
					cursor.Item = 0
					// No more pages for the import group. As out prop values only use
					// a single import group, so no more data for this <e,p,t> combination.
					if cursor.Page == int32(s.totalPage[e][p][t][ig]) {
						s.rawEntities[e][p][t][ig] = nil
						s.cursorGroup[e][p][t] = nil
						delete(s.next[e][p], t)
					} else {
						s.next[e][p][t] = cursor
						accs = append(accs, &bigtable.Accessor{
							ImportGroup: ig,
							Body: [][]string{
								{e},
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
	for e := range s.next {
		for p := range s.next[e] {
			if len(s.next[e][p]) > 0 {
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

// Process the next entity for in property value
//
// As the data in each import group is sorted already. The duplication is
// handled by advancing the pointer in each import group simultaneously like
// in a merge sort.
func nextIn(ctx context.Context, s *inState, btGroup *bigtable.Group) (bool, error) {
	accs := []*bigtable.Accessor{}
	for _, e := range s.entities {
		for _, p := range s.properties {
			types := []string{}
			for t := range s.next[e][p] {
				types = append(types, t)
			}
			sort.Strings(types)
			for _, t := range types {
				if s.heap[e][p][t].Len() == 0 {
					// All entities in all import groups for [e, p, t]have been exhausted.
					// Delete this entry in "s.next" so the outer for loop can skip it.
					delete(s.next[e][p], t)
					s.cursorGroup[e][p][t] = nil
					continue
				}
				elem := heap.Pop(s.heap[e][p][t]).(*heapElem)
				entity, ig := elem.data, elem.ig
				// If this entity has multiple types, check if it has been processed
				// for the other types already.
				processed := false
				if len(entity.Types) > 0 {
					for _, t := range entity.Types {
						if s.cursorGroup[e][p][t] == nil {
							// The cursor group for <e,p,t> is nil, which has been fully
							// processed in previous page.
							processed = true
							break
						}
						l := s.mergedEntities[e][p][t]
						if len(l) > 0 && entity.Dcid <= l[len(l)-1].Dcid {
							// This entity has been processed for type "t".
							processed = true
							break
						}
					}
				}
				if !processed {
					// Add the entity to "mergedEntities".
					if len(s.mergedEntities[e][p][t]) == 0 {
						s.mergedEntities[e][p][t] = []*pb.EntityInfo{entity}
					} else {
						prev := s.mergedEntities[e][p][t][len(s.mergedEntities[e][p][t])-1]
						if entity.Dcid != prev.Dcid || entity.Value != prev.Value {
							// Find a new entity, add to the result.
							s.mergedEntities[e][p][t] = append(s.mergedEntities[e][p][t], entity)
						}
					}
				}
				// Got enough entities, should stop.
				// Cursor is now at the next read item, ready to be returned.
				//
				// Here go past one entity over limit to ensure all duplicated entries have
				// been processed. Otherwise, next API request could get duplicate entries.

				// For example, given the two import groups data below and limit of 1, this
				// ensures the duplicated entry "a" are all processed in this request, and
				// next request would start processing from "b".

				// import group 1: ["a", "a", "b"]
				// import group 2: ["a", "c"]
				if len(s.mergedEntities[e][p][t]) == s.limit+1 {
					s.mergedEntities[e][p][t] = s.mergedEntities[e][p][t][:s.limit]
					delete(s.next[e][p], t)
				}
				if _, ok := s.next[e][p][t]; ok {
					// Update the cursor.
					cursor := s.cursorGroup[e][p][t][ig]
					cursor.Item++
					// Reach the end of the current page, should advance to next page.
					if int(cursor.Item) == len(s.rawEntities[e][p][t][ig]) {
						cursor.Page++
						cursor.Item = 0
						// No more pages
						if cursor.Page == int32(s.totalPage[e][p][t][ig]) {
							s.rawEntities[e][p][t][ig] = nil
						} else {
							accs = append(accs, &bigtable.Accessor{
								ImportGroup: ig,
								Body: [][]string{
									{e},
									{p},
									{t},
									{strconv.Itoa(int(cursor.Page))},
								},
							})
						}
					}
					s.next[e][p][t] = cursor
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
	for _, e := range s.entities {
		for _, p := range s.properties {
			types := []string{}
			for t := range s.cursorGroup[e][p] {
				types = append(types, t)
			}
			sort.Strings(types)
			for _, t := range types {
				if cursor, ok := s.next[e][p][t]; ok {
					// If there is data available in the current import group, push to the heap.
					if s.rawEntities[e][p][t][cursor.GetImportGroup()] != nil {
						elem := &heapElem{
							ig:   int(cursor.GetImportGroup()),
							pos:  cursor.GetItem(),
							data: s.rawEntities[e][p][t][cursor.GetImportGroup()][cursor.GetItem()],
						}
						heap.Push(s.heap[e][p][t], elem)
					}
				}
			}
		}
	}

	hasNext := false
	for e := range s.next {
		for p := range s.next[e] {
			if len(s.next[e][p]) > 0 {
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
