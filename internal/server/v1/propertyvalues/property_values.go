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
	properties []string,
	entities []string,
	limit int,
	token string,
	direction string,
) (
	map[string]map[string][]*pb.EntityInfo,
	*pb.PaginationInfo,
	error,
) {
	var err error
	// Empty cursor groups when no token is given.
	var cursorGroups []*pb.CursorGroup
	if token == "" {
		cursorGroups = buildDefaultCursorGroups(
			properties, entities, len(store.BtGroup.Tables()))
	} else {
		pi, err := pagination.Decode(token)
		if err != nil {
			return nil, nil, status.Errorf(
				codes.InvalidArgument, "invalid pagination token: %s", token)
		}
		if direction == util.DirectionOut {
			cursorGroups = pi.OutCursorGroups
		} else {
			cursorGroups = pi.InCursorGroups
		}
	}
	if limit == 0 || limit > defaultLimit {
		limit = defaultLimit
	}
	cursorGroup := map[string]map[string][]*pb.Cursor{}
	for _, g := range cursorGroups {
		keys := g.GetKeys()
		// First key is entity, second key is property.
		if len(keys) != 2 {
			return nil, nil, status.Errorf(
				codes.Internal, "cursor should have two keys, cursor: %s", g)
		}
		p, e := keys[1], keys[0]
		if _, ok := cursorGroup[p]; !ok {
			cursorGroup[p] = map[string][]*pb.Cursor{}
		}
		cursorGroup[p][e] = g.GetCursors()
	}
	if direction == util.DirectionOut {
		s := &outState{}
		if err = s.init(ctx, store.BtGroup, properties, entities, limit, cursorGroup); err != nil {
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
		for p := range s.rawEntities {
			for e := range s.rawEntities[p] {
				if s.rawEntities[p][e][s.usedImportGroup[p][e]] != nil {
					return s.mergedEntities, s.getPagination(util.DirectionOut), nil
				}
			}
		}
		return s.mergedEntities, nil, nil
	} else {
		s := &inState{}
		if err = s.init(ctx, store.BtGroup, properties, entities, limit, cursorGroup); err != nil {
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
		for p := range s.rawEntities {
			for e := range s.rawEntities[p] {
				for _, d := range s.rawEntities[p][e] {
					if d != nil {
						return s.mergedEntities, s.getPagination(util.DirectionIn), nil
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
	for _, p := range s.properties {
		for _, e := range s.entities {
			// No raw data for this "property", "entity"
			if _, ok := s.next[p][e]; !ok {
				continue
			}
			if len(s.mergedEntities[p][e]) == s.limit {
				delete(s.next[p], e)
				continue
			}
			ig := s.usedImportGroup[p][e]
			// Update the cursor.
			cursor := s.cursorGroup[p][e][ig]
			if _, ok := s.mergedEntities[p][e]; !ok {
				s.mergedEntities[p][e] = []*pb.EntityInfo{}
			}
			s.mergedEntities[p][e] = append(
				s.mergedEntities[p][e],
				s.rawEntities[p][e][ig][cursor.Item],
			)
			cursor.Item++
			// Still need more data, mark in s.hasNext
			s.next[p][e] = cursor
			// Reach the end of the current page, should advance to next page.
			if int(cursor.Item) == len(s.rawEntities[p][e][ig]) {
				cursor.Page++
				cursor.Item = 0
				// No more pages
				if cursor.Page == int32(s.totalPage[p][e][ig]) {
					s.rawEntities[p][e][ig] = nil
					delete(s.next[p], e)
				} else {
					s.next[p][e] = cursor
					accs = append(accs, &bigtable.Accessor{
						ImportGroup: ig,
						Body: [][]string{
							{e},
							{p},
							{strconv.Itoa(int(cursor.Page))},
						},
					})
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
	for p := range s.next {
		if len(s.next[p]) > 0 {
			hasNext = true
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
	for _, p := range s.properties {
		for _, e := range s.entities {
			// All entities in all import groups have been exhausted.
			if s.heap[p][e].Len() == 0 {
				delete(s.next[p], e)
				continue
			}
			elem := heap.Pop(s.heap[p][e]).(*heapElem)
			entity, ig := elem.data, elem.ig
			if len(s.mergedEntities[p][e]) == 0 {
				s.mergedEntities[p][e] = []*pb.EntityInfo{entity}
			} else {
				prev := s.mergedEntities[p][e][len(s.mergedEntities[p][e])-1]
				if entity.Dcid != prev.Dcid || entity.Value != prev.Value {
					// Find a new entity, add to the result.
					s.mergedEntities[p][e] = append(s.mergedEntities[p][e], entity)
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
			if len(s.mergedEntities[p][e]) == s.limit+1 {
				s.mergedEntities[p][e] = s.mergedEntities[p][e][:s.limit]
				delete(s.next[p], e)
			}
			if _, ok := s.next[p][e]; ok {
				// Update the cursor.
				cursor := s.cursorGroup[p][e][ig]
				cursor.Item++
				// Reach the end of the current page, should advance to next page.
				if int(cursor.Item) == len(s.rawEntities[p][e][ig]) {
					cursor.Page++
					cursor.Item = 0
					// No more pages
					if cursor.Page == int32(s.totalPage[p][e][ig]) {
						s.rawEntities[p][e][ig] = nil
						delete(s.next[p], e)
					} else {
						accs = append(accs, &bigtable.Accessor{
							ImportGroup: ig,
							Body: [][]string{
								{e},
								{p},
								{strconv.Itoa(int(cursor.Page))},
							},
						})
					}
				}
				s.next[p][e] = cursor
			}
		}
	}
	if len(accs) > 0 {
		err := s.readBt(ctx, btGroup, false, accs)
		if err != nil {
			return false, err
		}
	}
	for _, p := range s.properties {
		for _, e := range s.entities {
			if cursor, ok := s.next[p][e]; ok {
				// If there is data available in the current import group, push to the heap.
				if s.rawEntities[p][e][cursor.GetImportGroup()] != nil {
					elem := &heapElem{
						ig:   int(cursor.GetImportGroup()),
						pos:  cursor.GetItem(),
						data: s.rawEntities[p][e][cursor.GetImportGroup()][cursor.GetItem()],
					}
					heap.Push(s.heap[p][e], elem)
				}
			}
		}
	}

	hasNext := false
	for p := range s.next {
		if len(s.next[p]) > 0 {
			hasNext = true
			break
		}
	}
	return hasNext, nil
}
