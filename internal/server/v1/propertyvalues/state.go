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
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
)

// state holds raw and processed data for property values API.
// This struct is to be extended for in / out state.
type state struct {
	properties []string
	entities   []string
	limit      int
	// CursorGroup that tracks the state of current data.
	// Key: property, entity, neighbour node type
	cursorGroup map[string]map[string]map[string][]*pb.Cursor
	// Raw entities read from BigTable
	// Key: property, entity, neighbour node type
	rawEntities map[string]map[string]map[string][][]*pb.EntityInfo
	// Merged entities
	// Key: property, entity, neighbour node type
	mergedEntities map[string]map[string]map[string][]*pb.EntityInfo
	// Total page count for each import group
	// Key: property, entity, neighbour node type, import group index
	totalPage map[string]map[string]map[string]map[int]int
	// Record the import group for next item to read
	// Key: property, entity, neighbour node type
	next map[string]map[string]map[string]*pb.Cursor
}

type inState struct {
	state
	// Min heap for entity merge sort
	// Key: property, entity, neighbour node type
	heap map[string]map[string]map[string]*entityHeap
}

type outState struct {
	state
	// The used import group (only one import group is used for out property values).
	// Key: property, entity, neighbour node type
	usedImportGroup map[string]map[string]map[string]int
}

// init builds a new state given property, entity and cursor group.
func (s *state) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	properties []string,
	entities []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
	arcOut bool,
) error {
	// Constructor
	s.properties = properties
	s.entities = entities
	s.cursorGroup = cursorGroup
	s.rawEntities = map[string]map[string]map[string][][]*pb.EntityInfo{}
	s.mergedEntities = map[string]map[string]map[string][]*pb.EntityInfo{}
	s.limit = limit
	s.totalPage = map[string]map[string]map[string]map[int]int{}
	s.next = map[string]map[string]map[string]*pb.Cursor{}
	for _, p := range properties {
		s.next[p] = map[string]map[string]*pb.Cursor{}
		s.mergedEntities[p] = map[string]map[string][]*pb.EntityInfo{}
	}
	accs := []*bigtable.Accessor{}
	for property := range cursorGroup {
		for entity := range cursorGroup[property] {
			for typ := range cursorGroup[property][entity] {
				for _, c := range cursorGroup[property][entity][typ] {
					if c != nil {
						accs = append(accs, &bigtable.Accessor{
							ImportGroup: int(c.GetImportGroup()),
							Body: [][]string{
								{entity},
								{property},
								{typ},
								{strconv.Itoa(int(c.GetPage()))},
							},
						})
					}
				}
			}
		}
	}
	return s.readBt(ctx, btGroup, arcOut, accs)
}

func (s *state) readBt(
	ctx context.Context,
	btGroup *bigtable.Group,
	arcOut bool,
	accs []*bigtable.Accessor,
) error {
	prefix := bigtable.BtPagedPropTypeValOut
	if !arcOut {
		prefix = bigtable.BtPagedPropTypeValIn
	}
	btDataList, err := bigtable.ReadWithGroupRowList(
		ctx,
		btGroup,
		prefix,
		accs,
		unmarshalFunc,
	)
	if err != nil {
		return err
	}
	// Store the raw cache data.
	// The outer for loop is on the import group.
	n := len(btDataList)
	for idx, btData := range btDataList {
		if len(btData) == 0 {
			continue
		}
		for _, row := range btData {
			e := row.Parts[0] // entity
			p := row.Parts[1] // property
			t := row.Parts[2] // type
			values := row.Data.(*pb.PagedEntities)
			if _, ok := s.rawEntities[p]; !ok {
				s.rawEntities[p] = map[string]map[string][][]*pb.EntityInfo{}
			}
			if _, ok := s.rawEntities[p][e]; !ok {
				s.rawEntities[p][e] = make(map[string][][]*pb.EntityInfo, n)
			}
			s.rawEntities[p][e][t][idx] = values.Entities
			if _, ok := s.totalPage[p]; !ok {
				s.totalPage[p] = map[string]map[string]map[int]int{}
			}
			if _, ok := s.totalPage[p][e]; !ok {
				s.totalPage[p][e] = map[string]map[int]int{}
			}
			if _, ok := s.totalPage[p][e][t]; !ok {
				s.totalPage[p][e][t] = map[int]int{}
			}
			s.totalPage[p][e][t][idx] = int(values.TotalPageCount)
		}
	}
	return nil
}

// init the state for in property values API
func (s *inState) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	properties []string,
	entities []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, properties, entities, limit, cursorGroup, false)
	if err != nil {
		return err
	}
	s.heap = map[string]map[string]map[string]*entityHeap{}
	for _, p := range properties {
		s.heap[p] = map[string]map[string]*entityHeap{}
		for _, e := range entities {
			s.heap[p][e] = map[string]*entityHeap{}
			// Push the next entity of each import group to the heap.
			for t, typedEntityList := range s.rawEntities[p][e] {
				s.heap[p][e][t] = &entityHeap{}
				// Init the min heap
				heap.Init(s.heap[p][e][t])
				for idx, entityList := range typedEntityList {
					cursor := cursorGroup[p][e][t][idx]
					if int(cursor.GetItem()) < len(entityList) {
						elem := &heapElem{
							ig:   idx,
							pos:  cursor.GetItem(),
							data: entityList[cursor.GetItem()],
						}
						heap.Push(s.heap[p][e][t], elem)
						s.next[p][e][t] = cursor
					}
				}
			}
		}
	}
	return nil
}

// init the state for out property values API
func (s *outState) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	properties []string,
	entities []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, properties, entities, limit, cursorGroup, true)
	if err != nil {
		return err
	}
	s.usedImportGroup = map[string]map[string]map[string]int{}
	for p := range s.rawEntities {
		s.usedImportGroup[p] = map[string]map[string]int{}
		for e := range s.rawEntities[p] {
			for t, typedData := range s.rawEntities[p][e] {
				for idx, data := range typedData {
					if len(data) > 0 {
						s.usedImportGroup[p][e][t] = idx
						s.next[p][e][t] = cursorGroup[p][e][t][idx]
						break
					}
				}
			}
		}
	}
	return nil
}

func (s *state) getPagination() *pb.PaginationInfo {
	cursorGroups := []*pb.CursorGroup{}
	for p := range s.cursorGroup {
		for e := range s.cursorGroup[p] {
			for t := range s.cursorGroup[p][e] {
				cursorGroups = append(
					cursorGroups,
					&pb.CursorGroup{
						Keys:    []string{e, p, t},
						Cursors: s.cursorGroup[p][e][t],
					},
				)
			}
		}
	}
	return &pb.PaginationInfo{CursorGroups: cursorGroups}
}
