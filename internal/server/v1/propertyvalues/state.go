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
	"log"
	"strconv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
)

// state holds raw and processed data for property values API.
// This struct is to be extended for in / out state.
type state struct {
	properties []string
	entities   []string
	limit      int
	// CursorGroup that tracks the state of current data.
	// Key: property, entity
	cursorGroup map[string]map[string][]*pb.Cursor
	// Raw entities read from BigTable
	// Key: property, entity
	rawEntities map[string]map[string][][]*pb.EntityInfo
	// Merged entities
	// Key: property, entity
	mergedEntities map[string]map[string][]*pb.EntityInfo
	// Total page count for each import group
	totalPage map[string]map[string]map[int]int
	// Record the import group for next item to read
	next map[string]map[string]*pb.Cursor
}

type inState struct {
	state
	// Min heap for entity merge sort
	// Key: property, entity
	heap map[string]map[string]*entityHeap
}

type outState struct {
	state
	// The used import group (only one import group is used for out property values).
	// Key: property, entity
	usedImportGroup map[string]map[string]int
}

// init builds a new state given property, entity and cursor group.
func (s *state) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	properties []string,
	entities []string,
	limit int,
	cursorGroup map[string]map[string][]*pb.Cursor,
	arcOut bool,
) error {
	// Constructor
	s.properties = properties
	s.entities = entities
	s.cursorGroup = cursorGroup
	s.rawEntities = map[string]map[string][][]*pb.EntityInfo{}
	s.mergedEntities = map[string]map[string][]*pb.EntityInfo{}
	s.limit = limit
	s.totalPage = map[string]map[string]map[int]int{}
	s.next = map[string]map[string]*pb.Cursor{}
	for _, p := range properties {
		s.next[p] = map[string]*pb.Cursor{}
		s.mergedEntities[p] = map[string][]*pb.EntityInfo{}
	}
	accs := []*bigtable.Accessor{}
	for property := range cursorGroup {
		for entity := range cursorGroup[property] {
			for _, c := range cursorGroup[property][entity] {
				if c != nil {
					accs = append(accs, &bigtable.Accessor{
						ImportGroup: int(c.GetImportGroup()),
						Body: [][]string{
							{entity},
							{property},
							{strconv.Itoa(int(c.GetPage()))},
						},
					})
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
	log.Printf("Read data: %+v", accs)
	prefix := bigtable.BtPagedPropValOut
	if !arcOut {
		prefix = bigtable.BtPagedPropValIn
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
			e := row.Parts[0]
			p := row.Parts[1]
			values := row.Data.(*pb.PagedEntities)
			if _, ok := s.rawEntities[p]; !ok {
				s.rawEntities[p] = map[string][][]*pb.EntityInfo{}
			}
			if _, ok := s.rawEntities[p][e]; !ok {
				s.rawEntities[p][e] = make([][]*pb.EntityInfo, n)
			}
			s.rawEntities[p][e][idx] = values.Entities
			if _, ok := s.totalPage[p]; !ok {
				s.totalPage[p] = map[string]map[int]int{}
			}
			if _, ok := s.totalPage[p][e]; !ok {
				s.totalPage[p][e] = map[int]int{}
			}
			s.totalPage[p][e][idx] = int(values.TotalPageCount)
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
	cursorGroup map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, properties, entities, limit, cursorGroup, false)
	if err != nil {
		return err
	}
	s.heap = map[string]map[string]*entityHeap{}
	for _, p := range properties {
		s.heap[p] = map[string]*entityHeap{}
		for _, e := range entities {
			s.heap[p][e] = &entityHeap{}
			// Init the min heap
			heap.Init(s.heap[p][e])
			// Push the next entity of each import group to the heap.
			for idx, entityList := range s.rawEntities[p][e] {
				cursor := cursorGroup[p][e][idx]
				if int(cursor.GetItem()) < len(entityList) {
					elem := &heapElem{
						ig:   idx,
						pos:  cursor.GetItem(),
						data: entityList[cursor.GetItem()],
					}
					heap.Push(s.heap[p][e], elem)
					s.next[p][e] = cursor
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
	cursorGroup map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, properties, entities, limit, cursorGroup, true)
	if err != nil {
		return err
	}
	s.usedImportGroup = map[string]map[string]int{}
	for p := range s.rawEntities {
		s.usedImportGroup[p] = map[string]int{}
		for e := range s.rawEntities[p] {
			for idx, data := range s.rawEntities[p][e] {
				if len(data) > 0 {
					s.usedImportGroup[p][e] = idx
					s.next[p][e] = cursorGroup[p][e][idx]
					break
				}
			}
		}
	}
	return nil
}

func (s *state) getPagination(direction string) *pb.PaginationInfo {
	cursorGroups := []*pb.CursorGroup{}
	for _, p := range s.properties {
		for _, e := range s.entities {
			cursorGroups = append(
				cursorGroups,
				&pb.CursorGroup{
					Keys:    []string{e, p},
					Cursors: s.cursorGroup[p][e],
				},
			)
		}
	}
	res := &pb.PaginationInfo{}
	if direction == util.DirectionOut {
		res.OutCursorGroups = cursorGroups
	} else {
		res.InCursorGroups = cursorGroups
	}
	return res
}
