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
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
)

// state holds raw and processed data for property values API.
// This struct is to be extended for in / out state.
type state struct {
	entities   []string
	properties []string
	limit      int
	// CursorGroup that tracks the state of current data.
	// Key: entity, property, type
	cursorGroup map[string]map[string]map[string][]*pb.Cursor
	// Raw entities read from BigTable
	// Key: entity, property, type
	rawEntities map[string]map[string]map[string][][]*pb.EntityInfo
	// Merged entities
	// Key: entity, property, type
	mergedEntities map[string]map[string]map[string][]*pb.EntityInfo
	// Total page count for each import group
	// Key: entity, property, type, import group index
	totalPage map[string]map[string]map[string]map[int]int
	// Record the import group for next item to read
	// Key: entity, property, type
	next map[string]map[string]map[string]*pb.Cursor
}

type inState struct {
	state
	// Min heap for entity merge sort
	// Key: entity, property, type
	heap map[string]map[string]map[string]*entityHeap
}

type outState struct {
	state
	// The used import group (only one import group is used for out property values).
	// Key: entity, property, type
	usedImportGroup map[string]map[string]map[string]int
}

// init builds a new state given entity, property and cursor group.
func (s *state) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	entities []string,
	properties []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
	arcOut bool,
) error {
	// Constructor
	s.entities = entities
	s.properties = properties
	s.cursorGroup = cursorGroup
	s.rawEntities = map[string]map[string]map[string][][]*pb.EntityInfo{}
	s.mergedEntities = map[string]map[string]map[string][]*pb.EntityInfo{}
	s.limit = limit
	s.totalPage = map[string]map[string]map[string]map[int]int{}
	s.next = map[string]map[string]map[string]*pb.Cursor{}
	for _, e := range entities {
		s.next[e] = map[string]map[string]*pb.Cursor{}
		s.mergedEntities[e] = map[string]map[string][]*pb.EntityInfo{}
		for _, p := range properties {
			s.next[e][p] = map[string]*pb.Cursor{}
			s.mergedEntities[e][p] = map[string][]*pb.EntityInfo{}
		}
	}
	accs := []*bigtable.Accessor{}
	for e := range cursorGroup {
		for p := range cursorGroup[e] {
			for t := range cursorGroup[e][p] {
				for _, c := range cursorGroup[e][p][t] {
					if c != nil {
						accs = append(accs, &bigtable.Accessor{
							ImportGroup: int(c.GetImportGroup()),
							Body: [][]string{
								{e},
								{p},
								{t},
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
			if _, ok := s.rawEntities[e]; !ok {
				s.rawEntities[e] = map[string]map[string][][]*pb.EntityInfo{}
			}
			if _, ok := s.rawEntities[e][p]; !ok {
				s.rawEntities[e][p] = make(map[string][][]*pb.EntityInfo, n)
			}
			if _, ok := s.rawEntities[e][p][t]; !ok {
				s.rawEntities[e][p][t] = make([][]*pb.EntityInfo, n)
			}
			s.rawEntities[e][p][t][idx] = values.Entities
			if _, ok := s.totalPage[e]; !ok {
				s.totalPage[e] = map[string]map[string]map[int]int{}
			}
			if _, ok := s.totalPage[e][p]; !ok {
				s.totalPage[e][p] = map[string]map[int]int{}
			}
			if _, ok := s.totalPage[e][p][t]; !ok {
				s.totalPage[e][p][t] = map[int]int{}
			}
			s.totalPage[e][p][t][idx] = int(values.TotalPageCount)
		}
	}
	return nil
}

// init the state for in property values API
func (s *inState) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	entities []string,
	properties []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, entities, properties, limit, cursorGroup, false)
	if err != nil {
		return err
	}
	s.heap = map[string]map[string]map[string]*entityHeap{}
	for _, e := range entities {
		s.heap[e] = map[string]map[string]*entityHeap{}
		for _, p := range properties {
			s.heap[e][p] = map[string]*entityHeap{}
			// Push the next entity of each import group to the heap.
			for t, typedEntityList := range s.rawEntities[e][p] {
				s.heap[e][p][t] = &entityHeap{}
				// Init the min heap
				heap.Init(s.heap[e][p][t])
				for idx, entityList := range typedEntityList {
					cursor := cursorGroup[e][p][t][idx]
					if int(cursor.GetItem()) < len(entityList) {
						elem := &heapElem{
							ig:   idx,
							pos:  cursor.GetItem(),
							data: entityList[cursor.GetItem()],
						}
						heap.Push(s.heap[e][p][t], elem)
						s.next[e][p][t] = cursor
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
	entities []string,
	properties []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, entities, properties, limit, cursorGroup, true)
	if err != nil {
		return err
	}
	s.usedImportGroup = map[string]map[string]map[string]int{}
	for e := range s.rawEntities {
		s.usedImportGroup[e] = map[string]map[string]int{}
		for p := range s.rawEntities[e] {
			s.usedImportGroup[e][p] = map[string]int{}
			for t := range s.rawEntities[e][p] {
				for idx, data := range s.rawEntities[e][p][t] {
					if len(data) > 0 {
						s.usedImportGroup[e][p][t] = idx
						s.next[e][p][t] = cursorGroup[e][p][t][idx]
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
	for e := range s.cursorGroup {
		for p := range s.cursorGroup[e] {
			for t := range s.cursorGroup[e][p] {
				if s.cursorGroup[e][p][t] != nil {
					cursorGroups = append(
						cursorGroups,
						&pb.CursorGroup{
							Keys:    []string{e, p, t},
							Cursors: s.cursorGroup[e][p][t],
						},
					)
				}
			}
		}
	}
	sort.SliceStable(cursorGroups, func(i, j int) bool {
		keysi := cursorGroups[i].Keys
		keysj := cursorGroups[j].Keys
		if keysi[0] == keysj[0] {
			if keysi[1] == keysj[1] {
				return keysi[2] < keysj[2]
			}
			return keysi[1] < keysj[1]
		}
		return keysi[0] < keysj[0]
	})
	return &pb.PaginationInfo{CursorGroups: cursorGroups}
}
