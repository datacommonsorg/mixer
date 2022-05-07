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

	cbt "cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
)

type state struct {
	property string
	entity   string
	limit    int
	// CursorGroup that tracks the state of current data.
	cursorGroup *pb.CursorGroup
	// Raw entities read from BigTable
	rawEntities [][]*pb.EntityInfo
	// Total page count for each import group
	// Merged entities
	mergedEntities []*pb.EntityInfo
	totalPage      map[int]int
}

type inState struct {
	state
	// Min heap for entity merge sort
	heap *entityHeap
}

type outState struct {
	state
	// The used import group (only one import group is used for out property values).
	usedImportGroup int
	//
}

// init builds a new state given property, entity and cursor group.
func (s *state) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	property string,
	entity string,
	limit int,
	cursorGroup *pb.CursorGroup,
	arcOut bool,
) error {
	// Constructor
	s.property = property
	s.entity = entity
	s.cursorGroup = cursorGroup
	s.rawEntities = nil
	s.mergedEntities = nil
	s.limit = limit
	s.totalPage = map[int]int{}
	// Read cache data based on pagination info.
	rowListMap := map[int]cbt.RowList{}
	for _, c := range cursorGroup.Cursors {
		if c != nil {
			rowList := buildSimpleRequestRowList(entity, property, arcOut, c.GetPage())
			rowListMap[int(c.GetImportGroup())] = rowList
		}
	}
	btDataList, err := bigtable.ReadWithGroupRowList(
		ctx,
		btGroup,
		rowListMap,
		action,
		nil,
	)
	if err != nil {
		return err
	}
	// Store the raw cache data.
	for idx, btData := range btDataList {
		if data, ok := btData[entity]; ok {
			pe := data.(*pb.PagedEntities)
			s.rawEntities = append(s.rawEntities, pe.Entities)
			s.totalPage[idx] = int(pe.TotalPageCount)
		} else {
			s.rawEntities = append(s.rawEntities, nil)
		}
	}
	return nil
}

func (s *state) readNextPage(
	ctx context.Context,
	btGroup *bigtable.Group,
	arcOut bool,
	importGroup int,
	page int32,
) error {
	log.Printf("Read new page: import group %d, page %d", importGroup, page)
	rowList := buildSimpleRequestRowList(s.entity, s.property, arcOut, page)
	rowListMap := map[int]cbt.RowList{
		importGroup: rowList,
	}
	btDataList, err := bigtable.ReadWithGroupRowList(
		ctx,
		btGroup,
		rowListMap,
		action,
		nil,
	)
	if err != nil {
		return err
	}
	if data, ok := btDataList[importGroup][s.entity]; ok {
		pe := data.(*pb.PagedEntities)
		s.rawEntities[importGroup] = pe.Entities
		s.totalPage[importGroup] = int(pe.TotalPageCount)
	}
	return nil
}

// init the state for in property values API
func (s *inState) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	property string,
	entity string,
	limit int,
	cursorGroup *pb.CursorGroup,
) error {
	err := s.state.init(ctx, btGroup, property, entity, limit, cursorGroup, false)
	if err != nil {
		return err
	}
	s.heap = &entityHeap{}
	// Init the min heapš
	heap.Init(s.heap)
	// Push the next entity of each import group to the heap.
	for idx, entityList := range s.rawEntities {
		cursor := cursorGroup.Cursors[idx]
		if int(cursor.GetItem()) < len(entityList) {
			elem := &heapElem{
				ig:   idx,
				pos:  cursor.GetItem(),
				data: entityList[cursor.GetItem()],
			}
			heap.Push(s.heap, elem)
		}
	}
	return nil
}

// init the state for out property values API
func (s *outState) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	property string,
	entity string,
	limit int,
	cursorGroup *pb.CursorGroup,
) error {
	err := s.state.init(ctx, btGroup, property, entity, limit, cursorGroup, true)
	if err != nil {
		return err
	}
	for idx, data := range s.rawEntities {
		if len(data) > 0 {
			s.usedImportGroup = idx
			break
		}
	}
	return nil
}
