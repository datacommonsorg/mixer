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
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Cache builder generates page size of 500.
// This (approximately) allows one request to be fulfilled by reading one page
// from all import groups after merging.
var defaultLimit = 1000

type processor struct {
	property string
	entity   string
	limit    int
	// CursorGroup that tracks the state of current data.
	cursorGroup *pb.CursorGroup
	// Raw entities read from BigTable
	rawEntities [][]*pb.EntityInfo
	// Merged entities
	mergedEntities []*pb.EntityInfo
	// Min heap for entity merge sort
	heap *entityHeap
	// Total page count for each import group
	totalPage map[int]int
}

// InPropertyValues implements mixer.InPropertyValues handler.
func InPropertyValues(
	ctx context.Context,
	in *pb.InPropertyValuesRequest,
	store *store.Store,
) (*pb.InPropertyValuesResponse, error) {
	property := in.GetProperty()
	entity := in.GetEntity()
	limit := int(in.GetLimit())
	token := in.GetNextToken()
	// Check arguments
	if property == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing required argument: property")
	}
	if !util.CheckValidDCIDs([]string{entity}) {
		return nil, status.Errorf(
			codes.InvalidArgument, "invalid entity %s", entity)
	}
	var err error
	// Empty cursor group when no token is given.
	pi := &pb.PaginationInfo{CursorGroups: []*pb.CursorGroup{{}}}
	if token != "" {
		pi, err = pagination.Decode(token)
		if err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument, "invalid pagination token: %s", token)
		}
		// Simple API should have exact one entity, thus one cursor group.
		if len(pi.CursorGroups) != 1 {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"pagination group size should be 1, got %d instead",
				len(pi.CursorGroups),
			)
		}
	}
	if limit == 0 || limit > defaultLimit {
		limit = defaultLimit
	}
	cursorGroup := pi.CursorGroups[0]
	// build empty cursorGroup
	if len(cursorGroup.Cursors) == 0 {
		cursorGroup = buildEmptyCursorGroup(len(store.BtGroup.Tables()))
	}
	proc, err := newProcessor(ctx, store.BtGroup, property, entity, limit, cursorGroup)
	if err != nil {
		return nil, err
	}
	for {
		hasNext, err := proc.next(ctx, store.BtGroup)
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}
	}
	respToken := ""
	for _, d := range proc.rawEntities {
		// If any import group has more data, then should compute the token
		if d != nil {
			respToken, err = util.EncodeProto(
				&pb.PaginationInfo{
					CursorGroups: []*pb.CursorGroup{proc.cursorGroup},
				})
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return &pb.InPropertyValuesResponse{
		Data:      proc.mergedEntities,
		NextToken: respToken,
	}, nil
}

var action = func(jsonRaw []byte) (interface{}, error) {
	var p pb.PagedEntities
	err := proto.Unmarshal(jsonRaw, &p)
	return &p, err
}

// newProcessor builds a new processor given property, entity and cursor group.
func newProcessor(
	ctx context.Context,
	btGroup *bigtable.Group,
	property string,
	entity string,
	limit int,
	cursorGroup *pb.CursorGroup,
) (*processor, error) {
	// Constructor
	p := &processor{
		property:       property,
		entity:         entity,
		cursorGroup:    cursorGroup,
		rawEntities:    nil,
		mergedEntities: nil,
		limit:          limit,
		heap:           &entityHeap{},
		totalPage:      map[int]int{},
	}
	// Read cache data based on pagination info.
	rowListMap := map[int]cbt.RowList{}
	for _, c := range cursorGroup.Cursors {
		if c != nil {
			rowList := buildSimpleRequestRowList(entity, property, false, c.GetPage())
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
		return nil, err
	}
	// Store the raw cache data.
	for idx, btData := range btDataList {
		if data, ok := btData[entity]; ok {
			pe := data.(*pb.PagedEntities)
			p.rawEntities = append(p.rawEntities, pe.Entities)
			p.totalPage[idx] = int(pe.TotalPageCount)
		} else {
			p.rawEntities = append(p.rawEntities, nil)
		}
	}
	// Init the min heap.
	heap.Init(p.heap)
	// Push the next entity of each import group to the heap.
	for idx, entityList := range p.rawEntities {
		cursor := cursorGroup.Cursors[idx]
		if int(cursor.GetItem()) < len(entityList) {
			elem := &heapElem{
				ig:   idx,
				pos:  cursor.GetItem(),
				data: entityList[cursor.GetItem()],
			}
			heap.Push(p.heap, elem)
		}
	}
	return p, nil
}

// Process the next entity.
//
// As the data in each import group is sorted already. The duplication is
// handled by advancing the pointer in each import group simultaneously like
// in a merge sort.
func (p *processor) next(ctx context.Context, btGroup *bigtable.Group) (bool, error) {
	// All entities in all import groups have been exhausted.
	if p.heap.Len() == 0 {
		return false, nil
	}
	elem := heap.Pop(p.heap).(*heapElem)
	entity, ig := elem.data, elem.ig
	if len(p.mergedEntities) == 0 {
		p.mergedEntities = []*pb.EntityInfo{entity}
	} else {
		prev := p.mergedEntities[len(p.mergedEntities)-1]
		if entity.Dcid != prev.Dcid || entity.Value != prev.Value {
			// Find a new entity, add to the result.
			p.mergedEntities = append(p.mergedEntities, entity)
		}
	}
	// Got enough entities, should stop the process.
	// Cursor is now at the next read item, ready to be returned.
	//
	// Here go past one entity over limit to ensure all duplicated entries have
	// been processed. Otherwise, next API request could get duplicate entries.

	// For example, given the two import groups data below and limit of 1, this
	// ensures the duplicated entry "a" are all processed in this request, and
	// next request would start processing from "b".

	// import group 1: ["a", "a", "b"]
	// import group 2: ["a", "c"]
	if len(p.mergedEntities) == p.limit+1 {
		p.mergedEntities = p.mergedEntities[:p.limit]
		return false, nil
	}
	// Update the cursor.
	cursor := p.cursorGroup.Cursors[ig]
	cursor.Item++
	// Reach the end of the current page, should advance to next page.
	if int(cursor.Item) == len(p.rawEntities[ig]) {
		cursor.Page++
		cursor.Item = 0
		// No more pages
		if cursor.Page == int32(p.totalPage[ig]) {
			p.rawEntities[ig] = nil
		} else {
			log.Printf("Read new page: import group %d, page %d", ig, cursor.Page)
			rowList := buildSimpleRequestRowList(p.entity, p.property, false, cursor.Page)
			rowListMap := map[int]cbt.RowList{
				ig: rowList,
			}
			btDataList, err := bigtable.ReadWithGroupRowList(
				ctx,
				btGroup,
				rowListMap,
				action,
				nil,
			)
			if err != nil {
				return false, err
			}
			if data, ok := btDataList[ig][p.entity]; ok {
				pe := data.(*pb.PagedEntities)
				p.rawEntities[ig] = pe.Entities
				p.totalPage[ig] = int(pe.TotalPageCount)
			}
		}
	}
	// If there is data available in the current import group, push to the heap.
	if p.rawEntities[ig] != nil {
		elem := &heapElem{
			ig:   ig,
			pos:  cursor.GetItem(),
			data: p.rawEntities[ig][cursor.GetItem()],
		}
		heap.Push(p.heap, elem)
	}
	return true, nil
}

func buildEmptyCursorGroup(n int) *pb.CursorGroup {
	result := &pb.CursorGroup{Cursors: []*pb.Cursor{}}
	for i := 0; i < n; i++ {
		result.Cursors = append(result.Cursors, &pb.Cursor{ImportGroup: int32(i)})
	}
	return result
}
