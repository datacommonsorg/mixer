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

var defaultLimit = 1000

type processor struct {
	property string
	entity   string
	limit    int
	// CursorGroup that tracks the state of current data.
	cg *pb.CursorGroup
	// Raw entities read from BigTable
	raw [][]*pb.EntityInfo
	// Merged entities
	merged []*pb.EntityInfo
	// Min heap for entity merge sort
	h *entityHeap
	// Total page count for each import group
	totalPage map[int]int
}

// newProcessor builds a new processor given property, entity and cursor group.
func newProcessor(
	ctx context.Context,
	btGroup *bigtable.Group,
	property string,
	entity string,
	limit int,
	cg *pb.CursorGroup,
) (*processor, error) {
	// Constructor
	p := &processor{
		property:  property,
		entity:    entity,
		cg:        cg,
		raw:       nil,
		merged:    nil,
		limit:     limit,
		h:         &entityHeap{},
		totalPage: map[int]int{},
	}
	// Read cache data based on pagination info.
	rowListMap := map[int]cbt.RowList{}
	for _, c := range cg.Cursors {
		if c != nil {
			rowList := buildSimpleRequestRowList(entity, property, false, c.GetPage())
			rowListMap[int(c.GetIg())] = rowList
		}
	}
	btDataList, err := bigtable.ReadWithGroupRowList(
		ctx,
		btGroup,
		rowListMap,
		func(jsonRaw []byte) (interface{}, error) {
			var p pb.PagedEntities
			err := proto.Unmarshal(jsonRaw, &p)
			return &p, err
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	// Store the raw cache data.
	for idx, btData := range btDataList {
		if data, ok := btData[entity]; ok {
			pe := data.(*pb.PagedEntities)
			p.raw = append(p.raw, pe.Entities)
			p.totalPage[idx] = int(*pe.TotalPageCount)
		} else {
			p.raw = append(p.raw, nil)
		}
	}
	// Init the min heap.
	heap.Init(p.h)
	// Push the next entity of each import group to the heap.
	for idx, entityList := range p.raw {
		cursor := cg.Cursors[idx]
		if int(cursor.GetItem()) < len(entityList) {
			elem := &heapElem{
				ig:   idx,
				pos:  cursor.GetItem(),
				data: entityList[cursor.GetItem()],
			}
			heap.Push(p.h, elem)
		}
	}
	return p, nil
}

// Process the next entity.
//
// Import groups are ordered by preferences, so all entities from the preferred
// import groups should be added first. Additional entities from other non
// preferred import groups are added by the preference order.
//
// As the data in each import group is sorted already. The duplication is
// handled by advancing the pointer in each import group simounateously like
// in a merge sort.
func (p *processor) next(ctx context.Context, btGroup *bigtable.Group) (bool, error) {
	// All entities in all import groups have been exhausted.
	if p.h.Len() == 0 {
		return false, nil
	}
	elem := heap.Pop(p.h).(*heapElem)
	entity := elem.data
	ig := elem.ig
	if len(p.merged) == 0 {
		p.merged = []*pb.EntityInfo{entity}
	} else {
		prev := p.merged[len(p.merged)-1]
		if entity.Dcid != prev.Dcid || entity.Value != prev.Value {
			// Find a new entity, add to the result.
			p.merged = append(p.merged, entity)
		}
	}
	// Update the cursor.
	cursor := p.cg.Cursors[ig]
	cursor.Item++
	// Reach the end of the current page, should advance to next page.
	if int(cursor.Item) == len(p.raw[ig]) {
		cursor.Page++
		cursor.Item = 0
	}
	// Got enough entities, should stop the process.
	if len(p.merged) == p.limit {
		return false, nil
	}
	// Need to update raw data if advancing to the next page.
	if cursor.Item == 0 {
		// No more pages
		if cursor.Page == int32(p.totalPage[ig]) {
			p.raw[ig] = nil
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
				func(jsonRaw []byte) (interface{}, error) {
					var p pb.PagedEntities
					err := proto.Unmarshal(jsonRaw, &p)
					return &p, err
				},
				nil,
			)
			if err != nil {
				return false, err
			}
			if data, ok := btDataList[ig][p.entity]; ok {
				pe := data.(*pb.PagedEntities)
				p.raw[ig] = pe.Entities
				p.totalPage[ig] = int(*pe.TotalPageCount)
			}
		}
	}
	// If there is data available in the current import group, push to the heap.
	if p.raw[ig] != nil {
		elem := &heapElem{
			ig:   ig,
			pos:  cursor.GetItem(),
			data: p.raw[ig][cursor.GetItem()],
		}
		heap.Push(p.h, elem)
	}
	return true, nil
}

func buildEmptyCursorGroup(n int) *pb.CursorGroup {
	result := &pb.CursorGroup{Cursors: []*pb.Cursor{}}
	for i := 0; i < n; i++ {
		result.Cursors = append(result.Cursors, &pb.Cursor{Ig: int32(i)})
	}
	return result
}

func InPropertyValues(
	ctx context.Context,
	in *pb.InPropertyValuesRequest,
	store *store.Store,
) (*pb.InPropertyValuesResponse, error) {
	property := in.GetProperty()
	entity := in.GetEntity()
	limit := int(in.GetLimit())
	token := in.GetToken()
	// Check arguments
	if property == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument: property")
	}
	if !util.CheckValidDCIDs([]string{entity}) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid entity %s", entity)
	}
	var err error
	// Empty cursor group when no token is given.
	pi := &pb.PaginationInfo{CursorGroups: []*pb.CursorGroup{{}}}
	if token != "" {
		pi, err = pagination.Decode(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid pagination token: %s", token)
		}
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
	for _, d := range proc.raw {
		// If any import group has more data, then should compute the token
		if d != nil {
			respToken, err = util.EncodeProto(
				&pb.PaginationInfo{
					CursorGroups: []*pb.CursorGroup{proc.cg},
				})
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return &pb.InPropertyValuesResponse{
		Data:  proc.merged,
		Token: respToken,
	}, nil
}
