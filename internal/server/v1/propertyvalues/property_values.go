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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PropertyValues implements mixer.PropertyValues handler.
func PropertyValues(
	ctx context.Context,
	in *pb.PropertyValuesRequest,
	store *store.Store,
) (*pb.PropertyValuesResponse, error) {
	property := in.GetProperty()
	entity := in.GetEntity()
	limit := int(in.GetLimit())
	token := in.GetNextToken()
	direction := in.GetDirection()
	// Check arguments
	if property == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "missing required argument: property")
	}
	if direction != "out" && direction != "in" {
		return nil, status.Errorf(
			codes.InvalidArgument, "uri should be /v1/property/out/ or /v1/property/in/")
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

	respToken := ""
	var mergedEntities []*pb.EntityInfo
	if direction == "out" {
		s := &outState{}
		if err = s.init(ctx, store.BtGroup, property, entity, limit, cursorGroup); err != nil {
			return nil, err
		}
		for {
			hasNext, err := nextOut(ctx, s, store.BtGroup)
			if err != nil {
				return nil, err
			}
			if !hasNext {
				break
			}
		}
		// If used import group has more data, then should compute the token
		if s.rawEntities[s.used_import_group] != nil {
			respToken, err = util.EncodeProto(
				&pb.PaginationInfo{
					CursorGroups: []*pb.CursorGroup{s.cursorGroup},
				})
		}
		if err != nil {
			return nil, err
		}
		mergedEntities = s.mergedEntities
	} else {
		s := &inState{}
		if err = s.init(ctx, store.BtGroup, property, entity, limit, cursorGroup); err != nil {
			return nil, err
		}
		for {
			hasNext, err := nextIn(ctx, s, store.BtGroup)
			if err != nil {
				return nil, err
			}
			if !hasNext {
				break
			}
		}
		for _, d := range s.rawEntities {
			// If any import group has more data, then should compute the token
			if d != nil {
				respToken, err = util.EncodeProto(
					&pb.PaginationInfo{
						CursorGroups: []*pb.CursorGroup{s.cursorGroup},
					})
				if err != nil {
					return nil, err
				}
				break
			}
		}
		mergedEntities = s.mergedEntities
	}
	return &pb.PropertyValuesResponse{
		Data:      mergedEntities,
		NextToken: respToken,
	}, nil
}

// Process the next entity for out property value
//
// Out property values are not merged, only the preferred import group result
// is used.
func nextOut(ctx context.Context, s *outState, btGroup *bigtable.Group) (bool, error) {
	// Got enough entities, should stop.
	if len(s.mergedEntities) == s.limit {
		return false, nil
	}
	ig := s.used_import_group
	// Update the cursor.
	cursor := s.cursorGroup.Cursors[ig]
	s.mergedEntities = append(s.mergedEntities, s.rawEntities[ig][cursor.Item])
	cursor.Item++
	// Reach the end of the current page, should advance to next page.
	if int(cursor.Item) == len(s.rawEntities[ig]) {
		cursor.Page++
		cursor.Item = 0
		// No more pages
		if cursor.Page == int32(s.totalPage[ig]) {
			s.rawEntities[ig] = nil
			return false, nil
		} else {
			err := s.readNextPage(ctx, btGroup, true, ig, cursor.Page)
			if err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// Process the next entity for in property value
//
// As the data in each import group is sorted already. The duplication is
// handled by advancing the pointer in each import group simultaneously like
// in a merge sort.
func nextIn(ctx context.Context, s *inState, btGroup *bigtable.Group) (bool, error) {
	// All entities in all import groups have been exhausted.
	if s.heap.Len() == 0 {
		return false, nil
	}
	elem := heap.Pop(s.heap).(*heapElem)
	entity, ig := elem.data, elem.ig
	if len(s.mergedEntities) == 0 {
		s.mergedEntities = []*pb.EntityInfo{entity}
	} else {
		prev := s.mergedEntities[len(s.mergedEntities)-1]
		if entity.Dcid != prev.Dcid || entity.Value != prev.Value {
			// Find a new entity, add to the result.
			s.mergedEntities = append(s.mergedEntities, entity)
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
	if len(s.mergedEntities) == s.limit+1 {
		s.mergedEntities = s.mergedEntities[:s.limit]
		return false, nil
	}
	// Update the cursor.
	cursor := s.cursorGroup.Cursors[ig]
	cursor.Item++
	// Reach the end of the current page, should advance to next page.
	if int(cursor.Item) == len(s.rawEntities[ig]) {
		cursor.Page++
		cursor.Item = 0
		// No more pages
		if cursor.Page == int32(s.totalPage[ig]) {
			s.rawEntities[ig] = nil
		} else {
			err := s.readNextPage(ctx, btGroup, false, ig, cursor.Page)
			if err != nil {
				return false, err
			}
		}
	}
	// If there is data available in the current import group, push to the heap.
	if s.rawEntities[ig] != nil {
		elem := &heapElem{
			ig:   ig,
			pos:  cursor.GetItem(),
			data: s.rawEntities[ig][cursor.GetItem()],
		}
		heap.Push(s.heap, elem)
	}
	return true, nil
}
