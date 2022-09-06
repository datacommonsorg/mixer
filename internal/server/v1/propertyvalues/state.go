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
	nodes      []string
	properties []string
	limit      int
	// CursorGroup that tracks the state of current data.
	// Key: node, property, type
	cursorGroup map[string]map[string]map[string][]*pb.Cursor
	// Raw nodes read from BigTable
	// Key: node, property, type
	rawNodes map[string]map[string]map[string][][]*pb.EntityInfo
	// Merged nodes
	// Key: node, property, type
	mergedNodes map[string]map[string]map[string][]*pb.EntityInfo
	// Total page count for each import group
	// Key: node, property, type, import group index
	totalPage map[string]map[string]map[string]map[int]int
	// Record the import group for next item to read
	// Key: node, property, type
	next map[string]map[string]map[string]*pb.Cursor
}

type inState struct {
	state
	// Min heap for node merge sort
	// Key: node, property, type
	heap map[string]map[string]map[string]*nodeHeap
}

type outState struct {
	state
	// The used import group (only one import group is used for out property values).
	// Key: node, property, type
	usedImportGroup map[string]map[string]map[string]int
}

// init builds a new state given node, property and cursor group.
func (s *state) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	nodes []string,
	properties []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
	arcOut bool,
) error {
	// Constructor
	s.nodes = nodes
	s.properties = properties
	s.cursorGroup = cursorGroup
	s.rawNodes = map[string]map[string]map[string][][]*pb.EntityInfo{}
	s.mergedNodes = map[string]map[string]map[string][]*pb.EntityInfo{}
	s.limit = limit
	s.totalPage = map[string]map[string]map[string]map[int]int{}
	s.next = map[string]map[string]map[string]*pb.Cursor{}
	for _, n := range nodes {
		s.next[n] = map[string]map[string]*pb.Cursor{}
		s.mergedNodes[n] = map[string]map[string][]*pb.EntityInfo{}
		for _, p := range properties {
			s.next[n][p] = map[string]*pb.Cursor{}
			s.mergedNodes[n][p] = map[string][]*pb.EntityInfo{}
		}
	}
	accs := []*bigtable.Accessor{}
	for n := range cursorGroup {
		for p := range cursorGroup[n] {
			for t := range cursorGroup[n][p] {
				for _, c := range cursorGroup[n][p][t] {
					if c != nil {
						accs = append(accs, &bigtable.Accessor{
							ImportGroup: int(c.GetImportGroup()),
							Body: [][]string{
								{n},
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
	numImportGroup := len(btDataList)
	for idx, btData := range btDataList {
		if len(btData) == 0 {
			continue
		}
		for _, row := range btData {
			n := row.Parts[0] // node
			p := row.Parts[1] // property
			t := row.Parts[2] // type
			values := row.Data.(*pb.PagedNodes)
			if _, ok := s.rawNodes[n]; !ok {
				s.rawNodes[n] = map[string]map[string][][]*pb.EntityInfo{}
			}
			if _, ok := s.rawNodes[n][p]; !ok {
				s.rawNodes[n][p] = make(map[string][][]*pb.EntityInfo, numImportGroup)
			}
			if _, ok := s.rawNodes[n][p][t]; !ok {
				s.rawNodes[n][p][t] = make([][]*pb.EntityInfo, numImportGroup)
			}
			s.rawNodes[n][p][t][idx] = values.Nodes
			if _, ok := s.totalPage[n]; !ok {
				s.totalPage[n] = map[string]map[string]map[int]int{}
			}
			if _, ok := s.totalPage[n][p]; !ok {
				s.totalPage[n][p] = map[string]map[int]int{}
			}
			if _, ok := s.totalPage[n][p][t]; !ok {
				s.totalPage[n][p][t] = map[int]int{}
			}
			s.totalPage[n][p][t][idx] = int(values.TotalPageCount)
		}
	}
	return nil
}

// init the state for in property values API
func (s *inState) init(
	ctx context.Context,
	btGroup *bigtable.Group,
	nodes []string,
	properties []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, nodes, properties, limit, cursorGroup, false)
	if err != nil {
		return err
	}
	s.heap = map[string]map[string]map[string]*nodeHeap{}
	for _, n := range nodes {
		s.heap[n] = map[string]map[string]*nodeHeap{}
		for _, p := range properties {
			s.heap[n][p] = map[string]*nodeHeap{}
			// Push the next node of each import group to the heap.
			for t, typedNodeList := range s.rawNodes[n][p] {
				s.heap[n][p][t] = &nodeHeap{}
				// Init the min heap
				heap.Init(s.heap[n][p][t])
				for idx, nodeList := range typedNodeList {
					cursor := cursorGroup[n][p][t][idx]
					if int(cursor.GetItem()) < len(nodeList) {
						elem := &heapElem{
							ig:   idx,
							pos:  cursor.GetItem(),
							data: nodeList[cursor.GetItem()],
						}
						heap.Push(s.heap[n][p][t], elem)
						s.next[n][p][t] = cursor
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
	nodes []string,
	properties []string,
	limit int,
	cursorGroup map[string]map[string]map[string][]*pb.Cursor,
) error {
	err := s.state.init(ctx, btGroup, nodes, properties, limit, cursorGroup, true)
	if err != nil {
		return err
	}
	s.usedImportGroup = map[string]map[string]map[string]int{}
	for n := range s.rawNodes {
		s.usedImportGroup[n] = map[string]map[string]int{}
		for p := range s.rawNodes[n] {
			s.usedImportGroup[n][p] = map[string]int{}
			for t := range s.rawNodes[n][p] {
				for idx, data := range s.rawNodes[n][p][t] {
					if len(data) > 0 {
						s.usedImportGroup[n][p][t] = idx
						s.next[n][p][t] = cursorGroup[n][p][t][idx]
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
	for n := range s.cursorGroup {
		for p := range s.cursorGroup[n] {
			for t := range s.cursorGroup[n][p] {
				if s.cursorGroup[n][p][t] != nil {
					cursorGroups = append(
						cursorGroups,
						&pb.CursorGroup{
							Keys:    []string{n, p, t},
							Cursors: s.cursorGroup[n][p][t],
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
