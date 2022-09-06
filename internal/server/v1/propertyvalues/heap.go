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
	pb "github.com/datacommonsorg/mixer/internal/proto"
)

type heapElem struct {
	ig   int
	pos  int32
	typ  string
	data *pb.EntityInfo
}

// An nodeHeap is a min-heap of node.
type nodeHeap []*heapElem

func (h nodeHeap) Len() int { return len(h) }
func (h nodeHeap) Less(i, j int) bool {
	di := h[i].data
	dj := h[j].data
	// This needs to match the sorting in cache builder "triple_helper.cc"
	//
	// std::string EntityInfoSortKey(const EntityInfo& entity_info) {
	// return absl::StrJoin(
	//     {entity_info.dcid(), entity_info.value(), entity_info.provenance_id()},
	//      "!"});
	// }
	return di.Dcid+"!"+di.Value < dj.Dcid+"!"+dj.Value
}
func (h nodeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *nodeHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*heapElem))
}

func (h *nodeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
