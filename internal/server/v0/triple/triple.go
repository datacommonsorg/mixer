// Copyright 2020 Google LLC
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

package triple

import (
	"context"
	"sort"
	"strings"

	"github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/server/v1/triples"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetTriples implements API for Mixer.GetTriples.
func GetTriples(
	ctx context.Context,
	in *pb.GetTriplesRequest,
	store *store.Store,
	metadata *resource.Metadata,
) (*pb.GetTriplesResponse, error) {
	dcids := in.GetDcids()
	limit := int(in.GetLimit())

	if len(dcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing argument: dcids")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCIDs")
	}

	// Need to fetch additional information for observation node.
	var regDcids, obsDcids []string
	for _, dcid := range dcids {
		if strings.HasPrefix(dcid, "dc/o/") {
			obsDcids = append(obsDcids, dcid)
		} else {
			regDcids = append(regDcids, dcid)
		}
	}

	result := &pb.GetTriplesResponse{Triples: make(map[string]*pb.Triples)}
	var err error
	// Regular DCIDs.
	if len(regDcids) > 0 {
		result, err = ReadTriples(ctx, store, metadata, regDcids)
		if err != nil {
			return nil, err
		}
		for dcid, triples := range result.Triples {
			applyLimit(dcid, triples, limit)
		}
	}
	// Observation DCIDs.
	if len(obsDcids) > 0 {
		obsResult, err := node.GetObsTriples(ctx, store, metadata, obsDcids)
		if err != nil {
			return nil, err
		}
		for k, v := range obsResult {
			if result.Triples[k] == nil {
				result.Triples[k] = &pb.Triples{}
			}
			result.Triples[k].Triples = append(result.Triples[k].Triples, v...)
		}
	}
	return result, nil
}

// ReadTriples read triples from base cache for multiple dcids.
func ReadTriples(
	ctx context.Context,
	store *store.Store,
	metadata *resource.Metadata,
	nodes []string,
) (*pb.GetTriplesResponse, error) {
	result := &pb.GetTriplesResponse{Triples: make(map[string]*pb.Triples)}
	for _, node := range nodes {
		result.Triples[node] = &pb.Triples{}
	}
	for _, direction := range []string{util.DirectionOut, util.DirectionIn} {
		v1Resp, err := triples.BulkTriples(
			ctx,
			&pb.BulkTriplesRequest{
				Nodes:     nodes,
				Direction: direction,
			},
			store,
			metadata,
		)
		if err != nil {
			return nil, err
		}
		for _, item := range v1Resp.GetData() {
			if direction == util.DirectionIn {
				result.Triples[item.GetNode()].InNodes = convert(item.GetTriples())
			} else {
				result.Triples[item.GetNode()].OutNodes = convert(item.GetTriples())
			}
		}
	}
	return result, nil
}

func convert(data map[string]*pb.NodeInfoCollection) map[string]*pb.EntityInfoCollection {
	result := map[string]*pb.EntityInfoCollection{}
	for key, nodeCollection := range data {
		entityCollection := &pb.EntityInfoCollection{
			Entities: nodeCollection.Nodes,
		}
		result[key] = entityCollection
	}
	return result
}

// Filter triples in place.
func applyLimit(dcid string, t *pb.Triples, limit int) {
	if t == nil {
		return
	}
	// Default limit value means no further limit.
	if limit == 0 {
		return
	}
	if t.Triples != nil {
		// This is the old triples cache.
		// Key is {isOut + predicate + neighborType}.
		existTriple := map[string][]*pb.Triple{}
		for _, t := range t.Triples {
			isOut := "0"
			neighborTypes := t.SubjectTypes
			if t.SubjectId == dcid {
				isOut = "1"
				neighborTypes = t.ObjectTypes
			}
			var nt string
			if len(neighborTypes) == 0 {
				nt = ""
			} else {
				nt = neighborTypes[0]
			}
			key := isOut + t.Predicate + nt
			if _, ok := existTriple[key]; !ok {
				existTriple[key] = []*pb.Triple{}
			}
			existTriple[key] = append(existTriple[key], t)
		}

		filtered := []*pb.Triple{}
		keys := []string{}
		for key := range existTriple {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for key := range existTriple {
			count := 0
			triples := existTriple[key]
			sort.SliceStable(triples, func(i, j int) bool {
				if triples[i].Predicate != triples[j].Predicate {
					return triples[i].Predicate < triples[j].Predicate
				}
				if triples[i].SubjectId != triples[j].SubjectId {
					return triples[i].SubjectId < triples[j].SubjectId
				}
				if triples[i].ObjectId != triples[j].ObjectId {
					return triples[i].ObjectId < triples[j].ObjectId
				}
				return triples[i].ObjectValue < triples[j].ObjectValue
			})
			for _, t := range triples {
				filtered = append(filtered, t)
				count++
				if count == limit {
					break
				}
			}
		}
		t.Triples = filtered
	} else {
		// This is the import group mdoe.
		//
		// Apply the filtering
		for _, target := range []map[string]*pb.EntityInfoCollection{
			t.OutNodes,
			t.InNodes,
		} {
			for _, c := range target {
				if len(c.Entities) <= limit {
					continue
				}
				tmp := map[string][]*pb.EntityInfo{}
				var nt string
				for _, e := range c.Entities {
					if e.Types != nil {
						// Entity is a node and has type.
						nt = e.Types[0]
					} else {
						// Entity is a string with no type. Use a dummy type as key.
						nt = "_"
					}
					if len(tmp[nt]) < limit {
						tmp[nt] = append(tmp[nt], e)
					}
				}
				keys := []string{}
				for k := range tmp {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				c.Entities = []*pb.EntityInfo{}
				for _, key := range keys {
					sort.SliceStable(tmp[key], func(i, j int) bool {
						if tmp[key][i].Dcid != tmp[key][j].Dcid {
							return tmp[key][i].Dcid < tmp[key][j].Dcid
						}
						return tmp[key][i].Value < tmp[key][j].Value
					})
					c.Entities = append(c.Entities, tmp[key]...)
				}
			}
		}
	}
}
