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

package recon

import (
	"context"
	"fmt"
	"sort"

	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/protobuf/proto"
)

// ResolveIds resolve entities based on IDs.
func ResolveIds(
	ctx context.Context, in *pb.ResolveIdsRequest, store *store.Store,
) (*pb.ResolveIdsResponse, error) {
	inProp := in.GetInProp()
	outProp := in.GetOutProp()
	ids := in.GetIds()
	if inProp == "" || outProp == "" || len(ids) == 0 {
		return nil, fmt.Errorf(
			"invalid input: in_prop: %s, out_prop: %s, ids: %v", inProp, outProp, ids)
	}

	// Read cache data.
	btDataList, err := bigtable.Read(
		ctx, store.BtGroup, bigtable.BtReconIDMapPrefix,
		[][]string{{inProp}, ids, {outProp}},
		func(jsonRaw []byte) (interface{}, error) {
			var reconEntities pb.ReconEntities
			if err := proto.Unmarshal(jsonRaw, &reconEntities); err != nil {
				return nil, err
			}
			return &reconEntities, nil
		},
	)
	if err != nil {
		return nil, err
	}

	// Assemble result.
	// TODO: merge ids from different tables. Now it only picks up IDs from
	// one preferred table.
	res := &pb.ResolveIdsResponse{}
	existData := map[string]bool{}
	for _, btData := range btDataList {
		for _, row := range btData {
			inID := row.Parts[1]
			if exist, ok := existData[inID]; ok && exist {
				continue
			}
			reconEntitiesPb, ok := row.Data.(*pb.ReconEntities)
			if !ok {
				continue
			}
			entity := &pb.ResolveIdsResponse_Entity{InId: inID}
			for _, reconEntity := range reconEntitiesPb.GetEntities() {
				if len(reconEntity.GetIds()) != 1 {
					return nil, fmt.Errorf("wrong cache result for %s: %v", inID, row.Data)
				}
				entity.OutIds = append(entity.OutIds, reconEntity.GetIds()[0].GetVal())
			}
			existData[inID] = true
			// Sort to make the result deterministic.
			sort.Strings(entity.OutIds)
			res.Entities = append(res.Entities, entity)
		}
	}
	// Sort to make the result deterministic.
	sort.Slice(res.Entities, func(i, j int) bool {
		return res.Entities[i].GetInId() > res.Entities[j].GetInId()
	})
	return res, nil
}
