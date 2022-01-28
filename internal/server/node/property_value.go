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

package node

import (
	"context"
	"encoding/json"

	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// GetPropertyValues implements API for Mixer.GetPropertyValues.
func GetPropertyValues(
	ctx context.Context,
	in *pb.GetPropertyValuesRequest,
	store *store.Store,
) (*pb.GetPropertyValuesResponse, error) {
	dcids := in.GetDcids()
	prop := in.GetProperty()
	typ := in.GetValueType()
	direction := in.GetDirection()
	limit := int(in.GetLimit())

	// Check arguments
	if prop == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing required argument: property")
	}
	if len(dcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "missing required arguments: dcids")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid DCIDs %s", dcids)
	}

	// Get in, out or both direction
	var (
		inArc  = true
		outArc = true
		inRes  = map[string][]*model.Node{}
		outRes = map[string][]*model.Node{}
	)
	var err error
	if direction == "in" {
		outArc = false
	} else if direction == "out" {
		inArc = false
	}

	if inArc {
		inRes, err = GetPropertyValuesHelper(ctx, store, dcids, prop, false)
		if err != nil {
			return nil, err
		}
	}
	if outArc {
		outRes, err = GetPropertyValuesHelper(ctx, store, dcids, prop, true)
		if err != nil {
			return nil, err
		}
	}

	result := make(map[string]map[string][]*model.Node)
	for _, dcid := range dcids {
		result[dcid] = map[string][]*model.Node{}
	}
	for dcid, nodes := range inRes {
		trimedNodes := trimNodes(nodes, typ, limit)
		if len(trimedNodes) > 0 {
			result[dcid]["in"] = trimedNodes

		}
	}
	for dcid, nodes := range outRes {
		trimedNodes := trimNodes(nodes, typ, limit)
		if len(trimedNodes) > 0 {
			result[dcid]["out"] = trimedNodes
		}
	}

	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetPropertyValuesResponse{Payload: string(jsonRaw)}, nil
}

// GetPropertyValuesHelper get property values.
func GetPropertyValuesHelper(
	ctx context.Context,
	store *store.Store,
	dcids []string,
	prop string,
	arcOut bool,
) (map[string][]*model.Node, error) {
	rowList := bigtable.BuildPropertyValuesKey(dcids, prop, arcOut)
	baseDataList, _, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var propVals model.PropValueCache
			err := json.Unmarshal(jsonRaw, &propVals)
			if err != nil {
				return nil, err
			}
			return propVals.Nodes, nil
		},
		nil,
		true, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := map[string][]*model.Node{}
	visited := map[string]map[string]struct{}{}
	// Loop the import groups. They are ordered by the preferences. So only add
	// a node if it is not seen yet.
	for _, baseData := range baseDataList {
		for dcid, data := range baseData {
			if _, ok := result[dcid]; !ok {
				result[dcid] = []*model.Node{}
			}
			if _, ok := visited[dcid]; !ok {
				visited[dcid] = map[string]struct{}{}
			}
			if data != nil {
				nodes := data.([]*model.Node)
				for _, n := range nodes {
					// Check if a duplicate node has been added to the result.
					// Duplication is based on either the DCID or the value.
					if n.Dcid != "" {
						if _, ok := visited[dcid][n.Dcid]; !ok {
							result[dcid] = append(result[dcid], n)
							visited[dcid][n.Dcid] = struct{}{}
						}
					} else if n.Value != "" {
						if _, ok := visited[dcid][n.Value]; !ok {
							result[dcid] = append(result[dcid], n)
							visited[dcid][n.Value] = struct{}{}
						}
					}
				}
			}
		}
	}
	return result, nil
}

func trimNodes(nodes []*model.Node, typ string, limit int) []*model.Node {
	if limit == 0 && typ == "" {
		return nodes
	}
	result := []*model.Node{}
	for _, node := range nodes {
		if typ == "" {
			result = append(result, node)
		} else {
			for _, t := range node.Types {
				if t == typ {
					result = append(result, node)
					break
				}
			}
		}
		if limit > 0 && len(result) == limit {
			break
		}
	}
	return result
}
