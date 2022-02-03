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

package statvar

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// GetStatVarSummary implements API for Mixer.GetStatVarSummary.
func GetStatVarSummary(
	ctx context.Context, in *pb.GetStatVarSummaryRequest, store *store.Store) (
	*pb.GetStatVarSummaryResponse, error) {
	sv := in.GetStatVars()
	rowList := bigtable.BuildStatVarSummaryKey(sv)
	baseDataList, _, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			var statVarSummary pb.StatVarSummary
			if isProto {
				if err := proto.Unmarshal(jsonRaw, &statVarSummary); err != nil {
					return nil, err
				}
			} else {
				if err := protojson.Unmarshal(jsonRaw, &statVarSummary); err != nil {
					return nil, err
				}
			}
			return &statVarSummary, nil
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := &pb.GetStatVarSummaryResponse{
		StatVarSummary: map[string]*pb.StatVarSummary{},
	}
	for dcid, data := range baseDataList[0] {
		result.StatVarSummary[dcid] = data.(*pb.StatVarSummary)
	}
	return result, nil
}
