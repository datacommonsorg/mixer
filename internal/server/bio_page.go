// Copyright 2021 Google LLC
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

package server

import (
	"context"
	"encoding/json"

	cbt "cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetBioPageData implements API for Mixer.GetBioPageData.
func (s *Server) GetBioPageData(
	ctx context.Context, in *pb.GetBioPageDataRequest) (
	*pb.GraphNodes, error) {

	dcid := in.GetDcid()
	if dcid == "" {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required arguments: dcid")
	}

	data, _, err := bigtable.Read(
		ctx,
		s.store.BtGroup,
		cbt.RowList{bigtable.BtProteinPagePrefix + dcid},
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var graph pb.GraphNodes
			err := json.Unmarshal(jsonRaw, &graph)
			if err != nil {
				return nil, err
			}
			return &graph, nil
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	if _, ok := data[dcid]; !ok {
		return nil, nil
	}
	return data[dcid].(*pb.GraphNodes), nil
}
