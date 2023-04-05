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

package statvarsummary

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/statvar"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetStatVarSummary implements API for Mixer.GetStatVarSummary.
func GetStatVarSummary(
	ctx context.Context, in *pb.GetStatVarSummaryRequest, store *store.Store) (
	*pb.GetStatVarSummaryResponse, error) {
	statVars := in.GetStatVars()
	if len(statVars) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: stat_vars")
	}
	if err := util.CheckValidDCIDs(statVars); err != nil {
		return nil, err
	}
	res, err := statvar.GetStatVarSummaryHelper(ctx, statVars, store)
	if err != nil {
		return nil, err
	}

	return &pb.GetStatVarSummaryResponse{StatVarSummary: res}, nil
}
