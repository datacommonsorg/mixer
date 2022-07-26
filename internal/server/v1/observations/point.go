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

package observations

import (
	"context"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Point implements API for Mixer.ObservationsPoint.
func Point(
	ctx context.Context,
	in *pb.ObservationsPointRequest,
	store *store.Store,
) (*pb.PointStat, error) {
	entityVariable := in.GetEntityVariable()
	parts := strings.Split(entityVariable, "/")
	if len(parts) < 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid request URI")
	}
	variable := parts[len(parts)-1]
	entity := strings.Join(parts[0:len(parts)-1], "/")

	if entity == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: entity")
	}
	if variable == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: variable")
	}
	date := in.GetDate()

	btData, err := stat.ReadStatsPb(ctx, store.BtGroup, []string{entity}, []string{variable})
	if err != nil {
		return nil, err
	}
	stat, metadata := stat.GetValueFromBestSourcePb(btData[entity][variable], date)
	if stat == nil {
		return &pb.PointStat{}, nil
	}
	stat.Metadata = metadata
	return stat, nil
}
