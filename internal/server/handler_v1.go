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

package server

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/v1/info"
	"github.com/datacommonsorg/mixer/internal/server/v1/observations"
	"github.com/datacommonsorg/mixer/internal/server/v1/properties"
	"github.com/datacommonsorg/mixer/internal/server/v1/variables"
)

// Properties implements API for mixer.Properties.
func (s *Server) Properties(
	ctx context.Context, in *pb.PropertiesRequest,
) (*pb.PropertiesResponse, error) {
	return properties.Properties(ctx, in, s.store)
}

// BulkProperties implements API for mixer.BulkProperties.
func (s *Server) BulkProperties(
	ctx context.Context, in *pb.BulkPropertiesRequest,
) (*pb.BulkPropertiesResponse, error) {
	return properties.BulkProperties(ctx, in, s.store)
}

// Variables implements API for mixer.Variables.
func (s *Server) Variables(
	ctx context.Context, in *pb.VariablesRequest,
) (*pb.VariablesResponse, error) {
	return variables.Variables(ctx, in, s.store)
}

// BulkVariables implements API for mixer.BulkVariables.
func (s *Server) BulkVariables(
	ctx context.Context, in *pb.BulkVariablesRequest,
) (*pb.BulkVariablesResponse, error) {
	return variables.BulkVariables(ctx, in, s.store)
}

// PlaceInfo implements API for mixer.PlaceInfo.
func (s *Server) PlaceInfo(
	ctx context.Context, in *pb.PlaceInfoRequest,
) (*pb.PlaceInfoResponse, error) {
	return info.PlaceInfo(ctx, in, s.store)
}

// BulkPlaceInfo implements API for mixer.BulkPlaceInfo.
func (s *Server) BulkPlaceInfo(
	ctx context.Context, in *pb.BulkPlaceInfoRequest,
) (*pb.BulkPlaceInfoResponse, error) {
	return info.BulkPlaceInfo(ctx, in, s.store)
}

// VariableInfo implements API for mixer.VariableInfo.
func (s *Server) VariableInfo(
	ctx context.Context, in *pb.VariableInfoRequest,
) (*pb.VariableInfoResponse, error) {
	return info.VariableInfo(ctx, in, s.store)
}

// BulkVariableInfo implements API for mixer.BulkVariableInfo.
func (s *Server) BulkVariableInfo(
	ctx context.Context, in *pb.BulkVariableInfoRequest,
) (*pb.BulkVariableInfoResponse, error) {
	return info.BulkVariableInfo(ctx, in, s.store)
}

// ObservationsPoint implements API for mixer.ObservationsPoint.
func (s *Server) ObservationsPoint(
	ctx context.Context, in *pb.ObservationsPointRequest,
) (*pb.PointStat, error) {
	return observations.Point(ctx, in, s.store)
}
