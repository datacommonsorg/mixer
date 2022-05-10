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
	"github.com/datacommonsorg/mixer/internal/server/v1/page"
	"github.com/datacommonsorg/mixer/internal/server/v1/properties"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/server/v1/variable"
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

// InPropertyValues implements API for mixer.InPropertyValues.
func (s *Server) InPropertyValues(
	ctx context.Context, in *pb.InPropertyValuesRequest,
) (*pb.InPropertyValuesResponse, error) {
	return propertyvalues.InPropertyValues(ctx, in, s.store)
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

// BulkObservationsPoint implements API for mixer.BulkObservationsPoint.
func (s *Server) BulkObservationsPoint(
	ctx context.Context, in *pb.BulkObservationsPointRequest,
) (*pb.BulkObservationsPointResponse, error) {
	return observations.BulkPoint(ctx, in, s.store)
}

// BulkObservationsPointLinked implements API for mixer.BulkObservationsPointLinked.
func (s *Server) BulkObservationsPointLinked(
	ctx context.Context, in *pb.BulkObservationsPointLinkedRequest,
) (*pb.BulkObservationsPointResponse, error) {
	return observations.BulkPointLinked(ctx, in, s.store)
}

// ObservationsSeries implements API for mixer.ObservationsSeries.
func (s *Server) ObservationsSeries(
	ctx context.Context, in *pb.ObservationsSeriesRequest,
) (*pb.ObservationsSeriesResponse, error) {
	return observations.Series(ctx, in, s.store)
}

// BulkObservationsSeries implements API for mixer.BulkObservationsSeries.
func (s *Server) BulkObservationsSeries(
	ctx context.Context, in *pb.BulkObservationsSeriesRequest,
) (*pb.BulkObservationsSeriesResponse, error) {
	return observations.BulkSeries(ctx, in, s.store)
}

// BulkObservationsSeriesLinked implements API for mixer.BulkObservationsSeriesLinked.
func (s *Server) BulkObservationsSeriesLinked(
	ctx context.Context, in *pb.BulkObservationsSeriesLinkedRequest,
) (*pb.BulkObservationsSeriesResponse, error) {
	return observations.BulkSeriesLinked(ctx, in, s.store)
}

// ProteinPage implements API for mixer.ProteinPage.
func (s *Server) ProteinPage(
	ctx context.Context, in *pb.ProteinPageRequest,
) (*pb.GraphNodes, error) {
	return page.ProteinPage(ctx, in, s.store)
}

// PlacePage implements API for mixer.PlacePage.
func (s *Server) PlacePage(
	ctx context.Context, in *pb.PlacePageRequest,
) (*pb.GetPlacePageDataResponse, error) {
	return page.PlacePage(ctx, in, s.store)
}

// VariableAncestors implements API for Mixer.VariableAncestors.
func (s *Server) VariableAncestors(
	ctx context.Context, in *pb.VariableAncestorsRequest,
) (*pb.VariableAncestorsResponse, error) {
	return variable.Ancestors(ctx, in, s.store, s.cache)
}
