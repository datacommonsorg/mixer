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
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/recon"
	"github.com/datacommonsorg/mixer/internal/server/translator"
	"github.com/datacommonsorg/mixer/internal/server/v1/event"
	"github.com/datacommonsorg/mixer/internal/server/v1/info"
	"github.com/datacommonsorg/mixer/internal/server/v1/observationdates"
	"github.com/datacommonsorg/mixer/internal/server/v1/observationexistence"
	"github.com/datacommonsorg/mixer/internal/server/v1/observations"
	"github.com/datacommonsorg/mixer/internal/server/v1/page"
	"github.com/datacommonsorg/mixer/internal/server/v1/properties"
	"github.com/datacommonsorg/mixer/internal/server/v1/propertyvalues"
	"github.com/datacommonsorg/mixer/internal/server/v1/triples"
	"github.com/datacommonsorg/mixer/internal/server/v1/variable"
	"github.com/datacommonsorg/mixer/internal/server/v1/variables"
)

// QueryV1 implements API for Mixer.Query.
func (s *Server) QueryV1(
	ctx context.Context, in *pb.QueryRequest,
) (*pb.QueryResponse, error) {
	return translator.Query(ctx, in, s.metadata, s.store)
}

// Properties implements API for mixer.Properties.
func (s *Server) Properties(
	ctx context.Context, in *pbv1.PropertiesRequest,
) (*pbv1.PropertiesResponse, error) {
	return properties.Properties(ctx, in, s.store)
}

// BulkProperties implements API for mixer.BulkProperties.
func (s *Server) BulkProperties(
	ctx context.Context, in *pbv1.BulkPropertiesRequest,
) (*pbv1.BulkPropertiesResponse, error) {
	return properties.BulkProperties(ctx, in, s.store)
}

// PropertyValues implements API for mixer.PropertyValues.
func (s *Server) PropertyValues(
	ctx context.Context, in *pbv1.PropertyValuesRequest,
) (*pbv1.PropertyValuesResponse, error) {
	return propertyvalues.PropertyValues(ctx, in, s.store)
}

// LinkedPropertyValues implements API for mixer.LinkedPropertyValues.
func (s *Server) LinkedPropertyValues(
	ctx context.Context, in *pbv1.LinkedPropertyValuesRequest,
) (*pbv1.PropertyValuesResponse, error) {
	return propertyvalues.LinkedPropertyValues(ctx, in, s.store)
}

// BulkLinkedPropertyValues implements API for mixer.BulkLinkedPropertyValues.
func (s *Server) BulkLinkedPropertyValues(
	ctx context.Context, in *pbv1.BulkLinkedPropertyValuesRequest,
) (*pbv1.BulkPropertyValuesResponse, error) {
	return propertyvalues.BulkLinkedPropertyValues(ctx, in, s.store)
}

// BulkPropertyValues implements API for mixer.BulkPropertyValues.
func (s *Server) BulkPropertyValues(
	ctx context.Context, in *pbv1.BulkPropertyValuesRequest,
) (*pbv1.BulkPropertyValuesResponse, error) {
	return propertyvalues.BulkPropertyValues(ctx, in, s.store)
}

// Triples implements API for mixer.Triples.
func (s *Server) Triples(
	ctx context.Context, in *pbv1.TriplesRequest,
) (*pbv1.TriplesResponse, error) {
	return triples.Triples(ctx, in, s.store, s.metadata)
}

// BulkTriples implements API for mixer.BulkTriples.
func (s *Server) BulkTriples(
	ctx context.Context, in *pbv1.BulkTriplesRequest,
) (*pbv1.BulkTriplesResponse, error) {
	return triples.BulkTriples(ctx, in, s.store, s.metadata)
}

// Variables implements API for mixer.Variables.
func (s *Server) Variables(
	ctx context.Context, in *pbv1.VariablesRequest,
) (*pbv1.VariablesResponse, error) {
	return variables.Variables(ctx, in, s.store)
}

// BulkVariables implements API for mixer.BulkVariables.
func (s *Server) BulkVariables(
	ctx context.Context, in *pbv1.BulkVariablesRequest,
) (*pbv1.BulkVariablesResponse, error) {
	return variables.BulkVariables(ctx, in, s.store)
}

// PlaceInfo implements API for mixer.PlaceInfo.
func (s *Server) PlaceInfo(
	ctx context.Context, in *pbv1.PlaceInfoRequest,
) (*pbv1.PlaceInfoResponse, error) {
	return info.PlaceInfo(ctx, in, s.store)
}

// BulkPlaceInfo implements API for mixer.BulkPlaceInfo.
func (s *Server) BulkPlaceInfo(
	ctx context.Context, in *pbv1.BulkPlaceInfoRequest,
) (*pbv1.BulkPlaceInfoResponse, error) {
	return info.BulkPlaceInfo(ctx, in, s.store)
}

// VariableInfo implements API for mixer.VariableInfo.
func (s *Server) VariableInfo(
	ctx context.Context, in *pbv1.VariableInfoRequest,
) (*pbv1.VariableInfoResponse, error) {
	return info.VariableInfo(ctx, in, s.store)
}

// BulkVariableInfo implements API for mixer.BulkVariableInfo.
func (s *Server) BulkVariableInfo(
	ctx context.Context, in *pbv1.BulkVariableInfoRequest,
) (*pbv1.BulkVariableInfoResponse, error) {
	return info.BulkVariableInfo(ctx, in, s.store)
}

// VariableGroupInfo implements API for mixer.VariableGroupInfo.
func (s *Server) VariableGroupInfo(
	ctx context.Context, in *pbv1.VariableGroupInfoRequest,
) (*pbv1.VariableGroupInfoResponse, error) {
	return info.VariableGroupInfo(ctx, in, s.store, s.cache)
}

// BulkVariableGroupInfo implements API for mixer.BulkVariableGroupInfo.
func (s *Server) BulkVariableGroupInfo(
	ctx context.Context, in *pbv1.BulkVariableGroupInfoRequest,
) (*pbv1.BulkVariableGroupInfoResponse, error) {
	return info.BulkVariableGroupInfo(ctx, in, s.store, s.cache)
}

// ObservationsPoint implements API for mixer.ObservationsPoint.
func (s *Server) ObservationsPoint(
	ctx context.Context, in *pbv1.ObservationsPointRequest,
) (*pb.PointStat, error) {
	return observations.Point(ctx, in, s.store)
}

// BulkObservationsPoint implements API for mixer.BulkObservationsPoint.
func (s *Server) BulkObservationsPoint(
	ctx context.Context, in *pbv1.BulkObservationsPointRequest,
) (*pbv1.BulkObservationsPointResponse, error) {
	return observations.BulkPoint(ctx, in, s.store)
}

// BulkObservationsPointLinked implements API for mixer.BulkObservationsPointLinked.
func (s *Server) BulkObservationsPointLinked(
	ctx context.Context, in *pbv1.BulkObservationsPointLinkedRequest,
) (*pbv1.BulkObservationsPointResponse, error) {
	return observations.BulkPointLinked(ctx, in, s.store)
}

// ObservationsSeries implements API for mixer.ObservationsSeries.
func (s *Server) ObservationsSeries(
	ctx context.Context, in *pbv1.ObservationsSeriesRequest,
) (*pbv1.ObservationsSeriesResponse, error) {
	return observations.Series(ctx, in, s.store)
}

// BulkObservationsSeries implements API for mixer.BulkObservationsSeries.
func (s *Server) BulkObservationsSeries(
	ctx context.Context, in *pbv1.BulkObservationsSeriesRequest,
) (*pbv1.BulkObservationsSeriesResponse, error) {
	return observations.BulkSeries(ctx, in, s.store)
}

// BulkObservationsSeriesLinked implements API for mixer.BulkObservationsSeriesLinked.
func (s *Server) BulkObservationsSeriesLinked(
	ctx context.Context, in *pbv1.BulkObservationsSeriesLinkedRequest,
) (*pbv1.BulkObservationsSeriesResponse, error) {
	return observations.BulkSeriesLinked(ctx, in, s.store)
}

// BulkObservationDatesLinked implements API for mixer.BulkObservationDatesLinked.
func (s *Server) BulkObservationDatesLinked(
	ctx context.Context, in *pbv1.BulkObservationDatesLinkedRequest,
) (*pbv1.BulkObservationDatesLinkedResponse, error) {
	return observationdates.BulkObservationDatesLinked(ctx, in, s.store)
}

// BulkObservationExistence implements API for mixer.BulkObservationExistence.
func (s *Server) BulkObservationExistence(
	ctx context.Context, in *pbv1.BulkObservationExistenceRequest,
) (*pbv1.BulkObservationExistenceResponse, error) {
	return observationexistence.BulkObservationExistence(ctx, in, s.store)
}

// BioPage implements API for mixer.BioPage.
func (s *Server) BioPage(
	ctx context.Context, in *pbv1.BioPageRequest,
) (*pb.GraphNodes, error) {
	return page.BioPage(ctx, in, s.store)
}

// PlacePage implements API for mixer.PlacePage.
func (s *Server) PlacePage(
	ctx context.Context, in *pbv1.PlacePageRequest,
) (*pbv1.PlacePageResponse, error) {
	return page.PlacePage(ctx, in, s.store)
}

// VariableAncestors implements API for Mixer.VariableAncestors.
func (s *Server) VariableAncestors(
	ctx context.Context, in *pbv1.VariableAncestorsRequest,
) (*pbv1.VariableAncestorsResponse, error) {
	return variable.Ancestors(ctx, in, s.store, s.cache)
}

// DerivedObservationsSeries implements API for mixer.ObservationsSeries.
func (s *Server) DerivedObservationsSeries(
	ctx context.Context, in *pbv1.DerivedObservationsSeriesRequest,
) (*pbv1.DerivedObservationsSeriesResponse, error) {
	return observations.DerivedSeries(ctx, in, s.store)
}

// EventCollection implements API for mixer.EventCollection.
func (s *Server) EventCollection(
	ctx context.Context, in *pbv1.EventCollectionRequest,
) (*pbv1.EventCollectionResponse, error) {
	return event.Collection(ctx, in, s.store)
}

// EventCollectionDate implements API for mixer.EventCollectionDate.
func (s *Server) EventCollectionDate(
	ctx context.Context, in *pbv1.EventCollectionDateRequest,
) (*pbv1.EventCollectionDateResponse, error) {
	return event.CollectionDate(ctx, in, s.store)
}

// RecognizePlaces implements API for Mixer.RecognizePlaces.
func (s *Server) RecognizePlaces(
	ctx context.Context, in *pb.RecognizePlacesRequest,
) (*pb.RecognizePlacesResponse, error) {
	return recon.RecognizePlaces(ctx, in, s.store)
}
