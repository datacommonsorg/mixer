// Copyright 2026 Google LLC
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

// This file implements the methods for normalizedClient (Normalized Schema).
package spanner

import (
	"context"
	"fmt"
	"strconv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

// normalizedSchemaClient encapsulates the Spanner client for the normalized schema.
// It implements specialized queries optimized for the normalized schema.
//
// DESIGN NOTE: This client embeds the SpannerClient interface (initialized with the
// standard client) primarily to fulfill the full interface and to provide full
// internal-facing functionality (e.g., if a normalized method needs to call a
// standard method like GetNodeProps internally). It is NOT intended to be used
// by the Selector for general request fallbacks, which are handled explicitly
// by the Selector itself.
type normalizedSchemaClient struct {
	SpannerClient
	conn *SpannerConnector
}

// NewNormalizedClient creates a new normalizedSchemaClient.
func NewNormalizedClient(client SpannerClient) *normalizedSchemaClient {
	var conn *SpannerConnector
	if sc, ok := client.(*standardSpannerClient); ok {
		conn = sc.exec
	} else if sc, ok := client.(*selectorClient); ok {
		if std, ok := sc.SpannerClient.(*standardSpannerClient); ok {
			conn = std.exec
		}
	}
	if conn == nil {
		panic(fmt.Sprintf("NewNormalizedClient: unexpected client type %T", client))
	}
	return &normalizedSchemaClient{
		SpannerClient: client,
		conn:          conn,
	}
}

// Force compiler that all methods required by the interface are implemented by clients
var _ SpannerClient = (*normalizedSchemaClient)(nil)

// GetObservations retrieves observations from Spanner given a list of variables and entities
// using the normalized schema.
func (nc *normalizedSchemaClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	if len(entities) == 0 {
		return nil, fmt.Errorf("entity must be specified")
	}

	rawObs, err := nc.fetchRawObservations(ctx, variables, entities)
	if err != nil {
		return nil, err
	}

	if len(rawObs) == 0 {
		return []*Observation{}, nil
	}

	return reconstructObservations(rawObs), nil
}

// fetchRawObservations fetches data from TimeSeries and StatVarObservation tables.
func (nc *normalizedSchemaClient) fetchRawObservations(ctx context.Context, variables []string, entities []string) ([]*rawObservation, error) {
	stmt := GetNormalizedObservationsQuery(variables, entities)

	var rawObs []*rawObservation
	err := nc.conn.queryStructs(ctx, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	return rawObs, err
}

// reconstructObservations combines raw observations and attributes into full Observation structs.
func reconstructObservations(rawObs []*rawObservation) []*Observation {
	var result []*Observation

	for _, r := range rawObs {
		obs := &Observation{
			VariableMeasured: r.VariableMeasured,
			Observations:     TimeSeries{},
		}

		for _, dv := range r.DatesAndValues {
			obs.Observations = append(obs.Observations, &DateValue{Date: dv.Date, Value: dv.Value})
		}

		for _, attr := range r.Attributes {
			switch attr.Property {
			case "observationAbout":
				obs.ObservationAbout = attr.Value
			case "facetId":
				obs.FacetId = attr.Value
			case "importName":
				obs.ImportName = attr.Value
			case "provenanceUrl":
				obs.ProvenanceURL = attr.Value
			case "isDcAggregate":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					obs.IsDcAggregate = b
				}
			}
		}
		result = append(result, obs)
	}

	return result
}

// CheckVariableExistence checks which variables exist for which entities using the normalized schema.
func (nc *normalizedSchemaClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	stmt, err := GetNormalizedStatVarsByEntityQuery(variables, entities)
	if err != nil {
		return nil, err
	}
	return nc.conn.queryDynamic(ctx, *stmt)
}

// GetObservationsContainedInPlace retrieves observations for entities contained in a place
// using the normalized schema.
func (nc *normalizedSchemaClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	if containedInPlace == nil {
		return nil, fmt.Errorf("containedInPlace must be specified")
	}

	rawObs, err := nc.fetchRawObservationsContainedInPlace(ctx, variables, containedInPlace)
	if err != nil {
		return nil, err
	}

	if len(rawObs) == 0 {
		return []*Observation{}, nil
	}

	return reconstructObservations(rawObs), nil
}

// fetchRawObservationsContainedInPlace fetches data from Graph, TimeSeries and StatVarObservation tables.
func (nc *normalizedSchemaClient) fetchRawObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*rawObservation, error) {
	stmt := GetNormalizedObservationsContainedInPlaceQuery(variables, containedInPlace)

	var rawObs []*rawObservation
	err := nc.conn.queryStructs(ctx, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	return rawObs, err
}

var facetAttributes = map[string]bool{
	"unit":              true,
	"measurementMethod": true,
	"scalingFactor":     true,
	"observationPeriod": true,
	"importName":        true,
	"provenanceUrl":     true,
}

// GetSdmxObservations retrieves observations from Spanner given a list of constraints
// using the normalized schema and relational division.
func (nc *normalizedSchemaClient) GetSdmxObservations(ctx context.Context, req *pb.SdmxDataQuery) (*pb.SdmxDataResult, error) {
	stmt := GetSdmxObservationsQuery(req)

	var rawObs []*rawObservation
	err := nc.conn.queryStructs(ctx, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	if err != nil {
		return nil, err
	}

	if len(rawObs) == 0 {
		return &pb.SdmxDataResult{Observations: []*pb.SdmxObservation{}}, nil
	}

	return &pb.SdmxDataResult{Observations: reconstructSdmxObservations(rawObs)}, nil
}

// reconstructSdmxObservations combines raw observations and attributes into full SdmxObservation structs for SDMX.
func reconstructSdmxObservations(rawObs []*rawObservation) []*pb.SdmxObservation {
	var result []*pb.SdmxObservation

	for _, r := range rawObs {
		obs := &pb.SdmxObservation{
			VariableMeasured: r.VariableMeasured,
			Provenance:       r.Provenance,
			DatesAndValues:   []*pb.SdmxDateValue{},
			Dimensions:       map[string]string{},
			Attributes:       map[string]string{},
		}

		for _, dv := range r.DatesAndValues {
			obs.DatesAndValues = append(obs.DatesAndValues, &pb.SdmxDateValue{Date: dv.Date, Value: dv.Value})
		}

		for _, attr := range r.Attributes {
			if attr.Property == "provenance" {
				continue
			}
			if facetAttributes[attr.Property] {
				obs.Attributes[attr.Property] = attr.Value
			} else {
				obs.Dimensions[attr.Property] = attr.Value
			}
		}
		result = append(result, obs)
	}

	return result
}
