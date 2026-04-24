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

package spanner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
)

type multiEntityObservationClient interface {
	GetMultiEntityObservations(
		ctx context.Context,
		variables []string,
		dimensions []*pbv2.ObservationDimensionConstraint,
	) ([]*multiEntityObservation, error)
}

var multiEntityDimensionExcludedProperties = map[string]struct{}{
	"facetId":           {},
	"importName":        {},
	"isDcAggregate":     {},
	"measurementMethod": {},
	"observationAbout":  {},
	"observationPeriod": {},
	"provenanceUrl":     {},
	"scalingFactor":     {},
	"unit":              {},
}

func (sds *SpannerDataSource) multiEntityObservation(
	ctx context.Context,
	req *pbv2.ObservationRequest,
) (*pbv2.ObservationResponse, error) {
	variables, dimensions, err := validateMultiEntityObservationRequest(req)
	if err != nil {
		return nil, err
	}

	client, ok := sds.client.(multiEntityObservationClient)
	if !ok {
		return nil, fmt.Errorf("multi-entity observations require normalized Spanner schema")
	}

	observations, err := client.GetMultiEntityObservations(ctx, variables, dimensions)
	if err != nil {
		return nil, err
	}

	resp := multiEntityObservationsToResponse(variables, observations)
	if len(req.GetNodeProperties()) == 0 {
		return resp, nil
	}
	if err := sds.hydrateMultiEntityObservationNodes(ctx, resp, req.GetNodeProperties()); err != nil {
		return nil, err
	}
	return resp, nil
}

func validateMultiEntityObservationRequest(
	req *pbv2.ObservationRequest,
) ([]string, []*pbv2.ObservationDimensionConstraint, error) {
	if hasDcidOrExpressionValue(req.GetEntity()) {
		return nil, nil, fmt.Errorf("only one of entity and observation_dimensions can be specified")
	}

	variable := req.GetVariable()
	if variable == nil || len(variable.GetDcids()) == 0 {
		return nil, nil, fmt.Errorf("variable.dcids must be specified for multi-entity observations")
	}
	if variable.GetExpression() != "" || variable.GetFormula() != "" {
		return nil, nil, fmt.Errorf("variable expressions and formulas are not supported for multi-entity observations")
	}
	for _, dcid := range variable.GetDcids() {
		if dcid == "" {
			return nil, nil, fmt.Errorf("variable.dcids cannot contain empty values")
		}
	}

	dimensions := req.GetObservationDimensions()
	if len(dimensions) == 0 {
		return nil, nil, fmt.Errorf("observation_dimensions must be specified")
	}
	seenProperties := map[string]struct{}{}
	for _, dimension := range dimensions {
		property := dimension.GetProperty()
		if property == "" {
			return nil, nil, fmt.Errorf("observation_dimensions property must be specified")
		}
		if _, ok := seenProperties[property]; ok {
			return nil, nil, fmt.Errorf("duplicate observation_dimensions property: %s", property)
		}
		seenProperties[property] = struct{}{}

		value := dimension.GetValue()
		if value == nil || len(value.GetDcids()) == 0 {
			return nil, nil, fmt.Errorf("observation_dimensions value.dcids must be specified")
		}
		if value.GetExpression() != "" || value.GetFormula() != "" {
			return nil, nil, fmt.Errorf("observation_dimensions expressions and formulas are not supported")
		}
		for _, dcid := range value.GetDcids() {
			if dcid == "" {
				return nil, nil, fmt.Errorf("observation_dimensions value.dcids cannot contain empty values")
			}
		}
	}

	for _, property := range req.GetNodeProperties() {
		if property == "" {
			return nil, nil, fmt.Errorf("node_properties cannot contain empty values")
		}
	}

	return variable.GetDcids(), dimensions, nil
}

func hasDcidOrExpressionValue(value *pbv2.DcidOrExpression) bool {
	return value != nil &&
		(len(value.GetDcids()) > 0 || value.GetExpression() != "" || value.GetFormula() != "")
}

func multiEntityObservationsToResponse(
	variables []string,
	observations []*multiEntityObservation,
) *pbv2.ObservationResponse {
	resp := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	for _, variable := range variables {
		resp.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
	}

	for _, observation := range observations {
		facetObservation := multiEntityFacetObservation(observation)
		if facetObservation == nil {
			continue
		}
		variableObservation, ok := resp.ByVariable[observation.VariableMeasured]
		if !ok {
			variableObservation = &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{},
			}
			resp.ByVariable[observation.VariableMeasured] = variableObservation
		}
		variableObservation.MultiEntityObservations = append(
			variableObservation.MultiEntityObservations,
			&pbv2.MultiEntityObservation{
				ObservationDimensions: multiEntityObservationDimensions(observation.Attributes),
				Observation: &pbv2.EntityObservation{
					OrderedFacets: []*pbv2.FacetObservation{facetObservation},
				},
			},
		)
		if observation.Provenance != "" {
			resp.Facets[observation.Provenance] = &pb.Facet{ImportName: observation.Provenance}
		}
	}

	return resp
}

func multiEntityFacetObservation(observation *multiEntityObservation) *pbv2.FacetObservation {
	pointStats := []*pb.PointStat{}
	for _, dateValue := range observation.Observations {
		pointStat, err := dateValueToPointStat(dateValue)
		if err != nil {
			continue
		}
		pointStats = append(pointStats, pointStat)
	}
	if len(pointStats) == 0 {
		return nil
	}
	sort.Slice(pointStats, func(i, j int) bool {
		return pointStats[i].GetDate() < pointStats[j].GetDate()
	})
	return &pbv2.FacetObservation{
		FacetId:      observation.Provenance,
		Observations: pointStats,
		ObsCount:     int32(len(pointStats)),
		EarliestDate: pointStats[0].GetDate(),
		LatestDate:   pointStats[len(pointStats)-1].GetDate(),
	}
}

func multiEntityObservationDimensions(attributes []*spannerAttribute) []*pbv2.ObservationDimension {
	dimensions := []*pbv2.ObservationDimension{}
	for _, attr := range attributes {
		if _, excluded := multiEntityDimensionExcludedProperties[attr.Property]; excluded {
			continue
		}
		dimensions = append(dimensions, &pbv2.ObservationDimension{
			Property: attr.Property,
			Dcid:     attr.Value,
		})
	}
	sort.Slice(dimensions, func(i, j int) bool {
		if dimensions[i].GetProperty() == dimensions[j].GetProperty() {
			return dimensions[i].GetDcid() < dimensions[j].GetDcid()
		}
		return dimensions[i].GetProperty() < dimensions[j].GetProperty()
	})
	return dimensions
}

func (sds *SpannerDataSource) hydrateMultiEntityObservationNodes(
	ctx context.Context,
	resp *pbv2.ObservationResponse,
	nodeProperties []string,
) error {
	nodeDcids := multiEntityObservationDimensionDcids(resp)
	if len(nodeDcids) == 0 {
		return nil
	}
	nodeResp, err := sds.Node(ctx, &pbv2.NodeRequest{
		Nodes:    nodeDcids,
		Property: nodePropertyExpression(nodeProperties),
	}, datasources.DefaultPageSize)
	if err != nil {
		return err
	}
	resp.Nodes = nodeResp.GetData()
	return nil
}

func multiEntityObservationDimensionDcids(resp *pbv2.ObservationResponse) []string {
	dcidSet := map[string]struct{}{}
	for _, variableObservation := range resp.GetByVariable() {
		for _, observation := range variableObservation.GetMultiEntityObservations() {
			for _, dimension := range observation.GetObservationDimensions() {
				if dimension.GetDcid() != "" {
					dcidSet[dimension.GetDcid()] = struct{}{}
				}
			}
		}
	}
	dcids := make([]string, 0, len(dcidSet))
	for dcid := range dcidSet {
		dcids = append(dcids, dcid)
	}
	sort.Strings(dcids)
	return dcids
}

func nodePropertyExpression(properties []string) string {
	if len(properties) == 1 {
		return "->" + properties[0]
	}
	return "->[" + strings.Join(properties, ",") + "]"
}
