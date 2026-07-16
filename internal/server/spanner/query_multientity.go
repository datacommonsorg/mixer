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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"cloud.google.com/go/spanner"
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maxObservationPropertyEntitySlots matches the materialized entity1/entity2/entity3 TimeSeries slots.
	maxObservationPropertyEntitySlots = 3
	minObservationPropertiesPageSize  = 100
)

var sdmxFacetComponentIDs = map[string]struct{}{
	datacommons.ComponentUnit:              {},
	datacommons.ComponentMeasurementMethod: {},
	datacommons.ComponentObservationPeriod: {},
	datacommons.ComponentScalingFactor:     {},
}

// GetObservations retrieves observations using the new schema.
func (nc *multiEntityClient) GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*Observation, error) {
	stmt, err := nc.queryBuilder.GetObservationsQuery(variables, entities, date)
	if err != nil {
		return nil, err
	}

	var rawObs []*rawObservation
	err = queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	if err != nil {
		return nil, err
	}

	observations, err := reconstructObservations(rawObs)
	if err != nil {
		return nil, err
	}
	if err := validateObservations(observations); err != nil {
		return nil, err
	}
	return observations, nil
}

// CheckVariableExistence checks variable existence across all entity slots in a single CTE-based query.
func (nc *multiEntityClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	stmt, err := nc.queryBuilder.GetStatVarsByEntityQuery(variables, entities)
	if err != nil {
		return nil, err
	}
	return queryDynamic(ctx, nc.sc, *stmt)
}

// CheckVariableGroupPlaceExistence checks SVG/topic existence across all entity slots.
func (nc *multiEntityClient) CheckVariableGroupPlaceExistence(ctx context.Context, variableGroups []string, entities []string, predicate string) ([][]string, error) {
	if len(variableGroups) == 0 || len(entities) == 0 {
		return [][]string{}, nil
	}
	stmt, err := nc.queryBuilder.GetGroupPlaceExistenceQuery(variableGroups, entities, predicate)
	if err != nil {
		return nil, err
	}
	return queryDynamic(ctx, nc.sc, *stmt)
}

// GetStatVarGroupNode fetches StatVarGroupNode info using multi-entity TimeSeries existence checks.
func (nc *multiEntityClient) GetStatVarGroupNode(ctx context.Context, nodes []string, includeDefinitions bool) ([]*StatVarGroupNode, error) {
	var svgNodes []*StatVarGroupNode
	if len(nodes) == 0 {
		return svgNodes, nil
	}

	stmt, err := nc.queryBuilder.GetStatVarGroupNodeQuery(nodes, includeDefinitions)
	if err != nil {
		return svgNodes, err
	}

	err = queryStructs(
		ctx,
		nc.sc,
		*stmt,
		func() interface{} {
			return &StatVarGroupNode{}
		},
		func(rowStruct interface{}) {
			svgNodes = append(svgNodes, rowStruct.(*StatVarGroupNode))
		},
	)
	if err != nil {
		return svgNodes, err
	}

	return svgNodes, nil
}

// GetFilteredStatVarGroupNode fetches filtered StatVarGroupNode info using multi-entity TimeSeries filters.
func (nc *multiEntityClient) GetFilteredStatVarGroupNode(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (map[string]*FilteredStatVarGroupNode, error) {
	response := map[string]*FilteredStatVarGroupNode{}
	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(maxConcurrentFilteredSVGGoroutines)

	type nodeResult struct {
		node string
		resp *FilteredStatVarGroupNode
	}
	resps := make(chan nodeResult, len(nodes))

	for _, node := range nodes {
		node := node
		errGroup.Go(func() error {
			resp, err := nc.getSingleFilteredStatVarGroupNode(errCtx, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions)
			if err != nil {
				return fmt.Errorf("error fetching filtered StatVarGroupNode for node %s: %w", node, err)
			}
			resps <- nodeResult{node: node, resp: resp}
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(resps)

	for res := range resps {
		response[res.node] = res.resp
	}

	return response, nil
}

func (nc *multiEntityClient) getSingleFilteredStatVarGroupNode(ctx context.Context, node string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (*FilteredStatVarGroupNode, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	var svgChildren []*SVGChild
	var childSVs []*ChildSV
	var childSVGs []*ChildSVG

	errGroup.Go(func() error {
		return queryStructs(
			errCtx,
			nc.sc,
			*GetSVGChildrenQuery(node, includeDefinitions),
			func() interface{} {
				return &SVGChild{}
			},
			func(rowStruct interface{}) {
				svgChildren = append(svgChildren, rowStruct.(*SVGChild))
			},
		)
	})

	errGroup.Go(func() error {
		stmt, err := nc.queryBuilder.GetFilteredSVGChildrenQuery(templateSV, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions)
		if err != nil {
			return err
		}
		return queryStructs(
			errCtx,
			nc.sc,
			*stmt,
			func() interface{} {
				return &ChildSV{}
			},
			func(rowStruct interface{}) {
				childSVs = append(childSVs, rowStruct.(*ChildSV))
			},
		)
	})

	errGroup.Go(func() error {
		stmt, err := nc.queryBuilder.GetFilteredSVGChildrenQuery(templateSVG, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions)
		if err != nil {
			return err
		}
		return queryStructs(
			errCtx,
			nc.sc,
			*stmt,
			func() interface{} {
				return &ChildSVG{}
			},
			func(rowStruct interface{}) {
				childSVGs = append(childSVGs, rowStruct.(*ChildSVG))
			},
		)
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &FilteredStatVarGroupNode{
		SVGChild: svgChildren,
		ChildSV:  childSVs,
		ChildSVG: childSVGs,
	}, nil
}

// GetFilteredTopic fetches filtered Topic counts using multi-entity TimeSeries filters.
func (nc *multiEntityClient) GetFilteredTopic(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (map[string]int, error) {
	counts := make(map[string]int, len(nodes))
	for _, node := range nodes {
		counts[node] = 0
	}

	stmt, err := nc.queryBuilder.GetFilteredTopicChildrenQuery(nodes, constrainedPlaces, constrainedImport, numEntitiesExistence)
	if err != nil {
		return counts, err
	}
	err = nc.sc.executeQuery(ctx, *stmt, func(iter *spanner.RowIterator) error {
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			var parent string
			var count int64
			if err := row.Columns(&parent, &count); err != nil {
				return fmt.Errorf("error reading row for filtered Topic children count: %w", err)
			}
			counts[parent] = int(count)
		}
		return nil
	})
	if err != nil {
		return counts, err
	}

	return counts, nil
}

// GetObservationsContainedInPlace fetches observations for children containment.
func (nc *multiEntityClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace, date string) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || containedInPlace == nil {
		return observations, nil
	}

	stmt, err := nc.queryBuilder.GetObservationsContainedInPlaceQuery(variables, containedInPlace, date)
	if err != nil {
		return nil, err
	}

	var rawObs []*rawObservation
	err = queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	if err != nil {
		return nil, err
	}

	observations, err = reconstructObservations(rawObs)
	if err != nil {
		return nil, err
	}
	if err := validateObservations(observations); err != nil {
		return nil, err
	}
	return observations, nil
}

// reconstructObservations processes raw Spanner rows and handles JSON facets extraction in Go code.
func reconstructObservations(rawObs []*rawObservation) ([]*Observation, error) {
	var result []*Observation

	for _, r := range rawObs {
		obs := &Observation{
			VariableMeasured: r.VariableMeasured,
			ObservationAbout: r.ObservationAbout,
			FacetId:          r.FacetId,
			Observations:     TimeSeries{},
		}
		if r.ProvenanceID.Valid {
			obs.ProvenanceID = r.ProvenanceID.StringVal
		}

		for _, dv := range r.DatesAndValues {
			if dv == nil {
				continue
			}
			if dv.Date != "" {
				attributes, err := decodeObservationAttributes(dv.Attributes)
				if err != nil {
					return nil, fmt.Errorf(
						"invalid observation attributes: variable=%q entity=%q facet_id=%q date=%q: %w",
						r.VariableMeasured,
						r.ObservationAbout,
						r.FacetId,
						dv.Date,
						err,
					)
				}
				obs.Observations = append(obs.Observations, &DateValue{
					Date:       dv.Date,
					Value:      dv.Value,
					Attributes: attributes,
				})
			}
		}

		// Sort observations chronologically in Go.
		slices.SortFunc(obs.Observations, func(a, b *DateValue) int {
			return cmp.Compare(a.Date, b.Date)
		})

		if r.Facets.Valid {
			if m, ok := r.Facets.Value.(map[string]interface{}); ok {
				populateObservationFacets(obs, m)
			}
		}
		result = append(result, obs)
	}

	return result, nil
}

func validateObservations(observations []*Observation) error {
	for _, obs := range observations {
		if obs.ProvenanceID == "" {
			return fmt.Errorf(
				"observation missing provenance: variable=%q entity=%q facet_id=%q",
				obs.VariableMeasured,
				obs.ObservationAbout,
				obs.FacetId,
			)
		}
	}
	return nil
}

func populateObservationFacets(obs *Observation, facets map[string]interface{}) {
	obs.ImportName = getJSONString(facets, "importName")
	obs.ObservationPeriod = getJSONString(facets, "observationPeriod")
	obs.MeasurementMethod = getJSONString(facets, "measurementMethod")
	obs.Unit = getJSONString(facets, "unit")
	obs.ScalingFactor = getJSONString(facets, "scalingFactor")
	obs.IsDcAggregate = getJSONBool(facets, "isDcAggregate")
	obs.ProvenanceURL = getJSONString(facets, "provenanceUrl")
}

// decodeObservationAttributes expects attributes to be a JSON object that can be
// represented as a string:string map. Entries with incompatible values are skipped.
func decodeObservationAttributes(attrs spanner.NullJSON) (map[string]string, error) {
	if !attrs.Valid || attrs.Value == nil {
		return nil, nil
	}

	switch values := attrs.Value.(type) {
	case map[string]interface{}:
		result := make(map[string]string, len(values))
		for key, value := range values {
			stringValue, ok := observationAttributeValueToString(value)
			if !ok {
				continue
			}
			result[key] = stringValue
		}
		if len(result) == 0 {
			return nil, nil
		}
		return result, nil
	case map[string]string:
		if len(values) == 0 {
			return nil, nil
		}
		result := make(map[string]string, len(values))
		for key, value := range values {
			result[key] = value
		}
		return result, nil
	default:
		return nil, fmt.Errorf("attributes JSON must be an object, got %T", attrs.Value)
	}
}

func observationAttributeValueToString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case bool, float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v), true
	case json.Number:
		return v.String(), true
	default:
		return "", false
	}
}

func getJSONString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func getJSONBool(m map[string]interface{}, key string) bool {
	if val, ok := m[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

// GetSdmxObservations retrieves observations for SDMX query using dynamic entity slot mappings.
func (nc *multiEntityClient) GetSdmxObservations(
	ctx context.Context,
	req *sdmxpb.SdmxDataQuery,
) (*sdmxpb.SdmxDataResult, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "SDMX data request cannot be nil")
	}
	if req.Constraints == nil {
		return nil, status.Error(codes.InvalidArgument, "SDMX data request constraints cannot be nil")
	}

	prepared, err := prepareSdmxObservationsQuery(
		ctx,
		req.Constraints,
		nc.sc.GetNodeEdgesByID,
		nc.queryBuilder,
	)
	if err != nil {
		return nil, err
	}

	var rawObs []*rawObservation
	err = queryStructs(ctx, nc.sc, *prepared.statement, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	if err != nil {
		return nil, sdmxBackendError("failed to execute SDMX data query", err)
	}

	result := &sdmxpb.SdmxDataResult{
		Shape:  prepared.shape,
		Series: []*sdmxpb.SdmxTimeSeries{},
	}

	for _, r := range rawObs {
		// Parse entities JSON to map physical entity slots (entity1, entity2) to actual DCIDs.
		entitySlotValues := map[string]string{}
		if r.Entities.Valid {
			if m, ok := r.Entities.Value.(map[string]interface{}); ok {
				for k, v := range m {
					if s, ok := v.(string); ok {
						entitySlotValues[k] = s
					}
				}
			}
		}

		series := &sdmxpb.SdmxTimeSeries{
			Dimensions: sdmxSeriesDimensions(r.VariableMeasured, entitySlotValues, prepared.entitySlotByObservationProperty),
			Points:     []*sdmxpb.SdmxDataPoint{},
		}
		if r.ProvenanceID.Valid {
			series.Dimensions[datacommons.ComponentProvenance] = r.ProvenanceID.StringVal
		}

		for _, dv := range r.DatesAndValues {
			if dv == nil {
				continue
			}
			if dv.Date != "" {
				series.Points = append(series.Points, &sdmxpb.SdmxDataPoint{
					TimePeriod:       dv.Date,
					ObservationValue: dv.Value,
				})
			}
		}

		if r.Facets.Valid {
			if m, ok := r.Facets.Value.(map[string]interface{}); ok {
				populateSdmxFacetComponents(series, m)
			}
		}
		populateSdmxFacetID(series, r.FacetId)

		result.Series = append(result.Series, series)
	}

	return result, nil
}

type getNodeEdgesByIDFunc func(
	context.Context,
	[]string,
	*v2.Arc,
	int,
	int,
) (map[string][]*Edge, error)

type preparedSdmxObservationsQuery struct {
	shape                           *sdmxpb.SdmxDataShape
	entitySlotByObservationProperty map[string]string
	statement                       *spanner.Statement
}

type preparedSdmxShape struct {
	shape                           *sdmxpb.SdmxDataShape
	observationProperties           []string
	entitySlotByObservationProperty map[string]string
}

func prepareSdmxObservationsQuery(
	ctx context.Context,
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	getNodeEdgesByID getNodeEdgesByIDFunc,
	queryBuilder *multiEntityQueryBuilder,
) (*preparedSdmxObservationsQuery, error) {
	if err := datacommons.ValidateDataConstraints(constraints); err != nil {
		return nil, err
	}
	preparedShape, err := prepareSdmxShape(ctx, constraints, getNodeEdgesByID)
	if err != nil {
		return nil, err
	}
	if err := validateSdmxPropertyConstraintScopes(constraints, preparedShape.entitySlotByObservationProperty); err != nil {
		return nil, err
	}
	if err := validateSdmxDataConstraintComponents(constraints, preparedShape.shape); err != nil {
		return nil, err
	}
	if err := validateSdmxRequiredObservationProperty(constraints, preparedShape.observationProperties); err != nil {
		return nil, err
	}

	statement, err := queryBuilder.GetSdmxObservationsQuery(constraints, preparedShape.entitySlotByObservationProperty)
	if err != nil {
		return nil, err
	}
	return &preparedSdmxObservationsQuery{
		shape:                           preparedShape.shape,
		entitySlotByObservationProperty: preparedShape.entitySlotByObservationProperty,
		statement:                       statement,
	}, nil
}

func prepareSdmxShape(
	ctx context.Context,
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	getNodeEdgesByID getNodeEdgesByIDFunc,
) (*preparedSdmxShape, error) {
	if err := validateSdmxConstraintValues(constraints); err != nil {
		return nil, err
	}
	statVarIDs := sortedUniqueStrings(sdmxConstraintValues(constraints[datacommons.ComponentVariableMeasured]))

	arc := &v2.Arc{
		Out:        true,
		SingleProp: "observationProperties",
	}
	observationPropertyEdgesByStatVar, err := getNodeEdgesByID(ctx, statVarIDs, arc, observationPropertiesPageSize(len(statVarIDs)), 0)
	if err != nil {
		return nil, sdmxBackendError("failed to fetch observationProperties", err)
	}
	observationProperties, entitySlotByObservationProperty, err := resolveSdmxEntityShape(statVarIDs, observationPropertyEdgesByStatVar)
	if err != nil {
		return nil, err
	}
	shape := sdmxDataShape(observationProperties)
	return &preparedSdmxShape{
		shape:                           shape,
		observationProperties:           observationProperties,
		entitySlotByObservationProperty: entitySlotByObservationProperty,
	}, nil
}

func sdmxSeriesDimensions(
	variableMeasured string,
	entitySlotValues map[string]string,
	entitySlotByObservationProperty map[string]string,
) map[string]string {
	dimensionValues := map[string]string{
		datacommons.ComponentVariableMeasured: variableMeasured,
	}
	for observationProperty, entitySlot := range entitySlotByObservationProperty {
		if value, ok := entitySlotValues[entitySlot]; ok {
			dimensionValues[observationProperty] = value
		}
	}
	return dimensionValues
}

func validateSdmxConstraintValues(constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	variableMeasured, ok := constraints[datacommons.ComponentVariableMeasured]
	if !ok || len(sdmxConstraintValues(variableMeasured)) == 0 {
		return status.Error(codes.InvalidArgument, "SDMX component filter variableMeasured must be specified")
	}

	for _, componentID := range slices.Sorted(maps.Keys(constraints)) {
		constraint := constraints[componentID]
		values := sdmxConstraintValues(constraint)
		if len(values) == 0 {
			if len(constraint.GetPropertyConstraints()) > 0 {
				continue
			}
			return status.Errorf(codes.InvalidArgument, "SDMX component filter %q must have at least one value", componentID)
		}
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return status.Errorf(codes.InvalidArgument, "SDMX component filter %q contains an empty value", componentID)
			}
		}
	}
	return nil
}

func validateSdmxPropertyConstraintScopes(
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	entitySlotByObservationProperty map[string]string,
) error {
	for _, componentID := range slices.Sorted(maps.Keys(constraints)) {
		propertyConstraints := constraints[componentID].GetPropertyConstraints()
		if len(propertyConstraints) == 0 {
			continue
		}
		if _, ok := entitySlotByObservationProperty[componentID]; !ok {
			return status.Errorf(codes.Unimplemented, "SDMX property constraints on component %q are not implemented yet", componentID)
		}
	}
	return nil
}

func validateSdmxDataConstraintComponents(
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	shape *sdmxpb.SdmxDataShape,
) error {
	return validateSdmxConstraintComponents(constraints, sdmxFilterableDataComponents(shape), "components")
}

func validateSdmxAvailabilityConstraintComponents(
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	shape *sdmxpb.SdmxDataShape,
) error {
	return validateSdmxConstraintComponents(constraints, sdmxFilterableDimensions(shape), "dimensions")
}

func validateSdmxConstraintComponents(
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	filterableComponents map[string]struct{},
	componentKind string,
) error {

	for _, componentID := range slices.Sorted(maps.Keys(constraints)) {
		if _, ok := filterableComponents[componentID]; !ok {
			return status.Errorf(
				codes.InvalidArgument,
				"unsupported SDMX component filter %q; filterable %s are %v",
				componentID,
				componentKind,
				slices.Sorted(maps.Keys(filterableComponents)),
			)
		}
	}
	return nil
}

func validateSdmxRequiredObservationProperty(
	constraints map[string]*sdmxpb.SdmxComponentConstraint,
	observationProperties []string,
) error {
	for _, observationProperty := range observationProperties {
		if _, ok := constraints[observationProperty]; ok {
			return nil
		}
	}
	return status.Errorf(
		codes.InvalidArgument,
		"SDMX data query must include at least one observation property filter; allowed observation properties are %v",
		observationProperties,
	)
}

func validateSdmxAvailabilityComponent(componentID string, shape *sdmxpb.SdmxDataShape) error {
	filterableDimensions := sdmxFilterableDimensions(shape)
	if _, ok := filterableDimensions[componentID]; ok {
		return nil
	}
	return status.Errorf(
		codes.InvalidArgument,
		"unsupported SDMX availability component %q; filterable dimensions are %v",
		componentID,
		slices.Sorted(maps.Keys(filterableDimensions)),
	)
}

func sdmxFilterableDimensions(shape *sdmxpb.SdmxDataShape) map[string]struct{} {
	filterableDimensions := map[string]struct{}{}
	for _, component := range shape.GetComponents() {
		if component.GetKind() == sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION && component.GetId() != datacommons.ComponentTimePeriod {
			filterableDimensions[component.GetId()] = struct{}{}
		}
	}
	return filterableDimensions
}

func sdmxFilterableDataComponents(shape *sdmxpb.SdmxDataShape) map[string]struct{} {
	filterableComponents := sdmxFilterableDimensions(shape)
	for componentID := range datacommons.FilterableAttributes {
		filterableComponents[componentID] = struct{}{}
	}
	return filterableComponents
}

func populateSdmxFacetComponents(series *sdmxpb.SdmxTimeSeries, facets map[string]interface{}) {
	for key, value := range facets {
		kind, ok := sdmxFacetComponentKind(key)
		if !ok {
			continue
		}
		stringValue, ok := observationAttributeValueToString(value)
		if !ok || stringValue == "" {
			continue
		}
		switch kind {
		case datacommons.ComponentKindDimension:
			if series.Dimensions == nil {
				series.Dimensions = map[string]string{}
			}
			series.Dimensions[key] = stringValue
		case datacommons.ComponentKindAttribute:
			if series.Attributes == nil {
				series.Attributes = map[string]string{}
			}
			series.Attributes[key] = stringValue
		}
	}
}

func populateSdmxFacetID(series *sdmxpb.SdmxTimeSeries, facetID string) {
	if facetID == "" {
		return
	}
	if series.Attributes == nil {
		series.Attributes = map[string]string{}
	}
	series.Attributes[datacommons.ComponentFacetID] = facetID
}

func sdmxFacetComponentKind(componentID string) (datacommons.ComponentKind, bool) {
	if _, ok := sdmxFacetComponentIDs[componentID]; !ok {
		return "", false
	}
	return datacommons.DataComponentKind(componentID)
}

// GetSdmxAvailability retrieves available SDMX dimension values.
func (nc *multiEntityClient) GetSdmxAvailability(
	ctx context.Context,
	req *sdmxpb.SdmxAvailabilityQuery,
) (*sdmxpb.SdmxAvailabilityResult, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "SDMX availability request cannot be nil")
	}
	if req.Constraints == nil {
		return nil, status.Error(codes.InvalidArgument, "SDMX availability request constraints cannot be nil")
	}

	stmt, err := prepareSdmxAvailabilityQuery(
		ctx,
		req,
		nc.sc.GetNodeEdgesByID,
		nc.queryBuilder,
	)
	if err != nil {
		return nil, err
	}

	result := &sdmxpb.SdmxAvailabilityResult{}
	err = queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawSdmxAvailabilityValue{} }, func(row interface{}) {
		value := row.(*rawSdmxAvailabilityValue).Value
		if value != "" {
			result.Values = append(result.Values, value)
		}
	})
	if err != nil {
		return nil, sdmxBackendError("failed to execute SDMX availability query", err)
	}
	return result, nil
}

func prepareSdmxAvailabilityQuery(
	ctx context.Context,
	req *sdmxpb.SdmxAvailabilityQuery,
	getNodeEdgesByID getNodeEdgesByIDFunc,
	queryBuilder *multiEntityQueryBuilder,
) (*spanner.Statement, error) {
	if err := datacommons.ValidateAvailabilityConstraints(req.GetConstraints()); err != nil {
		return nil, err
	}
	preparedShape, err := prepareSdmxShape(ctx, req.GetConstraints(), getNodeEdgesByID)
	if err != nil {
		return nil, err
	}
	if err := validateSdmxAvailabilityConstraintComponents(req.GetConstraints(), preparedShape.shape); err != nil {
		return nil, err
	}
	if err := validateSdmxAvailabilityComponent(req.GetComponentId(), preparedShape.shape); err != nil {
		return nil, err
	}
	return queryBuilder.GetSdmxAvailabilityQuery(req, preparedShape.entitySlotByObservationProperty)
}

func resolveSdmxEntityShape(
	statVarIDs []string,
	observationPropertyEdgesByStatVar map[string][]*Edge,
) ([]string, map[string]string, error) {
	observationPropertiesByStatVar := map[string][]string{}
	for _, statVarID := range statVarIDs {
		observationPropertySet := map[string]struct{}{}
		for _, edge := range observationPropertyEdgesByStatVar[statVarID] {
			if edge == nil {
				continue
			}
			if edge.Predicate != "observationProperties" {
				continue
			}
			property := strings.TrimSpace(edge.Value)
			if property == "" {
				continue
			}
			if _, reserved := datacommons.DataComponentKind(property); reserved && property != datacommons.ComponentObservationAbout {
				return nil, nil, status.Errorf(
					codes.InvalidArgument,
					"resolveSdmxEntityShape: stat var %q has reserved observationProperty %q",
					statVarID,
					property,
				)
			}
			observationPropertySet[property] = struct{}{}
		}

		// Ingestion assigns sorted observationProperties to entity1, entity2, entity3.
		observationProperties := slices.Sorted(maps.Keys(observationPropertySet))
		if len(observationProperties) > maxObservationPropertyEntitySlots {
			return nil, nil, status.Errorf(
				codes.InvalidArgument,
				"resolveSdmxEntityShape: stat var %q has %d observationProperties; max supported entity slots is %d",
				statVarID,
				len(observationProperties),
				maxObservationPropertyEntitySlots,
			)
		}
		if len(observationProperties) == 0 {
			observationProperties = []string{datacommons.ComponentObservationAbout}
		}
		observationPropertiesByStatVar[statVarID] = observationProperties
	}

	var resolvedObservationProperties []string
	referenceStatVarID := ""
	for _, statVarID := range statVarIDs {
		observationProperties := observationPropertiesByStatVar[statVarID]
		if resolvedObservationProperties == nil {
			resolvedObservationProperties = observationProperties
			referenceStatVarID = statVarID
			continue
		}
		if !slices.Equal(resolvedObservationProperties, observationProperties) {
			return nil, nil, status.Errorf(
				codes.InvalidArgument,
				"resolveSdmxEntityShape: incompatible observationProperties for stat var %q: got %v, want %v from stat var %q",
				statVarID,
				observationProperties,
				resolvedObservationProperties,
				referenceStatVarID,
			)
		}
	}

	entitySlotByObservationProperty := map[string]string{}
	for i, observationProperty := range resolvedObservationProperties {
		entitySlotByObservationProperty[observationProperty] = fmt.Sprintf("entity%d", i+1)
	}
	return resolvedObservationProperties, entitySlotByObservationProperty, nil
}

func sdmxBackendError(message string, err error) error {
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return err
	}
	slog.Error(message, "error", err)
	return status.Error(codes.Internal, "Internal server error occurred while processing the request.")
}

func sdmxDataShape(observationProperties []string) *sdmxpb.SdmxDataShape {
	components := datacommons.DataComponentsForObservationProperties(observationProperties)
	result := &sdmxpb.SdmxDataShape{
		Components: make([]*sdmxpb.SdmxComponent, 0, len(components)),
	}
	for _, component := range components {
		result.Components = append(result.Components, &sdmxpb.SdmxComponent{
			Id:   component.ID,
			Kind: sdmxProtoComponentKind(component.Kind),
		})
	}
	return result
}

func sdmxProtoComponentKind(kind datacommons.ComponentKind) sdmxpb.SdmxComponentKind {
	switch kind {
	case datacommons.ComponentKindDimension:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_DIMENSION
	case datacommons.ComponentKindMeasure:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_MEASURE
	case datacommons.ComponentKindAttribute:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_ATTRIBUTE
	default:
		return sdmxpb.SdmxComponentKind_SDMX_COMPONENT_KIND_UNSPECIFIED
	}
}

func observationPropertiesPageSize(variableCount int) int {
	// Fetch one more property per variable than the schema supports so unsupported
	// metadata is detected before SQL generation.
	pageSize := variableCount * (maxObservationPropertyEntitySlots + 1)
	if pageSize < minObservationPropertiesPageSize {
		return minObservationPropertiesPageSize
	}
	return pageSize
}

func sortedUniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		seen[value] = struct{}{}
	}
	return slices.Sorted(maps.Keys(seen))
}
