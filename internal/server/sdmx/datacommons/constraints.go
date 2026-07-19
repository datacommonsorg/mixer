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

package datacommons

import (
	"maps"
	"slices"
	"strings"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	PropertyContainedInPlace = "containedInPlace"
	PropertyTypeOf           = "typeOf"

	graphPredicateLinkedContainedInPlace = "linkedContainedInPlace"
)

type ComponentScope int

const (
	ComponentScopeAll ComponentScope = iota
	ComponentScopeDimension
	ComponentScopeObservationProperty
	ComponentScopeAttribute
)

type PropertyRule struct {
	Scope          ComponentScope
	Transitive     bool
	GraphPredicate string
}

type constraintConfig struct {
	propertyRules map[string]PropertyRule
}

var dataConstraintConfig = constraintConfig{
	propertyRules: map[string]PropertyRule{
		PropertyContainedInPlace: {
			Scope:          ComponentScopeObservationProperty,
			Transitive:     true,
			GraphPredicate: graphPredicateLinkedContainedInPlace,
		},
		PropertyTypeOf: {
			Scope:          ComponentScopeObservationProperty,
			Transitive:     false,
			GraphPredicate: PropertyTypeOf,
		},
	},
}

var availabilityConstraintConfig = constraintConfig{propertyRules: map[string]PropertyRule{}}

type ContainedInPlaceConstraint struct {
	Ancestor       string
	ChildPlaceType string
}

type TimePeriodMode int

const (
	TimePeriodAll TimePeriodMode = iota
	TimePeriodExplicit
	TimePeriodLatest
)

type TimePeriodSelection struct {
	Mode  TimePeriodMode
	Dates []string
}

// DataPropertyRule returns the rule used to validate and compile an SDMX data
// property constraint.
func DataPropertyRule(propertyID string) (PropertyRule, bool) {
	rule, ok := dataConstraintConfig.propertyRules[propertyID]
	return rule, ok
}

// ValidateDataConstraints checks the predicate features supported by the SDMX
// data endpoint.
func ValidateDataConstraints(constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	if err := validateComponentPredicates(constraints); err != nil {
		return err
	}

	for _, componentID := range slices.Sorted(maps.Keys(constraints)) {
		constraint := constraints[componentID]
		properties := constraint.GetPropertyConstraints()
		if len(properties) == 0 {
			continue
		}
		if len(constraint.GetPredicates()) > 0 {
			return status.Errorf(codes.InvalidArgument, "SDMX component filter %q cannot combine direct and property predicates", componentID)
		}
		_, hasContainedInPlace := properties[PropertyContainedInPlace]
		_, hasTypeOf := properties[PropertyTypeOf]
		if hasContainedInPlace && hasTypeOf && len(properties) != 2 {
			return status.Errorf(codes.InvalidArgument, "SDMX component filter %q cannot include additional property predicates", componentID)
		}

		for _, propertyID := range slices.Sorted(maps.Keys(properties)) {
			propertyConstraint := properties[propertyID]
			rule, ok := DataPropertyRule(propertyID)
			if !ok {
				return status.Errorf(codes.Unimplemented, "SDMX property constraint %q is not implemented yet", propertyID)
			}
			if !componentMatchesScope(componentID, rule.Scope) {
				return status.Errorf(codes.Unimplemented, "SDMX property constraints on component %q are not implemented yet", componentID)
			}
			if err := validatePropertyConstraint(componentID, propertyID, propertyConstraint, rule); err != nil {
				return err
			}
		}
		if !hasContainedInPlace || !hasTypeOf {
			return status.Errorf(codes.InvalidArgument, "SDMX property filters on component %q require containedInPlace+ and typeOf", componentID)
		}
	}
	if _, err := ClassifyTimePeriod(constraints); err != nil {
		return err
	}
	return validateRequiredVariableMeasured(constraints)
}

// ClassifyTimePeriod returns the request's time selection mode.
func ClassifyTimePeriod(constraints map[string]*sdmxpb.SdmxComponentConstraint) (TimePeriodSelection, error) {
	constraint, ok := constraints[ComponentTimePeriod]
	if !ok {
		return TimePeriodSelection{Mode: TimePeriodAll}, nil
	}
	if len(constraint.GetPredicates()) == 0 {
		return TimePeriodSelection{}, status.Error(codes.InvalidArgument, "SDMX TIME_PERIOD filter must have at least one value")
	}

	dates := map[string]struct{}{}
	latest := false
	for _, predicate := range constraint.GetPredicates() {
		if err := validatePredicate(predicate, "SDMX component filter \""+ComponentTimePeriod+"\""); err != nil {
			return TimePeriodSelection{}, err
		}
		value := strings.TrimSpace(predicate.GetValue())
		if strings.EqualFold(value, "LATEST") {
			latest = true
			continue
		}
		dates[value] = struct{}{}
	}
	if latest && len(dates) > 0 {
		return TimePeriodSelection{}, status.Error(codes.InvalidArgument, "SDMX TIME_PERIOD filter cannot combine LATEST with explicit dates")
	}
	if latest {
		return TimePeriodSelection{Mode: TimePeriodLatest}, nil
	}
	return TimePeriodSelection{
		Mode:  TimePeriodExplicit,
		Dates: slices.Sorted(maps.Keys(dates)),
	}, nil
}

// ValidateAvailabilityConstraints checks the predicate features supported by
// the SDMX availability endpoint.
func ValidateAvailabilityConstraints(constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	if err := validateComponentPredicates(constraints); err != nil {
		return err
	}
	for _, componentID := range slices.Sorted(maps.Keys(constraints)) {
		for _, propertyID := range slices.Sorted(maps.Keys(constraints[componentID].GetPropertyConstraints())) {
			rule, ok := availabilityConstraintConfig.propertyRules[propertyID]
			if !ok {
				return status.Error(codes.Unimplemented, "SDMX property constraints are not implemented for availability yet")
			}
			if err := validatePropertyConstraint(componentID, propertyID, constraints[componentID].GetPropertyConstraints()[propertyID], rule); err != nil {
				return err
			}
		}
	}
	timeSelection, err := ClassifyTimePeriod(constraints)
	if err != nil {
		return err
	}
	if timeSelection.Mode == TimePeriodLatest {
		return status.Error(codes.InvalidArgument, "SDMX TIME_PERIOD filter LATEST is not valid for availability; use explicit dates")
	}
	return validateRequiredVariableMeasured(constraints)
}

// ContainedInPlaceConstraints returns the validated containment pair for each
// constrained component.
func ContainedInPlaceConstraints(constraints map[string]*sdmxpb.SdmxComponentConstraint) (map[string]ContainedInPlaceConstraint, error) {
	if err := ValidateDataConstraints(constraints); err != nil {
		return nil, err
	}
	result := map[string]ContainedInPlaceConstraint{}
	for componentID, constraint := range constraints {
		properties := constraint.GetPropertyConstraints()
		if len(properties) == 0 {
			continue
		}
		result[componentID] = ContainedInPlaceConstraint{
			Ancestor:       properties[PropertyContainedInPlace].GetPredicates()[0].GetValue(),
			ChildPlaceType: properties[PropertyTypeOf].GetPredicates()[0].GetValue(),
		}
	}
	return result, nil
}

func validateComponentPredicates(constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	for _, componentID := range slices.Sorted(maps.Keys(constraints)) {
		for _, predicate := range constraints[componentID].GetPredicates() {
			if err := validatePredicate(predicate, "SDMX component filter \""+componentID+"\""); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRequiredVariableMeasured(constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	constraint, ok := constraints[ComponentVariableMeasured]
	if !ok || len(constraint.GetPredicates()) == 0 {
		return status.Error(codes.InvalidArgument, "missing required SDMX component filter variableMeasured")
	}
	return nil
}

func componentMatchesScope(componentID string, scope ComponentScope) bool {
	if scope == ComponentScopeAll {
		return true
	}

	kind, known := DataComponentKind(componentID)
	switch scope {
	case ComponentScopeDimension:
		return !known || kind == ComponentKindDimension
	case ComponentScopeObservationProperty:
		return componentID == ComponentObservationAbout || !known
	case ComponentScopeAttribute:
		return known && kind == ComponentKindAttribute
	default:
		return false
	}
}

func validatePredicate(predicate *sdmxpb.SdmxPredicate, filter string) error {
	if predicate.GetOperator() != sdmxpb.SdmxOperator_SDMX_OPERATOR_EQ {
		return status.Error(codes.Unimplemented, "SDMX operators other than EQ are not implemented yet")
	}
	if strings.TrimSpace(predicate.GetValue()) == "" {
		return status.Errorf(codes.InvalidArgument, "%s contains an empty value", filter)
	}
	return nil
}

func validatePropertyConstraint(
	componentID string,
	propertyID string,
	constraint *sdmxpb.SdmxPropertyConstraint,
	rule PropertyRule,
) error {
	if constraint == nil || len(constraint.GetPredicates()) != 1 {
		return status.Errorf(codes.InvalidArgument, "SDMX property filter %q on component %q must have exactly one value", propertyID, componentID)
	}
	if constraint.GetTransitive() != rule.Transitive {
		selector := propertyID
		if constraint.GetTransitive() {
			selector += "+"
		}
		return status.Errorf(codes.Unimplemented, "SDMX property constraint %q is not implemented yet", selector)
	}
	return validatePredicate(constraint.GetPredicates()[0], propertyFilterName(componentID, propertyID))
}

func propertyFilterName(componentID, propertyID string) string {
	return "SDMX property filter " + componentID + "." + propertyID
}
