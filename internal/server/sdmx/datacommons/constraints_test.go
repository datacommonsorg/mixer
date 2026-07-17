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
	"testing"

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testContainedInPlaceConstraint(ancestor, childPlaceType string) *sdmxpb.SdmxComponentConstraint {
	return &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			PropertyContainedInPlace: {
				Predicates: []*sdmxpb.SdmxPredicate{{Value: ancestor}},
				Transitive: true,
			},
			PropertyTypeOf: {
				Predicates: []*sdmxpb.SdmxPredicate{{Value: childPlaceType}},
			},
		},
	}
}

func TestValidateDataConstraints(t *testing.T) {
	for _, tc := range []struct {
		name       string
		constraint *sdmxpb.SdmxComponentConstraint
		wantCode   codes.Code
	}{
		{name: "valid pair", constraint: testContainedInPlaceConstraint("country/USA", "County")},
		{
			name: "direct and property predicates",
			constraint: func() *sdmxpb.SdmxComponentConstraint {
				constraint := testContainedInPlaceConstraint("country/USA", "County")
				constraint.Predicates = []*sdmxpb.SdmxPredicate{{Value: "geoId/06"}}
				return constraint
			}(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing type",
			constraint: &sdmxpb.SdmxComponentConstraint{PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
				PropertyContainedInPlace: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "country/USA"}}, Transitive: true},
			}},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "multiple ancestors",
			constraint: &sdmxpb.SdmxComponentConstraint{PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
				PropertyContainedInPlace: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "country/USA"}, {Value: "country/CAN"}}, Transitive: true},
				PropertyTypeOf:           {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}},
			}},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "multiple child place types",
			constraint: &sdmxpb.SdmxComponentConstraint{PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
				PropertyContainedInPlace: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "country/USA"}}, Transitive: true},
				PropertyTypeOf:           {Predicates: []*sdmxpb.SdmxPredicate{{Value: "Country"}, {Value: "State"}}},
			}},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "direct containment",
			constraint: &sdmxpb.SdmxComponentConstraint{PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
				PropertyContainedInPlace: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "country/USA"}}},
				PropertyTypeOf:           {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}},
			}},
			wantCode: codes.Unimplemented,
		},
		{
			name: "transitive type",
			constraint: &sdmxpb.SdmxComponentConstraint{PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
				PropertyContainedInPlace: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "country/USA"}}, Transitive: true},
				PropertyTypeOf:           {Predicates: []*sdmxpb.SdmxPredicate{{Value: "County"}}, Transitive: true},
			}},
			wantCode: codes.Unimplemented,
		},
		{
			name: "unknown property",
			constraint: &sdmxpb.SdmxComponentConstraint{PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
				"name": {Predicates: []*sdmxpb.SdmxPredicate{{Value: "California"}}},
			}},
			wantCode: codes.Unimplemented,
		},
		{
			name: "additional property",
			constraint: func() *sdmxpb.SdmxComponentConstraint {
				constraint := testContainedInPlaceConstraint("country/USA", "County")
				constraint.PropertyConstraints["name"] = &sdmxpb.SdmxPropertyConstraint{Predicates: []*sdmxpb.SdmxPredicate{{Value: "California"}}}
				return constraint
			}(),
			wantCode: codes.InvalidArgument,
		},
		{
			name:       "blank ancestor",
			constraint: testContainedInPlaceConstraint(" ", "County"),
			wantCode:   codes.InvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDataConstraints(map[string]*sdmxpb.SdmxComponentConstraint{
				ComponentVariableMeasured: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "Count_Person"}}},
				ComponentObservationAbout: tc.constraint,
			})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("ValidateDataConstraints() code = %v, want %v; err = %v", got, tc.wantCode, err)
			}
		})
	}
}

func TestValidateDataConstraintsRequiresVariableMeasured(t *testing.T) {
	for _, tc := range []struct {
		name        string
		constraints map[string]*sdmxpb.SdmxComponentConstraint
	}{
		{name: "missing", constraints: nil},
		{name: "no predicates", constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			ComponentVariableMeasured: {},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDataConstraints(tc.constraints)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("ValidateDataConstraints() code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
			}
			if got, want := status.Convert(err).Message(), "missing required SDMX component filter variableMeasured"; got != want {
				t.Fatalf("ValidateDataConstraints() message = %q, want %q", got, want)
			}
		})
	}
}

func TestValidateDataConstraintsPropertyScopes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		componentID string
		wantCode    codes.Code
	}{
		{name: "observation about", componentID: ComponentObservationAbout},
		{name: "dynamic observation property", componentID: "sourceCountry"},
		{name: "known dimension", componentID: ComponentUnit, wantCode: codes.Unimplemented},
		{name: "known attribute", componentID: ComponentFacetID, wantCode: codes.Unimplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDataConstraints(map[string]*sdmxpb.SdmxComponentConstraint{
				ComponentVariableMeasured: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "Count_Person"}}},
				tc.componentID:            testContainedInPlaceConstraint("country/USA", "County"),
			})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("ValidateDataConstraints() code = %v, want %v; err = %v", got, tc.wantCode, err)
			}
		})
	}
}

func TestContainedInPlaceConstraints(t *testing.T) {
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{
		"observationAbout": testContainedInPlaceConstraint("country/USA", "County"),
		"variableMeasured": {Predicates: []*sdmxpb.SdmxPredicate{{Value: "Count_Person"}}},
	}
	got, err := ContainedInPlaceConstraints(constraints)
	if err != nil {
		t.Fatalf("ContainedInPlaceConstraints() error = %v", err)
	}
	want := map[string]ContainedInPlaceConstraint{
		"observationAbout": {Ancestor: "country/USA", ChildPlaceType: "County"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("ContainedInPlaceConstraints() mismatch (-want +got):\n%s", diff)
	}
}

func TestValidateAvailabilityConstraintsRejectsProperties(t *testing.T) {
	err := ValidateAvailabilityConstraints(map[string]*sdmxpb.SdmxComponentConstraint{
		"observationAbout": testContainedInPlaceConstraint("country/USA", "County"),
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("ValidateAvailabilityConstraints() code = %v, want %v; err = %v", status.Code(err), codes.Unimplemented, err)
	}
}

func TestValidateAvailabilityConstraintsRequiresVariableMeasured(t *testing.T) {
	for _, tc := range []struct {
		name        string
		constraints map[string]*sdmxpb.SdmxComponentConstraint
		wantCode    codes.Code
	}{
		{name: "valid", constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			ComponentVariableMeasured: {Predicates: []*sdmxpb.SdmxPredicate{{Value: "Count_Person"}}},
		}},
		{name: "missing", wantCode: codes.InvalidArgument},
		{name: "no predicates", constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			ComponentVariableMeasured: {},
		}, wantCode: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAvailabilityConstraints(tc.constraints)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("ValidateAvailabilityConstraints() code = %v, want %v; err = %v", got, tc.wantCode, err)
			}
		})
	}
}
