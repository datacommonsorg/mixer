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
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ValidateSupportedConstraints checks that constraints only use SDMX features
// currently supported by Data Commons.
func ValidateSupportedConstraints(constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	for _, constraint := range constraints {
		if len(constraint.GetPropertyConstraints()) > 0 {
			return status.Error(codes.Unimplemented, "SDMX property constraints are not implemented yet")
		}
		for _, predicate := range constraint.GetPredicates() {
			if predicate.GetOperator() != sdmxpb.SdmxOperator_SDMX_OPERATOR_EQ {
				return status.Error(codes.Unimplemented, "SDMX operators other than EQ are not implemented yet")
			}
		}
	}
	return nil
}
