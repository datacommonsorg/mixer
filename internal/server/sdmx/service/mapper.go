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

package service

import (
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	restv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/rest/v2"
)

func dataQueryFromREST(request *restv2.DataRequest) (*sdmxpb.SdmxDataQuery, error) {
	return &sdmxpb.SdmxDataQuery{Constraints: constraintsFromRESTFilters(request.Constraints)}, nil
}

func availabilityQueryFromREST(request *restv2.AvailabilityRequest) (*sdmxpb.SdmxAvailabilityQuery, error) {
	return &sdmxpb.SdmxAvailabilityQuery{
		ComponentId: request.Path.ComponentID,
		Constraints: constraintsFromRESTFilters(request.Constraints),
	}, nil
}

func constraintsFromRESTFilters(filters map[string][]string) map[string]*sdmxpb.SdmxComponentConstraint {
	constraints := map[string]*sdmxpb.SdmxComponentConstraint{}
	for componentID, values := range filters {
		predicates := make([]*sdmxpb.SdmxPredicate, 0, len(values))
		for _, value := range values {
			predicates = append(predicates, &sdmxpb.SdmxPredicate{Value: value})
		}
		constraints[componentID] = &sdmxpb.SdmxComponentConstraint{Predicates: predicates}
	}
	return constraints
}

func dataStructureID(path restv2.ResourcePath) string {
	agencyID := path.AgencyID
	if agencyID == "" {
		agencyID = datacommons.DataflowAgencyID
	}
	resourceID := path.ResourceID
	if resourceID == "" {
		resourceID = datacommons.DataflowID
	}
	version := path.Version
	if version == "" {
		version = datacommons.DataflowVersion
	}
	return agencyID + ":" + resourceID + "(" + version + ")"
}
