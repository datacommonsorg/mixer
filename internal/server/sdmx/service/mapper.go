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
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	restv2 "github.com/datacommonsorg/mixer/internal/server/sdmx/rest/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func dataQueryFromREST(request *restv2.DataRequest) (*pb.SdmxDataQuery, error) {
	constraints := map[string]*pb.ConstraintList{}
	for componentID, values := range request.Constraints {
		constraintID, err := restv2.InternalConstraintComponentID(componentID)
		if err != nil {
			return nil, err
		}
		if _, exists := constraints[constraintID]; exists {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate SDMX component filter %q", constraintID)
		}
		constraints[constraintID] = &pb.ConstraintList{Values: values}
	}
	return &pb.SdmxDataQuery{Constraints: constraints}, nil
}

func availabilityQueryFromREST(request *restv2.AvailabilityRequest) (*pb.SdmxAvailabilityQuery, error) {
	componentID, err := restv2.InternalAvailabilityComponentID(request.Path.ComponentID)
	if err != nil {
		return nil, err
	}
	constraints := map[string]*pb.ConstraintList{}
	for filterID, values := range request.Constraints {
		constraintID, err := restv2.InternalAvailabilityConstraintComponentID(filterID)
		if err != nil {
			return nil, err
		}
		if _, exists := constraints[constraintID]; exists {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate SDMX component filter %q", constraintID)
		}
		constraints[constraintID] = &pb.ConstraintList{Values: values}
	}
	return &pb.SdmxAvailabilityQuery{ComponentId: componentID, Constraints: constraints}, nil
}

func dataStructureID(path restv2.ResourcePath) string {
	agencyID := path.AgencyID
	if agencyID == "" {
		agencyID = datacommons.DataAgencyID
	}
	resourceID := path.ResourceID
	if resourceID == "" {
		resourceID = datacommons.DataResourceID
	}
	version := path.Version
	if version == "" {
		version = datacommons.DataVersion
	}
	return agencyID + ":" + resourceID + "(" + version + ")"
}
