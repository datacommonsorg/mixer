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

package restv2

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ComponentTimePeriod       = "TIME_PERIOD"
	ComponentObservationValue = "OBS_VALUE"

	internalObservationDate = "observationDate"
)

func InternalConstraintComponentID(componentID string) (string, error) {
	switch componentID {
	case ComponentTimePeriod:
		return internalObservationDate, nil
	case ComponentObservationValue:
		return "", status.Error(codes.Unimplemented, "SDMX observation value filters are not implemented yet")
	default:
		return componentID, nil
	}
}
