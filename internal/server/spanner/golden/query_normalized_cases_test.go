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

package golden

import (
	pb "github.com/datacommonsorg/mixer/internal/proto"
)

var normalizedObservationsTestCases = []struct {
	variables []string
	entities  []string
	golden    string
}{
	{
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{"geoId/01001", "geoId/02013"},
		golden:    "get_observations_normalized",
	},
}

var checkVariableExistenceTestCases = []struct {
	variables []string
	entities  []string
	golden    string
}{
	{
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{"geoId/01001", "geoId/02013"},
		golden:    "check_variable_existence_normalized",
	},
}

var getObservationsContainedInPlaceTestCases = []struct {
	variables      []string
	ancestor       string
	childPlaceType string
	golden         string
}{
	{
		variables:      []string{"AirPollutant_Cancer_Risk"},
		ancestor:       "geoId/10",
		childPlaceType: "County",
		golden:         "get_observations_contained_in_place_normalized",
	},
}

var sdmxObservationsTestCases = []struct {
	constraints *pb.SdmxDataQuery
	golden      string
}{
	{
		constraints: &pb.SdmxDataQuery{
			Constraints: map[string]*pb.ConstraintList{
				"variableMeasured": {Values: []string{"Count_Person"}},
				"observationAbout": {Values: []string{"country/USA"}},
			},
		},
		golden: "get_sdmx_observations_basic",
	},
	{
		constraints: &pb.SdmxDataQuery{
			Constraints: map[string]*pb.ConstraintList{
				"variableMeasured": {Values: []string{"Count_Person"}},
				"place":            {Values: []string{"country/USA"}},
				"race":             {Values: []string{"Race_Asian"}},
			},
		},
		golden: "get_sdmx_observations_multi_constraints",
	},
}
