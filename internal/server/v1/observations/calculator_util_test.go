// Copyright 2022 Google LLC
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

package observations

import (
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

func TestParseVarName(t *testing.T) {
	for _, c := range []struct {
		varName     string
		wantVarInfo *varInfo
	}{
		{
			"Person_Count",
			&varInfo{statVar: "Person_Count"},
		},
		{
			"Person_Count_Female[ut=NumberUnit;mm=dcAggregate/Census;op=P1Y;sf=100]",
			&varInfo{
				statVar: "Person_Count_Female",
				statMetadata: &pb.StatMetadata{
					MeasurementMethod: "dcAggregate/Census",
					ObservationPeriod: "P1Y",
					Unit:              "NumberUnit",
					ScalingFactor:     "100",
				},
			},
		},
	} {
		gotVarInfo, err := parseVarName(c.varName)
		if err != nil {
			t.Errorf("parseVarName(%s) = %s", c.varName, err)
		}
		if ok := reflect.DeepEqual(gotVarInfo, c.wantVarInfo); !ok {
			t.Errorf("parseVarName(%s) = %v, want %v",
				c.varName, gotVarInfo, c.wantVarInfo)
		}
	}
}
